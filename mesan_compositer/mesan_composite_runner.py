#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015-2023, 2026 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Posttroll-runner for the mesan composite generator.

Listens to incoming satellite data products (lvl2 cloud products) and generates
a mesan composite valid for the closest (whole) hour.
"""

import argparse
import datetime as dt
import logging.config
import os
import socket
import sys
import threading
import time
from dataclasses import dataclass
from functools import partial
from itertools import count
from multiprocessing import Manager, Pool
from queue import Empty, Queue
from urllib.parse import urlparse

import posttroll.subscriber
from posttroll.message import Message
from posttroll.publisher import Publish

from mesan_compositer.composite_tools import get_analysis_time
from mesan_compositer.config import get_config
from mesan_compositer.ct_quicklooks import ctth_quicklook_from_netcdf
from mesan_compositer.logger import setup_logging
from mesan_compositer.make_ct_composite import CloudproductCompositer
from mesan_compositer.netcdf_io import cloudComposite
from mesan_compositer.prt_nwcsaf_cloudamount import derive_sobs as derive_sobs_clamount
from mesan_compositer.prt_nwcsaf_cloudheight import derive_sobs as derive_sobs_clheight
from mesan_compositer.utils import NoGeoScenesError, check_uri, get_local_ips

LOG = logging.getLogger(__name__)

DEFAULT_AREA = "mesanEx"
DEFAULT_SUPEROBS_WINDOW_SIZE_NPIX = 32


SENSOR = {"NOAA-19": "avhrr/3",
          "NOAA-18": "avhrr/3",
          "NOAA-15": "avhrr/3",
          "Metop-A": "avhrr/3",
          "Metop-B": "avhrr/3",
          "Metop-C": "avhrr/3",
          "EOS-Terra": "modis",
          "EOS-Aqua": "modis",
          "Suomi-NPP": "viirs",
          "NOAA-20": "viirs"}


POLAR_SATELLITES: list[str] = []


GEO_SATS = ["Meteosat-10", "Meteosat-9", "Meteosat-8", "Meteosat-11", ]
MSG_NAME = {"Meteosat-10": "MSG3", "Meteosat-9": "MSG2",
            "Meteosat-8": "MSG1", "Meteosat-11": "MSG4"}

PRODUCT_NAMES = ["CMA", "CT", "CTTH", "PC", "CPP"]


def get_arguments():
    """Get command line arguments.

    args.logging_conf_file, args.config_file, obs_time, area_id, wsize

    Return:
      File path of the logging.ini file
      File path of the application configuration file

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config_file",
                        type=str,
                        dest="config_file",
                        required=True,
                        help="The file containing configuration parameters e.g. mesan_sat_config.yaml")
    parser.add_argument("-l", "--logging",
                        help="The path to the log-configuration file (e.g. './log_config.yaml')",
                        dest="log_config_file",
                        type=str,
                        required=False)
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")

    args = parser.parse_args()
    if "template" in args.config_file:
        print("Template file given as master config, aborting!")
        sys.exit()

    return args



class StuckPoolJob(RuntimeError):
    """Raised when a submitted pool job exceeds the configured hard timeout."""


@dataclass
class PendingPoolJob:
    """Parent-side state for one Pool.apply_async submission."""

    key: str
    token: int
    product: str
    submitted_monotonic: float
    async_result: object
    last_warning_monotonic: float = 0.0


def _notify_pool_success(completion_queue, job_key, token, _worker_result):
    """Notify the main thread without doing work in Pool's result thread."""
    completion_queue.put_nowait((job_key, token, "success"))


def _notify_pool_failure(completion_queue, job_key, token, exc):
    """Notify the main thread; AsyncResult.get() will log the traceback."""
    completion_queue.put_nowait((job_key, token, "failure", repr(exc)))


def _pool_worker_snapshot(pool):
    """Return best-effort worker diagnostics.

    ``Pool._pool`` is private, so this information is diagnostic only.
    """
    return [
        {
            "pid": process.pid,
            "alive": process.is_alive(),
            "exitcode": process.exitcode,
        }
        for process in getattr(pool, "_pool", ())
    ]


def submit_pool_job(
        pool,
        completion_queue,
        pending_jobs,
        token_counter,
        *,
        job_key,
        product,
        worker,
        worker_args):
    """Submit one job and retain its AsyncResult in the parent."""
    if job_key in pending_jobs:
        LOG.warning("Job is already pending; duplicate ignored: job=%s", job_key)
        return False

    token = next(token_counter)

    LOG.info(
        "Submitting pool job: job=%s token=%s product=%s workers=%s",
        job_key,
        token,
        product,
        _pool_worker_snapshot(pool),
    )

    try:
        async_result = pool.apply_async(
            worker,
            args=worker_args,
            callback=partial(
                _notify_pool_success,
                completion_queue,
                job_key,
                token,
            ),
            error_callback=partial(
                _notify_pool_failure,
                completion_queue,
                job_key,
                token,
            ),
        )
    except Exception:
        # This covers synchronous submission failures, for example a pool
        # that has already stopped accepting work.
        LOG.exception(
            "pool.apply_async failed synchronously: "
            "job=%s token=%s product=%s",
            job_key,
            token,
            product,
        )
        return False

    pending_jobs[job_key] = PendingPoolJob(
        key=job_key,
        token=token,
        product=product,
        submitted_monotonic=time.monotonic(),
        async_result=async_result,
    )

    LOG.info(
        "Submitted pool job: job=%s token=%s product=%s pending=%d",
        job_key,
        token,
        product,
        len(pending_jobs),
    )
    return True


def _finish_pool_job(pending_jobs, job_key, token):
    """Collect one AsyncResult.

    Return ``(finished, succeeded, worker_result)``. ``finished`` is false
    only for the tiny callback-before-ready race.
    """
    pending = pending_jobs.get(job_key)
    if pending is None or pending.token != token:
        LOG.debug(
            "Ignoring stale pool completion: job=%s token=%s",
            job_key,
            token,
        )
        return True, None, None

    # Pool invokes the callback immediately before marking AsyncResult ready.
    # If the main thread wins that small race, leave the record pending; the
    # fallback scan in collect_pool_results() will collect it next time.
    if not pending.async_result.ready():
        return False, None, None

    elapsed = time.monotonic() - pending.submitted_monotonic

    try:
        # This is the important call: worker exceptions are re-raised here,
        # including the multiprocessing remote traceback.
        worker_result = pending.async_result.get()
    except Exception:
        LOG.exception(
            "Pool job failed: job=%s token=%s product=%s elapsed=%.1fs",
            job_key,
            token,
            pending.product,
            elapsed,
        )
        succeeded = False
        worker_result = None
    else:
        LOG.info(
            "Pool job completed: job=%s token=%s product=%s "
            "elapsed=%.1fs worker_result=%r",
            job_key,
            token,
            pending.product,
            elapsed,
            worker_result,
        )
        succeeded = True
    finally:
        pending_jobs.pop(job_key, None)

    return True, succeeded, worker_result


def collect_pool_results(completion_queue, pending_jobs):
    """Collect completed jobs without blocking the main message loop."""
    completed = []

    while True:
        try:
            event = completion_queue.get_nowait()
        except Empty:
            break

        job_key, token, event_type, *details = event

        if event_type == "failure" and details:
            LOG.error(
                "Pool error callback: job=%s token=%s exception=%s",
                job_key,
                token,
                details[0],
            )

        finished, succeeded, worker_result = _finish_pool_job(
            pending_jobs,
            job_key,
            token,
        )
        if finished and succeeded is not None:
            completed.append((job_key, succeeded, worker_result))

    # Fallback: do not depend exclusively on callback delivery.
    for job_key, pending in list(pending_jobs.items()):
        if not pending.async_result.ready():
            continue

        finished, succeeded, worker_result = _finish_pool_job(
            pending_jobs,
            job_key,
            pending.token,
        )
        if finished and succeeded is not None:
            completed.append((job_key, succeeded, worker_result))

    return completed


def monitor_pending_pool_jobs(
        pending_jobs,
        pool,
        *,
        warning_seconds,
        warning_repeat_seconds,
        hard_timeout_seconds):
    """Report jobs that stay unready and optionally fail the service."""
    now = time.monotonic()

    for pending in pending_jobs.values():
        elapsed = now - pending.submitted_monotonic

        if (elapsed >= warning_seconds and
                now - pending.last_warning_monotonic >= warning_repeat_seconds):
            LOG.error(
                "Pool job has not completed: job=%s token=%s product=%s "
                "elapsed=%.1fs ready=%s workers=%s",
                pending.key,
                pending.token,
                pending.product,
                elapsed,
                pending.async_result.ready(),
                _pool_worker_snapshot(pool),
            )
            pending.last_warning_monotonic = now

        if hard_timeout_seconds > 0 and elapsed >= hard_timeout_seconds:
            raise StuckPoolJob(
                f"Pool job {pending.key!r} has been pending for "
                f"{elapsed:.1f} seconds"
            )


def reset_job_registry(objdict, key):
    """Remove job key from registry."""
    LOG.debug("Release/reset job-key %s from job registry", key)
    objdict.pop(key, None)


class FilePublisher(threading.Thread):
    """A publisher for the cloud composite result files.

    Picks up the return value from the ctype_composite_worker when ready, and
    publishes the files via posttroll.

    """

    def __init__(self, queue):
        """Initialize the file publisher object."""
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.jobs = {}

    def stop(self):
        """Stop the file publisher."""
        self.loop = False
        self.queue.put(None)

    def run(self):
        """Start the publisher thread and publish as adequate until interrupted/stopped."""
        with Publish("mesan_composite_runner", 0, ["netCDF/3", ]) as publisher:
            while self.loop:
                retv = self.queue.get()
                if retv is not None:
                    LOG.info("Publish the files...")
                    publisher.send(retv)


class FileListener(threading.Thread):
    """A file listener class, to listen for incoming messages.

    The messages requires a relevant file and will trigger for further processing on it.
    """

    def __init__(self, queue):
        """Initialize the file listener object."""
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue

    def stop(self):
        """Stop the file listener."""
        self.loop = False
        self.queue.put(None)

    def run(self):
        """Start the runner and run indefinately until interrupted."""
        with posttroll.subscriber.Subscribe("", ["CF/2",
                                                 "2/nwcsaf-geo/0deg/ctth-plax-corrected",
                                                 "2/nwcsaf-geo/0deg/ct-plax-corrected"], True) as subscr:
            for msg in subscr.recv(timeout=90):
                if not self.loop:
                    break

                # Check if it is a relevant message:
                if self.check_message(msg):
                    LOG.debug("Put the message on the queue...")
                    self.queue.put(msg)

    def check_message(self, msg):
        """Check that the incoming message is okay."""
        if not msg:
            return False

        urlobj = urlparse(msg.data["uri"])
        url_ip = socket.gethostbyname(urlobj.netloc)
        if urlobj.netloc and (url_ip not in get_local_ips()):
            LOG.warning("Server %s not the current one: %s", str(urlobj.netloc), socket.gethostname())
            return False

        if ("platform_name" not in msg.data or
                "orbit_number" not in msg.data or
                "start_time" not in msg.data):
            LOG.info(
                "Message is lacking crucial fields, probably an MSG scene...")
            if ("platform_name" not in msg.data or
                    "nominal_time" not in msg.data or
                    "pge" not in msg.data):
                LOG.warning("Message is lacking crucial fields...")
                return False

        if msg.data["platform_name"] not in (GEO_SATS + POLAR_SATELLITES):
            LOG.info(str(msg.data["platform_name"]) + ": " +
                     "Not a MSG or a NOAA/Metop/S-NPP/Terra/Aqua scene. Continue...")
            return False

        LOG.debug("Ok: message = %s", str(msg))
        return True


def create_message(resultfile, scene, servername):
    """Create the posttroll message."""
    to_send = {}
    to_send["uri"] = ("ssh://%s/%s" % (servername, resultfile))
    to_send["uid"] = resultfile
    to_send["sensor"] = scene.get("instrument")
    if not to_send["sensor"]:
        to_send["sensor"] = scene.get("sensor")

    to_send["platform_name"] = scene["platform_name"]
    to_send["orbit_number"] = scene.get("orbit_number")
    to_send["type"] = "netCDF"
    to_send["format"] = "MESAN"
    to_send["data_processing_level"] = "3"
    to_send["start_time"] = scene["starttime"]
    to_send["end_time"] = scene["endtime"]
    pub_message = Message("/" + to_send["format"] + "/" + to_send["data_processing_level"] +
                          "/polar/direct_readout/",
                          "file", to_send).encode()

    return pub_message


def ready2run(msg, files4comp, job_register, sceneid, product="CT"):
    """Check whether we can start a composite generation on scene."""
    LOG.debug("Ready to run?")
    LOG.info("Got message: " + str(msg))

    if msg.type == "file":
        uri = (msg.data["uri"])
    else:
        LOG.debug(
            "Ignoring this type of message data: type = " + str(msg.type))
        return False

    try:
        file4mesan = check_uri(uri)
    except IOError:
        LOG.info("Requested file not present on this host!")
        return False

    platform_name = msg.data["platform_name"]

    sensors = msg.data["sensor"]
    if not isinstance(sensors, (list, tuple, set)):
        sensors = [sensors]

    if "start_time" not in msg.data and "nominal_time" not in msg.data:
        LOG.warning("No start time in message!")
        return False

    if platform_name not in POLAR_SATELLITES and platform_name not in GEO_SATS:
        LOG.info("Platform not supported: " + str(platform_name))
        return False

    if platform_name in POLAR_SATELLITES and SENSOR.get(platform_name, "avhrr/3") not in sensors:
        LOG.debug("Scene not applicable. platform and instrument: " +
                  str(msg.data["platform_name"]) + " " +
                  str(msg.data["sensor"]))
        return False
    elif platform_name in GEO_SATS and "seviri" not in sensors:
        LOG.debug("Scene not applicable. platform and instrument: " +
                  str(msg.data["platform_name"]) + " " +
                  str(msg.data["sensor"]))
        return False

    if "uid" not in msg.data:
        if "uri" not in msg.data:
            raise IOError("No uri or url in message!")
        # Get uid from uri:
        uri = urlparse(msg.data["uri"])
        uid = os.path.basename(uri.path)
    else:
        uid = msg.data["uid"]
    prefixes = ["S_NWC_" + product + "_",
                "SAFNWC_" + MSG_NAME.get(str(msg.data["platform_name"]), "MSG4") +
                "_" + product + "_"]
    file_ok = False
    for prfx in prefixes:
        LOG.debug("File prefix to check for: %s", prfx)
        if uid.startswith(prfx):
            LOG.debug("File uid ok: %s", str(uid))
            file_ok = True
            break
    if not file_ok:
        LOG.debug("File uid not ok: %s", str(uid))
        LOG.debug("File is not applicable. " +
                  "Product requested: " + str(product))
        return False

    LOG.debug("Scene identifier = " + str(sceneid))
    LOG.debug("Job register = " + str(job_register))
    if job_register.get(sceneid):
        LOG.debug("Processing of scene " + str(sceneid) +
                  " have already been launched...")
        return False

    if sceneid not in files4comp:
        files4comp[sceneid] = []

    files4comp[sceneid].append(file4mesan)

    LOG.info("Files ready for Mesan composite: " +
             str(files4comp[sceneid]))

    job_register[sceneid] = dt.datetime.now(dt.timezone.utc)
    return True


def ctype_composite_worker(scene, job_id, publish_q, config_options):
    """Create a CT composite and return a small, picklable result record."""
    servername = config_options.get("servername", socket.gethostname())
    try:
        LOG.debug("Ctype: Start compositer...")
        time_of_analysis = get_analysis_time(
            scene["starttime"], scene["endtime"])
        twindow = int(config_options.get("absolute_time_threshold_minutes", "30"))
        delta_t = dt.timedelta(minutes=twindow)
        LOG.debug("Time window = %s", twindow)
        mesan_area_id = config_options.get("mesan_area_id", None)
        if not mesan_area_id:
            LOG.warning(
                "No area id specified in config file. Using default = %s",
                DEFAULT_AREA,
            )
            mesan_area_id = DEFAULT_AREA

        LOG.info("Make ctype composite for area id = %s", mesan_area_id)
        result_file = do_cloud_type_composite(
            time_of_analysis, delta_t, mesan_area_id, config_options)

        if not result_file:
            return {
                "status": "no_result",
                "product": "CT",
                "worker_pid": os.getpid(),
                "result_file": None,
                "super_obs_file": None,
            }

        pubmsg = create_message(result_file, scene, servername)
        LOG.info("Sending: %s", pubmsg)
        publish_q.put(pubmsg)

        if isinstance(job_id, dt.datetime):
            dt_ = dt.datetime.now(dt.timezone.utc) - job_id
            LOG.info(
                "Ctype composite scene %s finished. It took: %s",
                job_id,
                dt_,
            )
        else:
            LOG.warning("Job entry is not a datetime instance: %s", job_id)

        super_obs_filename = do_cloudamount(
            result_file, time_of_analysis, mesan_area_id, config_options)
        LOG.info(
            "Cloud amount super observations generated: %s",
            super_obs_filename,
        )

        return {
            "status": "success",
            "product": "CT",
            "worker_pid": os.getpid(),
            "result_file": result_file,
            "super_obs_file": super_obs_filename,
        }

    except Exception:
        LOG.exception("Failed in ctype_composite_worker...")
        raise


def ctth_composite_worker(scene, job_id, publish_q, config_options):
    """Create a CTTH composite and return a small, picklable result record."""
    servername = config_options.get("servername", socket.gethostname())
    try:
        LOG.debug("CTTH compositer: Start...")
        time_of_analysis = get_analysis_time(
            scene["starttime"], scene["endtime"])
        twindow = int(config_options.get("absolute_time_threshold_minutes", "30"))
        delta_t = dt.timedelta(minutes=twindow)
        LOG.debug("Time window = %s", twindow)
        mesan_area_id = config_options.get("mesan_area_id", None)
        if not mesan_area_id:
            LOG.warning(
                "No area id specified in config file. Using default = %s",
                DEFAULT_AREA,
            )
            mesan_area_id = DEFAULT_AREA

        LOG.info(
            "Make cloud height composite for area id = %s", mesan_area_id)
        result_file = do_ctth_composite(
            time_of_analysis, delta_t, mesan_area_id, config_options)
        LOG.debug(
            "After CTTH compositer part. Filename returned = %s",
            result_file,
        )

        if not result_file:
            return {
                "status": "no_result",
                "product": "CTTH",
                "worker_pid": os.getpid(),
                "result_file": None,
                "super_obs_file": None,
            }

        pubmsg = create_message(result_file, scene, servername)
        LOG.info("Sending: %s", pubmsg)
        publish_q.put(pubmsg)

        if isinstance(job_id, dt.datetime):
            dt_ = dt.datetime.now(dt.timezone.utc) - job_id
            LOG.info(
                "Cloud Height composite scene %s finished. It took: %s",
                job_id,
                dt_,
            )
        else:
            LOG.warning("Job entry is not a datetime instance: %s", job_id)

        super_obs_filename = do_cloudheight(
            result_file, time_of_analysis, mesan_area_id, config_options)
        LOG.info(
            "Cloud height super observations generated: %s",
            super_obs_filename,
        )

        return {
            "status": "success",
            "product": "CTTH",
            "worker_pid": os.getpid(),
            "result_file": result_file,
            "super_obs_file": super_obs_filename,
        }

    except Exception:
        LOG.exception("Failed in ctth_composite_worker...")
        raise



def mesan_live_runner(config_options):
    """Start and run the Mesan cloud composite processing in real-time.

    Run the live service while supervising every asynchronous pool job.

    Processing is triggered on incoming Meteosat NWCSAF/Geo cloud scene by
    listening to incoming messages. Processing is being triggered and possible
    NWCSAF/PPS scenes are taken into account. When successfully finished on a
    time slot, the composite is written to disk and a Posttroll message is
    being sent.

    """
    LOG.info("*** Start the runner for the Mesan composite generator:")
    LOG.debug("os.environ = %s", os.environ)
    npix = int(config_options.get(
        "number_of_pixels", DEFAULT_SUPEROBS_WINDOW_SIZE_NPIX))
    LOG.debug("Number of pixels = %s", npix)

    warning_seconds = float(
        config_options.get("pool_job_warning_seconds", 20 * 60))
    warning_repeat_seconds = float(
        config_options.get("pool_job_warning_repeat_seconds", 5 * 60))
    hard_timeout_seconds = float(
        config_options.get("pool_job_hard_timeout_seconds", 0))


    def worker_succeeded(job_key, result):
        completion_q.put(("success", job_key, result))

    def worker_failed(job_key, exc):
        completion_q.put(("failed", job_key, exc))

    completion_q = Queue()
    pending = {}

    pool = Pool(processes=1, maxtasksperchild=1)
    manager = Manager()
    listener_q = manager.Queue()
    publisher_q = manager.Queue()

    # Pool callbacks execute in the parent, so this can be a normal
    # thread-safe queue. It is not passed to a child process.
    completion_q = Queue()

    pub_thread = FilePublisher(publisher_q)
    pub_thread.start()
    listen_thread = FileListener(listener_q)
    listen_thread.start()

    composite_files = {}
    jobs_dict = {}
    pending_jobs = {}
    token_counter = count(1)

    try:
        while True:
            # Calling AsyncResult.get() inside this function is what surfaces
            # child-process Python exceptions in the parent log.
            collect_pool_results(completion_q, pending_jobs)

            monitor_pending_pool_jobs(
                pending_jobs,
                pool,
                warning_seconds=warning_seconds,
                warning_repeat_seconds=warning_repeat_seconds,
                hard_timeout_seconds=hard_timeout_seconds,
            )

            try:
                msg = listener_q.get(timeout=1.0)
            except Empty:
                continue

            if msg is None:
                LOG.info("Listener requested shutdown")
                break

            registered_key = None
            try:
                LOG.debug(
                    "Number of threads currently alive: %s",
                    threading.active_count(),
                )

                if "start_time" in msg.data:
                    start_time = msg.data["start_time"]
                elif "nominal_time" in msg.data:
                    start_time = msg.data["nominal_time"]
                else:
                    LOG.warning(
                        "Neither start_time nor nominal_time in message!")
                    continue

                if "end_time" in msg.data:
                    end_time = msg.data["end_time"]
                else:
                    LOG.debug("No end_time in message!")
                    end_time = None

                sensor = str(msg.data["sensor"])
                platform_name = msg.data["platform_name"]

                if platform_name not in GEO_SATS:
                    orbit_number = int(msg.data["orbit_number"])
                    LOG.info("Polar satellite: %s", platform_name)
                else:
                    orbit_number = "00000"
                    LOG.info("Geostationary satellite: %s", platform_name)

                keyname = (
                    f"{platform_name}_{orbit_number}_"
                    f"{start_time.strftime('%Y%m%d%H%M')}"
                )

                product = "UNKNOWN"
                if "pge" in msg.data:
                    product = msg.data["pge"]
                elif "uid" in msg.data:
                    uid = msg.data["uid"]
                    for pge in PRODUCT_NAMES:
                        if f"_{pge}_" in uid:
                            product = pge
                            break

                keyname = f"{product}_{keyname}"

                # Unlike the old five-minute registry timer, this entry remains
                # present for the actual lifetime of the asynchronous job.
                if keyname in pending_jobs:
                    pending = pending_jobs[keyname]
                    LOG.warning(
                        "Duplicate message ignored because pool job is still "
                        "pending: job=%s token=%s elapsed=%.1fs",
                        keyname,
                        pending.token,
                        time.monotonic() - pending.submitted_monotonic,
                    )
                    continue

                status = ready2run(
                    msg, composite_files, jobs_dict, keyname, product)
                if not status:
                    continue

                # If an exception occurs before successful submission, release
                # the registry entry in the per-message exception handler.
                registered_key = keyname

                urlobj = urlparse(msg.data["uri"])
                path, fname = os.path.split(urlobj.path)
                LOG.debug("path %s filename = %s", path, fname)
                scene = {
                    "platform_name": platform_name,
                    "orbit_number": orbit_number,
                    "starttime": start_time,
                    "endtime": end_time,
                    "sensor": sensor,
                    "filename": urlobj.path,
                    "product": product,
                }

                if keyname not in jobs_dict:
                    LOG.warning("Scene-run seems unregistered! Forget it...")
                    registered_key = None
                    continue

                worker = {
                    "CT": ctype_composite_worker,
                    "CTTH": ctth_composite_worker,
                }.get(product)

                if worker is None:
                    LOG.warning("Product %s not supported!", product)
                    jobs_dict.pop(keyname, None)
                    registered_key = None
                    continue

                submitted = submit_pool_job(
                    pool,
                    completion_q,
                    pending_jobs,
                    token_counter,
                    job_key=keyname,
                    product=product,
                    worker=worker,
                    worker_args=(
                        scene,
                        jobs_dict[keyname],
                        publisher_q,
                        config_options,
                    ),
                )

                if not submitted:
                    jobs_dict.pop(keyname, None)
                    registered_key = None
                    continue

                # Preserve the old five-minute duplicate-message hold. The
                # pending_jobs check above continues blocking duplicates if the
                # actual worker runs for more than five minutes.
                registry_timer = threading.Timer(
                    5 * 60.0,
                    reset_job_registry,
                    args=(jobs_dict, keyname),
                )
                registry_timer.daemon = True
                registry_timer.start()
                registered_key = None

            except Exception:
                if registered_key is not None:
                    jobs_dict.pop(registered_key, None)
                LOG.exception(
                    "Unhandled exception while processing message: %r", msg)

    except StuckPoolJob:
        LOG.critical(
            "A pool job exceeded the hard timeout; terminating the service "
            "so the process supervisor can restart it."
        )
        raise
    finally:
        # close()/join() may wait forever when the worker itself is hung.
        pool.terminate()
        pool.join()

        pub_thread.stop()
        listen_thread.stop()
        pub_thread.join(timeout=10)
        listen_thread.join(timeout=10)
        manager.shutdown()



def do_cloud_type_composite(time_of_analysis, delta_t, area_id, config_options):
    """Make the cloud type composite."""
    ctcomp = CloudproductCompositer(time_of_analysis, delta_t, area_id, config_options, "CT")
    try:
        ctcomp.get_catalogue()
    except NoGeoScenesError:
        LOG.info("No Geo scenes for composite, so skip further processing.")
        return

    ctcomp.blend_cloud_products()
    output_filepath = ctcomp.write()
    ctcomp.quicklook(output_filepath)

    return output_filepath


def do_ctth_composite(time_of_analysis, delta_t, area_id, config_options):
    """Make the cloud top temperature height composite."""
    ctcomp = CloudproductCompositer(time_of_analysis, delta_t, area_id, config_options, "CTTH")
    try:
        ctcomp.get_catalogue()
    except NoGeoScenesError:
        LOG.info("No Geo scenes for composite, so skip further processing.")
        return

    LOG.debug("CTTH catalogue created. Now do the blending...")
    ctcomp.blend_cloud_products()
    output_filepath = ctcomp.write()

    ctth_quicklook_from_netcdf(ctcomp.group_name, output_filepath)
    LOG.debug("CTTH quicklook done.")
    return output_filepath


def do_cloudamount(filename, time_of_analysis, area_id, config_options):
    """Make the cloud amount super observations."""
    npix = int(config_options.get("number_of_pixels", DEFAULT_SUPEROBS_WINDOW_SIZE_NPIX))
    ipar = str(config_options.get("cloud_amount_ipar"))
    if not ipar:
        raise IOError("No ipar value in config file!")

    # Make Super observations:
    LOG.info("Make Cloud Type super observations")

    try:
        ctype = cloudComposite(filename, "CT_group", areaname=area_id)
        ctype.load()
    except KeyError:
        ctype = cloudComposite(filename, "ct", areaname=area_id)
        ctype.load()

    values = {"area": area_id, }
    bname = time_of_analysis.strftime(config_options["cloudamount_filename"]) % values
    path = config_options["composite_output_dir"]
    filename = os.path.join(path, bname + ".dat")

    derive_sobs_clamount(ctype, ipar, npix, filename)
    return filename


def do_cloudheight(filename, time_of_analysis, area_id, config_options):
    """Make the cloud height super observations."""
    npix = int(config_options.get("number_of_pixels", DEFAULT_SUPEROBS_WINDOW_SIZE_NPIX))

    # Make Super observations:
    LOG.info("Make Cloud Top Height super observations")
    try:
        ctth = cloudComposite(filename, "CTTH_ALTI_group", areaname=area_id)
        ctth.load()
    except KeyError:
        ctth = cloudComposite(filename, "ctth_alti", areaname=area_id)
        ctth.load()

    values = {"area": area_id, }

    bname = time_of_analysis.strftime(config_options["cloudheight_filename"]) % values
    path = config_options["composite_output_dir"]
    filename = os.path.join(path, bname + ".dat")
    LOG.info("Make Cloud Height super observations. Output file = %s", str(filename))
    derive_sobs_clheight(ctth, npix, filename)
    return filename


def main():
    """Start the live runner using command line arguments."""
    global POLAR_SATELLITES
    cmd_args = get_arguments()
    setup_logging(cmd_args)

    configuration = get_config(cmd_args.config_file)
    POLAR_SATELLITES = configuration.get("polar_satellites")
    mesan_live_runner(configuration)


if __name__ == "__main__":
    main()
