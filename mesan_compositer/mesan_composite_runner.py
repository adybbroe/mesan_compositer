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


def make_scene_id(product, platform_name, orbit_number, start_time):
    """Return the identifier used to prevent duplicate processing."""
    return (
        f"{product}_"
        f"{platform_name}_"
        f"{orbit_number:05d}_"
        f"{start_time:%Y%m%d%H%M}"
    )


def job_is_registered(scene_id, job_register):
    """Return True when processing has already been launched."""
    return bool(job_register.get(scene_id))


def register_job(scene_id, job_register, now=None):
    """Register a scene as submitted for processing."""
    if now is None:
        now = dt.datetime.now(dt.timezone.utc)

    if now.tzinfo is None:
        raise ValueError("Job registration time must be timezone-aware")

    job_register[scene_id] = now


def message_is_applicable(msg):
    """Return True if message platform, sensor and time are supported."""
    platform_name = msg.data["platform_name"]

    sensors = msg.data["sensor"]
    if isinstance(sensors, (list, tuple, set)):
        sensors = set(sensors)
    else:
        sensors = {sensors}

    if "start_time" not in msg.data and "nominal_time" not in msg.data:
        LOG.warning("No start time in message!")
        return False

    if platform_name not in POLAR_SATELLITES and platform_name not in GEO_SATS:
        LOG.info("Platform not supported: %s", platform_name)
        return False

    if platform_name in POLAR_SATELLITES:
        expected_sensor = SENSOR.get(platform_name, "avhrr/3")

        if expected_sensor not in sensors:
            LOG.debug(
                "Scene not applicable. platform and instrument: %s %s",
                platform_name,
                msg.data["sensor"],
            )
            return False

    if platform_name in GEO_SATS and "seviri" not in sensors:
        LOG.debug(
            "Scene not applicable. platform and instrument: %s %s",
            platform_name,
            msg.data["sensor"],
        )
        return False

    return True


def find_files_for_composite(msg, product):
    """Find the input cloud product files (Geo+Polar) for the composite."""
    if msg.type == "file":
        uri = (msg.data["uri"])
    else:
        LOG.debug(
            "Ignoring this type of message data: type = " + str(msg.type))
        return None

    try:
        file4mesan = check_uri(uri)
    except IOError:
        LOG.info("Requested file not present on this host!")
        return None

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
        LOG.debug("File is not applicable. " + "Product requested: " + str(product))
        return None

    return file4mesan


def ready2run(msg, files4comp, job_register, product="CT", now=None):
    """Return scene id and files when a scene is ready for processing."""
    if not message_is_applicable(msg):
        return None

    metadata = get_scene_metadata(msg)
    scene_id = make_scene_id(product,
                             metadata["platform_name"],
                             metadata["orbit_number"],
                             metadata["start_time"]
                             )

    LOG.debug("Scene identifier = %s", scene_id)
    LOG.debug("Job register = %s", job_register)

    if job_is_registered(scene_id, job_register):
        LOG.debug("Processing of scene %s already launched", scene_id)
        return None

    file4mesan = find_files_for_composite(msg, product)
    if file4mesan is None:
        return None

    files4comp.setdefault(scene_id, []).append(file4mesan)
    files = files4comp[scene_id]
    LOG.info("Files ready for Mesan composite: %s", files)

    register_job(scene_id, job_register, now=now)

    return scene_id, files


def get_product(msg):
    """Get the cloud product type from the incoming message."""
    product = msg.data.get("pge")

    if product in ("CT", "CTTH"):
        return product

    uid = msg.data.get("uid", "")

    for product in ("CT", "CTTH"):
        if f"_{product}_" in uid:
            return product

    return None


def get_scene_metadata(msg):
    """Extract normalized scene metadata from a message."""
    platform_name = msg.data["platform_name"]

    start_time = msg.data.get("start_time", msg.data.get("nominal_time"))
    end_time = msg.data.get("end_time")

    if platform_name in GEO_SATS:
        orbit_number = 0
        LOG.info("Geostationary satellite: %s", platform_name)
    else:
        orbit_number = int(msg.data["orbit_number"])
        LOG.info("Polar satellite: %s", platform_name)

    sensor = msg.data.get("sensor")
    if sensor is None:
        raise ValueError("Message is missing sensor")

    return {
        "platform_name": platform_name,
        "start_time": start_time,
        "end_time": end_time,
        "orbit_number": orbit_number,
        "sensor": sensor,
    }



class CompositeWorker:
    """Base class for generating a MESAN cloud composite."""

    product = ""

    def __call__(self, scene, job_id, publish_q, config_options):
        """Run the composite worker."""
        servername = config_options.get("servername", socket.gethostname())

        try:
            LOG.debug("%s: Start compositer...", self.product)

            time_of_analysis, delta_t, area_id = self.get_processing_parameters(scene, config_options)
            LOG.info("Make %s composite for area id = %s", self.product, area_id)

            result_file = self.make_composite(
                time_of_analysis,
                delta_t,
                area_id,
                config_options,
            )

            if not result_file:
                return self.make_result(status="no_result")

            self.publish_result(result_file, scene, servername, publish_q)
            self.log_elapsed_time(job_id)

            super_obs_file = self.make_super_observations(
                result_file,
                time_of_analysis,
                area_id,
                config_options,
            )

            LOG.info("%s super observations generated: %s", self.product, super_obs_file)

            return self.make_result(status="success", result_file=result_file, super_obs_file=super_obs_file)

        except Exception:
            LOG.exception("Failed in %s composite worker", self.product)
            raise

    def make_composite(self, time_of_analysis, delta_t, area_id, config_options):
        """Make composite."""
        raise NotImplementedError


    def make_super_observations(self, result_file, time_of_analysis, area_id, config_options):
        """Make super observations."""
        raise NotImplementedError


    def get_processing_parameters(self, scene, config_options):
        """Get analysis time, time window and area id."""
        time_of_analysis = get_analysis_time(scene["starttime"], scene["endtime"])

        time_window = int(config_options.get("absolute_time_threshold_minutes", "30"))
        LOG.debug("Time window = %s", time_window)

        area_id = config_options.get("mesan_area_id")

        if not area_id:
            LOG.warning(
                "No area id specified in config file. "
                "Using default = %s",
                DEFAULT_AREA,
            )
            area_id = DEFAULT_AREA

        return (time_of_analysis, dt.timedelta(minutes=time_window), area_id)

    def publish_result(self, result_file, scene, servername, publish_q):
        """Publish the generated composite."""
        pubmsg = create_message( result_file, scene, servername, )

        LOG.info("Sending: %s", pubmsg)
        publish_q.put(pubmsg)

    def log_elapsed_time(self, job_id):
        """Log elapsed processing time."""
        if not isinstance(job_id, dt.datetime):
            LOG.warning( "Job entry is not a datetime instance: %s", job_id)
            return

        elapsed = dt.datetime.now(dt.timezone.utc) - job_id

        LOG.info("%s composite scene %s finished. It took: %s", self.product, job_id, elapsed,)

    def make_result(self, *, status, result_file=None, super_obs_file=None):
        """Return a small picklable worker result."""
        return {
            "status": status,
            "product": self.product,
            "worker_pid": os.getpid(),
            "result_file": result_file,
            "super_obs_file": super_obs_file,
        }


class CloudTypeCompositeWorker(CompositeWorker):
    """Worker generating the Cloud Type composite."""

    product = "CT"

    def make_composite(self, time_of_analysis, delta_t, area_id, config_options):
        """Make the high resolution cloud composite."""
        return do_cloud_type_composite(time_of_analysis, delta_t, area_id, config_options)

    def make_super_observations(self, result_file, time_of_analysis, area_id, config_options):
        """Make the cloud parameter super observations."""
        return do_cloudamount(result_file, time_of_analysis, area_id, config_options, )


class CloudTopHeightCompositeWorker(CompositeWorker):
    """Worker generating the CTTH composite."""

    product = "CTTH"

    def make_composite(self, time_of_analysis, delta_t, area_id, config_options):
        """Make the high resolution cloud composite."""
        return do_ctth_composite(time_of_analysis, delta_t, area_id, config_options)

    def make_super_observations(self, result_file, time_of_analysis, area_id, config_options):
        """Make the cloud parameter super observations."""
        return do_cloudheight(result_file, time_of_analysis, area_id, config_options, )


def ctype_composite_worker(scene, job_id, publish_q, config_options):
    """Create a CT composite."""
    return _CT_WORKER( scene, job_id, publish_q, config_options)


def ctth_composite_worker(scene, job_id, publish_q, config_options):
    """Create a CTTH composite."""
    return _CTTH_WORKER(scene, job_id, publish_q, config_options, )


NO_MESSAGE = object()

COMPOSITE_WORKERS = {
    "CT": ctype_composite_worker,
    "CTTH": ctth_composite_worker,
}

_CT_WORKER = CloudTypeCompositeWorker()
_CTTH_WORKER = CloudTopHeightCompositeWorker()


def get_next_message(listener_q):
    """Return the next listener message, or ``NO_MESSAGE`` on timeout."""
    try:
        return listener_q.get(timeout=1.0)
    except Empty:
        return NO_MESSAGE


def build_scene(msg, metadata, product):
    """Build the scene dictionary passed to a composite worker."""
    urlobj = urlparse(msg.data["uri"])
    path, fname = os.path.split(urlobj.path)
    LOG.debug("path %s filename = %s", path, fname)

    return {
        "platform_name": metadata["platform_name"],
        "orbit_number": metadata["orbit_number"],
        "starttime": metadata["start_time"],
        "endtime": metadata["end_time"],
        "sensor": metadata["sensor"],
        "filename": urlobj.path,
        "product": product,
    }


def start_registry_timer(jobs_dict, scene_id, interval=5 * 60.0):
    """Release a job registry entry after the duplicate-message hold period."""
    timer = threading.Timer(
        interval,
        reset_job_registry,
        args=(jobs_dict, scene_id),
    )
    timer.daemon = True
    timer.start()
    return timer


def _pending_duplicate(scene_id, pending_jobs):
    """Return True and log when a scene already has an active pool job."""
    pending = pending_jobs.get(scene_id)
    if pending is None:
        return False

    LOG.warning(
        "Duplicate message ignored because pool job is still pending: "
        "job=%s token=%s elapsed=%.1fs",
        scene_id,
        pending.token,
        time.monotonic() - pending.submitted_monotonic,
    )
    return True


@dataclass
class RunnerState:
    """Runner state class."""

    pool: object
    publisher_q: object
    completion_q: Queue
    composite_files: dict
    jobs_dict: dict
    pending_jobs: dict
    token_counter: object
    config_options: dict


def process_message(msg, state):
    """Process one incoming message and submit a composite job if ready."""
    registered_key = None

    try:
        LOG.debug(
            "Number of threads currently alive: %s",
            threading.active_count(),
        )

        metadata = get_scene_metadata(msg)
        if not metadata["start_time"]:
            LOG.warning("Neither start_time nor nominal_time in message!")
            return

        if not metadata["end_time"]:
            LOG.debug("No end_time in message!")

        product = get_product(msg)
        if product is None:
            LOG.debug("Message does not contain a supported product")
            return

        scene_id = make_scene_id(
            product,
            metadata["platform_name"],
            metadata["orbit_number"],
            metadata["start_time"],
        )

        # This check must happen before ready2run().  The job registry entry is
        # deliberately released after five minutes, while a worker may still be
        # running.  Calling ready2run() first would append the same input file and
        # create a fresh registry entry for that still-running scene.
        if _pending_duplicate(scene_id, state.pending_jobs):
            return

        job = ready2run(
            msg,
            state.composite_files,
            state.jobs_dict,
            product,
        )
        if job is None:
            return

        keyname, _ = job
        registered_key = keyname

        # Defensive check: a successful ready2run() must have registered the job.
        if keyname not in state.jobs_dict:
            LOG.warning("Scene-run seems unregistered! Forget it...")
            registered_key = None
            return

        scene = build_scene(msg, metadata, product)

        worker = COMPOSITE_WORKERS.get(product)
        if worker is None:
            LOG.warning("Product %s not supported!", product)
            state.jobs_dict.pop(keyname, None)
            registered_key = None
            return

        submitted = submit_pool_job(
            state.pool,
            state.completion_q,
            state.pending_jobs,
            state.token_counter,
            job_key=keyname,
            product=product,
            worker=worker,
            worker_args=(
                scene,
                state.jobs_dict[keyname],
                state.publisher_q,
                state.config_options,
            ),
        )

        if not submitted:
            state.jobs_dict.pop(keyname, None)
            registered_key = None
            return

        start_registry_timer(state.jobs_dict, keyname)
        registered_key = None

    except Exception:
        if registered_key is not None:
            state.jobs_dict.pop(registered_key, None)
        raise


def mesan_live_runner(config_options):
    """Start and run the Mesan cloud composite processing in real-time."""
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

    manager = Manager()

    listener_q = manager.Queue()
    publisher_q = manager.Queue()

    # Pool callbacks execute in the parent, so this can be a normal
    # thread-safe queue. It is not passed to a child process.
    completion_q = Queue()

    pool = Pool(processes=1, maxtasksperchild=1)

    pub_thread = FilePublisher(publisher_q)
    pub_thread.start()

    listen_thread = FileListener(listener_q)
    listen_thread.start()

    state = RunnerState(
        pool=pool,
        publisher_q=publisher_q,
        completion_q=completion_q,
        composite_files={},
        jobs_dict={},
        pending_jobs={},
        token_counter=count(1),
        config_options=config_options,
    )

    try:
        while True:
            # Calling AsyncResult.get() inside this function is what surfaces
            # child-process Python exceptions in the parent log.
            collect_pool_results(state.completion_q, state.pending_jobs)

            monitor_pending_pool_jobs(
                state.pending_jobs,
                state.pool,
                warning_seconds=warning_seconds,
                warning_repeat_seconds=warning_repeat_seconds,
                hard_timeout_seconds=hard_timeout_seconds,
            )

            msg = get_next_message(listener_q)
            if msg is NO_MESSAGE:
                continue

            if msg is None:
                LOG.info("Listener requested shutdown")
                break

            try:
                process_message(msg, state)
            except Exception:
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
        state.pool.terminate()
        state.pool.join()

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
