#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c20671.ad.smhi.se>

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

"""Posttroll-runner for the mesan composite generator.  Listens to incoming
satellite data products (lvl2 cloud products) and generates a mesan composite
valid for the closest (whole) hour.

"""

import os
import ConfigParser
import logging
LOG = logging.getLogger(__name__)

CFG_DIR = os.environ.get('MESAN_COMPOSITE_CONFIG_DIR', './')
DIST = os.environ.get("SMHI_DIST", None)
if not DIST or DIST == 'linda4':
    MODE = 'offline'
else:
    MODE = os.environ.get("SMHI_MODE", 'offline')

CONF = ConfigParser.ConfigParser()
CFG_FILE = os.path.join(CFG_DIR, "mesan_sat_config.cfg")
if not os.path.exists(CFG_FILE):
    raise IOError('Config file %s does not exist!' % CFG_FILE)
CONF.read(CFG_FILE)

OPTIONS = {}
for option, value in CONF.items(MODE, raw=True):
    OPTIONS[option] = value

TIME_WINDOW = int(OPTIONS.get('absolute_time_threshold_minutes', '30'))
MESAN_AREA_ID = OPTIONS.get('mesan_area_id', None)
DEFAULT_AREA = "mesanX"
if not MESAN_AREA_ID:
    LOG.warning("No area id specified in config file. Using default = " +
                str(DEFAULT_AREA))
    MESAN_AREA_ID = DEFAULT_AREA

servername = None
import socket
servername = socket.gethostname()
SERVERNAME = OPTIONS.get('servername', servername)

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

import sys
from urlparse import urlparse
import posttroll.subscriber
from posttroll.publisher import Publish
from posttroll.message import Message
import threading
import Queue
from datetime import timedelta, datetime
from mesan_compositer.composite_tools import get_analysis_time
from mesan_compositer import make_ct_composite as mcc

CFG_DIR = os.environ.get('MESAN_COMPOSITE_CONFIG_DIR', './')
DIST = os.environ.get("SMHI_DIST", None)
if not DIST or DIST == 'linda4':
    MODE = 'offline'
else:
    MODE = os.environ.get("SMHI_MODE", 'offline')

SENSOR = {'NOAA-19': 'avhrr/3',
          'NOAA-18': 'avhrr/3',
          'NOAA-15': 'avhrr/3',
          'Metop-A': 'avhrr/3',
          'Metop-B': 'avhrr/3',
          'Metop-C': 'avhrr/3',
          'EOS-Terra': 'modis',
          'EOS-Aqua': 'modis',
          'Suomi-NPP': 'viirs',
          'JPSS-1': 'viirs'}

SATELLITES = SENSOR.keys()

GEO_SATS = ['Meteosat-10', 'Meteosat-9', 'Meteosat-8', 'Meteosat-11', ]


class MesanCompRunError(Exception):
    pass


def reset_job_registry(objdict, key):
    """Remove job key from registry"""
    LOG.debug("Release/reset job-key " + str(key) + " from job registry")
    if key in objdict:
        objdict.pop(key)
    else:
        LOG.warning("Nothing to reset/release - " +
                    "Register didn't contain any entry matching: " +
                    str(key))
    return


class FilePublisher(threading.Thread):

    """A publisher for the MESAN composite result files. Picks up the return value
    from the ctype_composite_worker when ready, and publishes the files via posttroll

    """

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.jobs = {}

    def stop(self):
        """Stops the file publisher"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        with Publish('mesan_composite_runner', 0, ['netCDF', ]) as publisher:

            while self.loop:
                retv = self.queue.get()

                if retv != None:
                    LOG.info("Publish the files...")
                    publisher.send(retv)


class FileListener(threading.Thread):

    """A file listener class, to listen for incoming messages with a 
    relevant file for further processing"""

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue

    def stop(self):
        """Stops the file listener"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        with posttroll.subscriber.Subscribe('', ['CF/2', ], True) as subscr:

            for msg in subscr.recv(timeout=90):
                if not self.loop:
                    break

                # Check if it is a relevant message:
                if self.check_message(msg):
                    LOG.info("Put the message on the queue...")
                    LOG.debug("Message = " + str(msg))
                    self.queue.put(msg)

    def check_message(self, msg):

        if not msg:
            return False

        if ('platform_name' not in msg.data or
                'orbit_number' not in msg.data or
                'start_time' not in msg.data):
            LOG.warning("Message is lacking crucial fields...")
            return False

        if (msg.data['platform_name'] not in SATELLITES):
            LOG.info(str(msg.data['platform_name']) + ": " +
                     "Not a NOAA/Metop/S-NPP/Terra/Aqua scene. Continue...")
            return False

        return True


def ready2run(msg, files4comp, job_register, sceneid, product='CT'):
    """Check whether we can start a composite generation on scene"""

    from trollduction.producer import check_uri

    LOG.debug("Ready to run?")
    LOG.info("Got message: " + str(msg))

    if msg.type == 'file':
        uri = (msg.data['uri'])
    else:
        LOG.debug(
            "Ignoring this type of message data: type = " + str(msg.type))
        return False

    try:
        file4mesan = check_uri(uri)
    except IOError:
        LOG.info('Requested file not present on this host!')
        return False

    if 'start_time' not in msg.data:
        LOG.warning("No start time in message!")
        return False

    if (msg.data['platform_name'] not in SATELLITES or
            msg.data['sensor'] != SENSOR.get(msg.data['platform_name'],
                                             'avhrr/3') or
            not msg.data['uid'].startswith('S_NWC_' + product + '_')):

        LOG.debug("Scene and file is not applicable")
        LOG.debug("platform and instrument: " +
                  str(msg.data['platform_name']) + " " +
                  str(msg.data['sensor']))
        LOG.debug("Product requested: " + str(product))
        return False

    LOG.debug("Scene identifier = " + str(sceneid))
    LOG.debug("Job register = " + str(job_register))
    if sceneid in job_register and job_register[sceneid]:
        LOG.debug("Processing of scene " + str(sceneid) +
                  " have already been launched...")
        return False

    if sceneid not in files4comp:
        files4comp[sceneid] = []

    files4comp[sceneid].append(file4mesan)

    LOG.info("Files ready for Mesan composite: " +
             str(files4comp[sceneid]))

    job_register[sceneid] = datetime.utcnow()
    return True


def ctype_composite_worker(semaphore_obj, scene, job_id, publish_q):
    """Spawn/Start a Mesan composite generation on a new thread if available"""

    try:
        LOG.debug("Waiting for acquired semaphore...")
        with semaphore_obj:
            LOG.debug("Acquired semaphore")

            # Get the time of analysis from start and end times:
            time_of_analysis = get_analysis_time(
                scene['starttime'], scene['endtime'])
            delta_t = timedelta(minutes=TIME_WINDOW)

            LOG.info("Make composite for area id = " + str(MESAN_AREA_ID))
            ctcomp = mcc.ctCompositer(time_of_analysis, delta_t, MESAN_AREA_ID)
            ctcomp.get_catalogue()
            ctcomp.make_composite()
            ctcomp.write()
            ctcomp.make_quicklooks()

            result_file = ctcomp.filename
            to_send = {}
            to_send['uri'] = ('ssh://%s/%s' % (SERVERNAME, result_file))
            to_send['uid'] = ctcomp.filename
            to_send['sensor'] = scene.get('instrument')
            if not to_send['sensor']:
                to_send['sensor'] = scene.get('sensor')

            to_send['platform_name'] = scene['platform_name']
            to_send['orbit_number'] = scene.get('orbit_number')
            to_send['type'] = 'netCDF'
            to_send['data_processing_level'] = '3'
            environment = MODE
            to_send['start_time'], to_send['end_time'] = scene[
                'starttime'], scene['endtime']
            # Hardcoded station! Norrkoping. FIXME!
            pubmsg = Message('/' + to_send['format'] + '/' + to_send['data_processing_level'] +
                             '/norrköping/' + environment +
                             '/polar/direct_readout/',
                             "file", to_send).encode()
            LOG.debug("sending: " + str(pubmsg))
            LOG.info("Sending: " + str(pubmsg))
            publish_q.put(pubmsg)

            if isinstance(job_id, datetime):
                dt_ = datetime.utcnow() - job_id
                LOG.info("PPS on scene " + str(job_id) +
                         " finished. It took: " + str(dt_))
            else:
                LOG.warning(
                    "Job entry is not a datetime instance: " + str(job_id))

    except:
        LOG.exception('Failed in ctype_composite_worker...')
        raise


def mesan_live_runner():
    """Listens and triggers processing"""

    LOG.info("*** Start the runner for the Mesan composite generator:")

    sema = threading.Semaphore(5)
    listener_q = Queue.Queue()
    publisher_q = Queue.Queue()

    pub_thread = FilePublisher(publisher_q)
    pub_thread.start()
    listen_thread = FileListener(listener_q)
    listen_thread.start()

    composite_files = {}
    threads = []
    jobs_dict = {}
    while True:

        try:
            msg = listener_q.get()
        except Queue.Empty:
            continue

        LOG.debug(
            "Number of threads currently alive: " + str(threading.active_count()))

        start_time = msg.data['start_time']
        if 'end_time' in msg.data:
            end_time = msg.data['end_time']
        else:
            LOG.warning("No end time in message!")
            end_time = None

        sensor = str(msg.data['sensor'])
        platform_name = msg.data['platform_name']
        if platform_name not in GEO_SATS:
            orbit_number = int(msg.data['orbit_number'])
            LOG.info("Polar satellite: " + str(platform_name))
        else:
            orbit_number = '00000'
            LOG.info("Geostationary satellite: " + str(platform_name))

        keyname = (str(platform_name) + '_' +
                   str(sensor) + '_' +
                   str(orbit_number) + '_' +
                   str(start_time.strftime('%Y%m%d%H%M')))

        status = ready2run(msg, composite_files,
                           jobs_dict, keyname, 'CT')

        if status:
            # Start Cloudtype composite generation:

            urlobj = urlparse(msg.data['uri'])
            path, fname = os.path.split(urlobj.path)
            LOG.debug("path " + str(path) + " filename = " + str(fname))

            scene = {'platform_name': platform_name,
                     'orbit_number': orbit_number,
                     'starttime': start_time, 'endtime': end_time,
                     'sensor': sensor,
                     'filename': urlobj.path,
                     'product': 'CT'}

            if keyname not in jobs_dict:
                LOG.warning("Scene-run seems unregistered! Forget it...")
                continue

            t__ = threading.Thread(target=ctype_composite_worker, args=(sema, scene,
                                                                        jobs_dict[
                                                                            keyname],
                                                                        publisher_q))
            threads.append(t__)
            t__.start()

        # with Publish('mesan_composite_runner', 0) as publisher:
        #     composites = {}
        #     for msg in subscr.recv():
        #         composites = make_composite(composites,
        #                                     publisher, msg)

        #         # Clean the registry composites at some point...
        #         # FIXME!

        #         # # Block any future run on this scene for x minutes from now
        #         # # x = 20
        #         # thread_job_registry = threading.Timer(
        #         #     20 * 60.0, reset_job_registry, args=(composites, keyname))
        #         # thread_job_registry.start()

    LOG.info("Wait till all threads are dead...")
    while True:
        workers_ready = True
        for thread in threads:
            if thread.is_alive():
                workers_ready = False

        if workers_ready:
            break

    pub_thread.stop()
    listen_thread.stop()


if __name__ == "__main__":

    handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('mesan_composite_runner')

    mesan_live_runner()
