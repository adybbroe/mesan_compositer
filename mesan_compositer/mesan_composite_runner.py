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


from datetime import datetime, timedelta
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


class MesanCompRunError(Exception):
    pass


def make_composite(mcomps,
                   mypublisher, message):
    """From a posttroll message start the modis lvl1 processing"""

    LOG.info("")
    LOG.info("composite dict: " + str(mcomps))
    LOG.info("\tMessage:")
    LOG.info(message)
    urlobj = urlparse(message.data['uri'])

    if 'start_time' in message.data:
        start_time = message.data['start_time']
        scene_id = start_time.strftime('%Y%m%d%H%M')
    else:
        LOG.warning("No start time in message!")
        start_time = None
        return mcomps

    if 'end_time' in message.data:
        end_time = message.data['end_time']
    else:
        LOG.warning("No end time in message!")
        end_time = None

    if (message.data['platform_name'] in SATELLITES and
            message.data['sensor'] == SENSOR.get(message.data['platform_name'],
                                                 'avhrr/3') and
            message.data['uid'].startswith('S_NWC_CT_')):

        path, fname = os.path.split(urlobj.path)
        LOG.debug("path " + str(path) + " filename = " + str(fname))

        instrument = str(message.data['sensor'])
        platform_name = message.data['platform_name']
        mcomps[scene_id] = os.path.join(path, fname)

    else:
        LOG.debug("Scene and file is not supported")
        LOG.debug("platform and instrument: " +
                  str(message.data['platform_name']) + " " +
                  str(message.data['sensor']))
        return mcomps

    LOG.info("Sat and Instrument: " + platform_name + " " + instrument)
    # prfx = platform_name.lower() + start_time.strftime("_%Y%m%d_%H")

    # Get the time of analysis from start and end times:
    time_of_analysis = get_analysis_time(start_time, end_time)
    delta_t = timedelta(minutes=TIME_WINDOW)

    LOG.info("Make composite for area id = " + str(MESAN_AREA_ID))
    ctcomp = mcc.ctCompositer(time_of_analysis, delta_t, MESAN_AREA_ID)
    ctcomp.get_catalogue()
    ctcomp.make_composite()
    ctcomp.write()

    return mcomps


def mesan_live_runner():
    """Listens and triggers processing"""

    LOG.info("*** Start the runner for the Mesan composite generator:")
    with posttroll.subscriber.Subscribe('', ['CF/2', ], True) as subscr:
        with Publish('mesan_composite_runner', 0) as publisher:
            composites = {}
            for msg in subscr.recv():
                composites = make_composite(composites,
                                            publisher, msg)

                # Clean the registry composites at some point...
                # FIXME!


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
