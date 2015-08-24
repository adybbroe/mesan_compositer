#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c14526.ad.smhi.se>

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

"""Make quick look images of the cloudtype composite
"""

import argparse
from datetime import datetime
from mesan_compositer.netcdf_io import ncCloudTypeComposite
import sys
import os

import logging
LOG = logging.getLogger(__name__)

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

import ConfigParser

CFG_DIR = os.environ.get('MESAN_COMPOSITE_CONFIG_DIR', './')
DIST = os.environ.get("SMHI_DIST", None)
if not DIST or DIST == 'linda4':
    MODE = 'offline'
else:
    MODE = os.environ.get("SMHI_MODE", 'offline')


CONF = ConfigParser.ConfigParser()
CONFIGFILE = os.path.join(CFG_DIR, "mesan_sat_config.cfg")
if not os.path.exists(CONFIGFILE):
    raise IOError('Config file %s does not exist!' % CONFIGFILE)
CONF.read(CONFIGFILE)

OPTIONS = {}
for option, value in CONF.items(MODE, raw=True):
    OPTIONS[option] = value

_MESAN_LOG_FILE = OPTIONS.get('mesan_log_file', None)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--datetime', '-d',
                        help='Date and time of observation - yyyymmddhh',
                        required=True)
    parser.add_argument('--area_id', '-a', help='Area id',
                        required=True)

    args = parser.parse_args()

    from logging import handlers

    if _MESAN_LOG_FILE:
        ndays = int(OPTIONS["log_rotation_days"])
        ncount = int(OPTIONS["log_rotation_backup"])
        handler = handlers.TimedRotatingFileHandler(_MESAN_LOG_FILE,
                                                    when='midnight',
                                                    interval=ndays,
                                                    backupCount=ncount,
                                                    encoding=None,
                                                    delay=False,
                                                    utc=True)

        # handler.doRollover()
    else:
        handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('mpop').setLevel(logging.DEBUG)

    LOG = logging.getLogger('ct_quicklooks')

    obstime = datetime.strptime(args.datetime, '%Y%m%d%H')
    values = {"area": args.area_id, }
    bname = obstime.strftime(OPTIONS['ct_composite_filename']) % values
    path = OPTIONS['composite_output_dir']
    filename = os.path.join(path, bname) + '.nc'
    if not os.path.exists(filename):
        LOG.error("File " + str(filename) + " does not exist!")
        sys.exit(-1)

    comp = ncCloudTypeComposite()
    comp.load(filename)

    import mpop.imageo.palettes
    palette = mpop.imageo.palettes.cms_modified()
    from mpop.imageo import geo_image
    img = geo_image.GeoImage(comp.cloudtype.data,
                             args.area_id,
                             None,
                             fill_value=(0),
                             mode="P",
                             palette=palette)
    img.save(filename.strip('.nc') + '_cloudtype.png')

    comp_id = comp.id.data * 13
    idimg = geo_image.GeoImage(comp_id,
                               args.area_id,
                               None,
                               fill_value=(0),
                               mode="P",
                               palette=palette)
    idimg.save(filename.strip('.nc') + '_id.png')

    comp_w = comp.weight.data * 20
    wimg = geo_image.GeoImage(comp_w,
                              args.area_id,
                              None,
                              fill_value=(0),
                              mode="P",
                              palette=palette)
    wimg.save(filename.strip('.nc') + '_weight.png')
