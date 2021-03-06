#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015, 2019 Adam.Dybbroe

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

"""Make quick look images of the ctth composite
"""

import argparse
from datetime import datetime
import numpy as np
import xarray as xr
from trollimage.xrimage import XRImage
from mesan_compositer import ctth_height
from mesan_compositer.netcdf_io import ncCTTHComposite
from mesan_compositer import get_config
from satpy.composites import ColormapCompositor
import sys
import os
from logging import handlers
import logging

LOG = logging.getLogger(__name__)

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'


def get_arguments():
    """
    Get command line arguments

    args.logging_conf_file, args.config_file, obs_time, area_id, wsize

    Return
      File path of the logging.ini file
      File path of the application configuration file
      Observation/Analysis time
      Area id
      Window size
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('--datetime', '-d', help='Date and time of observation - yyyymmddhh',
                        required=True)
    parser.add_argument('--area_id', '-a', help='Area id',
                        required=True)
    parser.add_argument('-c', '--config_file',
                        type=str,
                        dest='config_file',
                        required=True,
                        help="The file containing configuration parameters e.g. mesan_sat_config.yaml")
    parser.add_argument("-l", "--logging",
                        help="The path to the log-configuration file (e.g. './logging.ini')",
                        dest="logging_conf_file",
                        type=str,
                        required=False)
    parser.add_argument("-v", "--verbose",
                        help="print debug messages too",
                        action="store_true")

    args = parser.parse_args()

    tanalysis = datetime.strptime(args.datetime, '%Y%m%d%H')
    area_id = args.area_id
    if 'template' in args.config_file:
        print("Template file given as master config, aborting!")
        sys.exit()

    return args.logging_conf_file, args.config_file, tanalysis, area_id


if __name__ == "__main__":

    (logfile, config_filename, time_of_analysis, areaid) = get_arguments()

    if logfile:
        logging.config.fileConfig(logfile)

    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)

    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('satpy').setLevel(logging.INFO)

    LOG = logging.getLogger('ctth_quicklooks')

    log_handlers = logging.getLogger('').handlers
    for log_handle in log_handlers:
        if type(log_handle) is handlers.SMTPHandler:
            LOG.debug("Mail notifications to: %s", str(log_handle.toaddrs))

    OPTIONS = get_config(config_filename)

    values = {"area": areaid, }
    bname = time_of_analysis.strftime(OPTIONS['ctth_composite_filename']) % values
    path = OPTIONS['composite_output_dir']
    filename = os.path.join(path, bname) + '.nc'
    if not os.path.exists(filename):
        LOG.error("File " + str(filename) + " does not exist!")
        sys.exit(-1)

    comp = ncCTTHComposite()
    comp.load(filename)

    palette = ctth_height()

    ctth_data = comp.height.data
    ctth_data = ctth_data / 500.0 + 1
    ctth_data = ctth_data.astype(np.uint8)

    cmap = ColormapCompositor('mesan_cloudheight_composite')
    colors, sqpal = cmap.build_colormap(palette, np.uint8, {})

    attrs = {'_FillValue': 0}
    xdata = xr.DataArray(ctth_data, dims=['y', 'x'], attrs=attrs).astype('uint8')
    pimage = XRImage(xdata)
    pimage.palettize(colors)
    pimage.save(filename.strip('.nc') + '_height.png')
