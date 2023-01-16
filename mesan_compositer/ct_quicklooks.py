#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 - 2019, 2023 Adam.Dybbroe

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

"""Make quick look images of the cloudtype composite."""

import argparse
from datetime import datetime
import numpy as np
import xarray as xr
import dask.array as da
from trollimage.xrimage import XRImage
from mesan_compositer import get_config
from satpy.composites import PaletteCompositor
from mesan_compositer import nwcsaf_cloudtype
from mesan_compositer.netcdf_io import ncCloudTypeComposite
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
    Get command line arguments.

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


def make_quicklooks(netcdf_filename, cloudtype, ids, weights):
    """Make Cloudtype composite quicklook imagery.

    A cloudtype composite image is created along side images of the id's (MSG
    or PPS) and pixel weights.

    """
    palette = nwcsaf_cloudtype()

    # Cloud type field:
    attrs = {'_FillValue': np.nan, 'valid_range': (0, 20)}
    palette_attrs = {'palette_meanings': list(range(21))}

    pdata = xr.DataArray(palette, attrs=palette_attrs)

    # xdata = xr.DataArray(cloudtype.data, dims=['y', 'x'], attrs=attrs)
    masked_data = np.ma.masked_outside(cloudtype.data, 0, 20)
    xdata = xr.DataArray(da.from_array(masked_data), dims=['y', 'x'], attrs=attrs)

    pcol = PaletteCompositor('mesan_cloudtype_composite')((xdata, pdata))
    ximg = XRImage(pcol)
    ximg.save(netcdf_filename.strip('.nc') + '_cloudtype.png')

    # Id field:
    pdata = xr.DataArray(palette, attrs=palette_attrs)
    data = (ids.data * 13).astype(np.dtype('uint8'))
    xdata = xr.DataArray(da.from_array(data), dims=['y', 'x'], attrs=attrs)
    pcol = PaletteCompositor('mesan_cloudtype_composite')((xdata, pdata))
    ximg = XRImage(pcol)
    ximg.save(netcdf_filename.strip('.nc') + '_id.png')

    # Weight field:
    pdata = xr.DataArray(palette, attrs=palette_attrs)
    data = (weights.data * 20).astype(np.dtype('uint8'))
    xdata = xr.DataArray(da.from_array(data), dims=['y', 'x'], attrs=attrs)
    pcol = PaletteCompositor('mesan_cloudtype_composite')((xdata, pdata))
    ximg = XRImage(pcol)
    ximg.save(netcdf_filename.strip('.nc') + '_weight.png')

    return


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

    LOG = logging.getLogger('ct_quicklooks')

    log_handlers = logging.getLogger('').handlers
    for log_handle in log_handlers:
        if type(log_handle) is handlers.SMTPHandler:
            LOG.debug("Mail notifications to: %s", str(log_handle.toaddrs))

    OPTIONS = get_config(config_filename)

    values = {"area": areaid, }
    bname = time_of_analysis.strftime(OPTIONS['ct_composite_filename']) % values
    path = OPTIONS['composite_output_dir']
    FILENAME = os.path.join(path, bname) + '.nc'
    if not os.path.exists(FILENAME):
        LOG.error("File " + str(FILENAME) + " does not exist!")
        sys.exit(-1)

    comp = ncCloudTypeComposite()
    comp.load(FILENAME)

    make_quicklooks(FILENAME, comp.cloudtype, comp.id, comp.weight)
