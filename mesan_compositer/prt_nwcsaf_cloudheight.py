#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015-2024 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <Firstname.Lastname @ smhi.se>

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

"""Make cloud height super observations.

From the cloud top temperature and height composite retrieve super
observations of cloud height and print to stdout
"""

import argparse
import logging
import os
import shutil
import sys
import tempfile
from datetime import datetime
from logging import handlers

import dask.array as da
import numpy as np
import xarray as xr

from mesan_compositer.config import get_config

LOG = logging.getLogger(__name__)

#: Default time format
_DEFAULT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

#: Default log format
_DEFAULT_LOG_FORMAT = "[%(levelname)s: %(asctime)s : %(name)s] %(message)s"

# min 8 x 8 pixels in super obs
DLENMIN = 4


def get_arguments():
    """Get command line arguments.

    args.logging_conf_file, args.config_file, obs_time, area_id, wsize

    Return:
      File path of the logging.ini file
      File path of the application configuration file
      Observation/Analysis time
      Area id
      Window size

    """
    parser = argparse.ArgumentParser()

    parser = argparse.ArgumentParser()
    parser.add_argument("--datetime", "-d", help="Date and time of observation - yyyymmddhh",
                        required=True)
    parser.add_argument("--area_id", "-a", help="Area id",
                        required=True)
    parser.add_argument("--size", "-s", help="Size of integration area in pixels",
                        required=True)
    parser.add_argument("-c", "--config_file",
                        type=str,
                        dest="config_file",
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

    wsize = args.size
    area_id = args.area_id
    obs_time = datetime.strptime(args.datetime, "%Y%m%d%H")
    if "template" in args.config_file:
        print("Template file given as master config, aborting!")
        sys.exit()

    return args.logging_conf_file, args.config_file, obs_time, area_id, wsize


def derive_sobs(ctth_comp, npix, filepath):
    """Derive the super observations and print data to file."""
    # non overlapping super observations
    # min 8x8 pixels = ca 8x8 km = 2*dlen x 2*dlen pixels for a
    # superobservation
    dlen = int(np.ceil(float(npix) / 2.0))
    dx = int(max(2 * DLENMIN, 2 * dlen))
    dy = dx
    LOG.info("\tUsing %d x %d pixels in a superobservation", dx, dy)

    # Get the lon,lat:
    lons, lats = ctth_comp.lon, ctth_comp.lat
    height = xr.DataArray(data=ctth_comp.data, dims=["y", "x"])
    height = height.coarsen({"y": dy, "x": dx}, boundary="trim").mean(skipna=True)

    so_lon = lons[int(dy/2)::dy, int(dx/2)::dx]
    so_lat = lats[int(dy/2)::dy, int(dx/2)::dx]

    height = da.nan_to_num(height, nan=-1.0).astype("int32")
    with  tempfile.NamedTemporaryFile(suffix=("_" + os.path.basename(filepath)),
                                      dir=os.path.dirname(filepath),
                                      mode='w', delete=False) as file_obj:
        write_data(file_obj, so_lon, so_lat, height)

    now = datetime.utcnow()
    fname_with_timestamp = str(filepath) + now.strftime("_%Y%m%d%H%M%S")
    # Change the file permissions to match current umask:
    umask = os.umask(0o666)
    os.umask(umask)
    os.chmod(file_obj.name, 0o666 & ~umask)

    shutil.copy(file_obj.name, fname_with_timestamp)
    os.rename(file_obj.name, filepath)


def write_data(fileobj, longitudes, latitudes, clheight):
    """Write the cloud top height data to file name."""
    cortyp = 1
    sd_ = 999.9

    # Create a Dataset with lon, lat and cloud top height in meters:
    shape = clheight.shape
    # height_ds = xr.Dataset(data_vars={"clheight": clheight,
    #                                   "lon": longitudes[:shape[0], :shape[1]],
    #                                   # 'lat': latitudes[:shape[0], :shape[1]],
    #                                   "minus_sixti": xr.DataArray(data=(np.ones(shape)*-60).astype("int32"),
    #                                                               dims=["y", "x"]),
    #                                   "minus_999": xr.DataArray(data=(np.ones(shape)*-999).astype("int32"),
    #                                                             dims=["y", "x"]),
    #                                   "five_nines": xr.DataArray(data=(np.ones(shape)*99999).astype("int32"),
    #                                                              dims=["y", "x"]),
    #                                   "sdv": xr.DataArray(data=np.ones(shape)*sd_,
    #                                                       dims=["y", "x"]),
    #                                   "cortyp": xr.DataArray(data=(np.ones(shape)*cortyp).astype("int32"),
    #                                                          dims=["y", "x"])
    #                                   }
    #                        )

    # df = height_ds.to_dataframe()

    height = clheight.data
    # height = clheight.data.compute()

    for y in range(shape[0]):
        yidx = shape[0]-1-y
        for x in range(shape[1]):
            xidx = x
            if height[yidx, xidx] < 0:
                continue

            result = "%8d %7.2f %7.2f %5d %d %d %8.2f %8.2f\n" % \
                (99999, latitudes[yidx, xidx], longitudes[yidx, xidx], -999, cortyp, -60,
                 height[yidx, xidx], sd_)
            fileobj.write(result)

if __name__ == "__main__":

    (logfile, config_filename, obstime, areaid, window_size) = get_arguments()

    if logfile:
        logging.config.fileConfig(logfile)

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)

    if not logfile:
        formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                      datefmt=_DEFAULT_TIME_FORMAT)
        handler.setFormatter(formatter)

    logging.getLogger("").addHandler(handler)
    logging.getLogger("").setLevel(logging.DEBUG)

    LOG = logging.getLogger("prt_nwcsaf_cloudheight")

    log_handlers = logging.getLogger("").handlers
    for log_handle in log_handlers:
        if type(log_handle) is handlers.SMTPHandler:
            LOG.debug("Mail notifications to: %s", str(log_handle.toaddrs))

    OPTIONS = get_config(config_filename)

    values = {"area": areaid, }
    bname = obstime.strftime(OPTIONS["ctth_composite_filename"]) % values
    path = OPTIONS["composite_output_dir"]
    filename = os.path.join(path, bname) + ".nc"
    if not os.path.exists(filename):
        LOG.error("File " + str(filename) + " does not exist!")
        sys.exit(-1)

    # Load the Cloud Height composite from file
    from netcdf_io import cloudComposite

    #ctth = cloudComposite(filename, "CTTH_ALTI", areaname=areaid)
    ctth = cloudComposite(filename, "CTTH_ALTI_group", areaname=areaid)

    ctth.load()

    NPIX = int(window_size)

    bname = obstime.strftime(OPTIONS["cloudheight_filename"]) % values
    path = OPTIONS["composite_output_dir"]
    filename = os.path.join(path, bname + ".dat")
    derive_sobs(ctth, NPIX, filename)
