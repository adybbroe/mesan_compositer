#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015-2024, 2026 Adam.Dybbroe

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
import datetime as dt
import logging
import logging.config
import os
import shutil
import sys
import tempfile
from pathlib import Path

import dask.array as da
import numpy as np
import xarray as xr
from trollsift import Parser

from mesan_compositer.config import get_config
from mesan_compositer.logger import setup_logging
from mesan_compositer.netcdf_io import cloudComposite

DEFAULT_SUPEROBS_WINDOW_SIZE_NPIX = 32
USE_LEGACY_WRITING = False

LOG = logging.getLogger(__name__)

#: Default time format
_DEFAULT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

#: Default log format
_DEFAULT_LOG_FORMAT = "[%(levelname)s: %(asctime)s : %(name)s] %(message)s"

# min 8 x 8 pixels in super obs
DLENMIN = 4


def get_arguments():
    """Get command line arguments.

    args.logging_conf_file, args.config_file, args.obs_time, args.composite-filename

    Return:
      File path of the log-config yaml file
      File path of the application configuration file
      Observation/Analysis time


    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--netcdf_filepath",
                        type=str,
                        dest="netcdf_filepath",
                        required=True,
                        help="The netcdf file path of the cloud top height composite.")
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
        print("Template file given as master config, aborting!")  # noqa: T201
        sys.exit()

    return args


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
                                      mode="w", delete=False) as file_obj:
        write_data(file_obj, so_lon, so_lat, height)

    now = dt.datetime.now(dt.timezone.utc)
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

    shape = clheight.shape
    height = clheight.data

    if USE_LEGACY_WRITING:
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
    else:
        # Reverse the array to fit with the old format, going from south to north (y-axis first):
        height_ = np.asarray(height)[::-1]
        latitudes_ = np.asarray(latitudes[:shape[0], :shape[1]])[::-1]
        longitudes_ = np.asarray(longitudes[:shape[0], :shape[1]])[::-1]

        valid = height_ >= 0

        data = np.column_stack(
            [
                np.full(valid.sum(), 99999),
                latitudes_[valid],
                longitudes_[valid],
                np.full(valid.sum(), -999),
                np.full(valid.sum(), cortyp),
                np.full(valid.sum(), -60),
                height_[valid],
                np.full(valid.sum(), sd_),
            ]
        )

        np.savetxt(fileobj, data, fmt="%8d %7.2f %7.2f %5d %d %d %8.2f %8.2f")


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
    derive_sobs(ctth, npix, filename)
    return filename


if __name__ == "__main__":

    cmd_args = get_arguments()
    setup_logging(cmd_args)

    configuration = get_config(cmd_args.config_file)

    file_parser = Parser(configuration["ctth_composite_filename"])
    filename = cmd_args.netcdf_filepath

    res = file_parser.parse(Path(filename).name)
    areaid = res["area"]
    time_of_analysis = res["obstime"]

    if not os.path.exists(filename):
        LOG.error("File " + str(filename) + " does not exist!")
        sys.exit(-1)

    do_cloudheight(filename, time_of_analysis, areaid, configuration)
