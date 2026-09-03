#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014-2026 Adam.Dybbroe

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

"""Generate and print cloud amount super observations to ascii.

From the cloud type composite retrieve super observations of cloud
amount/cover and print to stdout.
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

# thresholds
FPASS = 0.5    # min fraction of valid obs in a superob
QPASS = 0.05   # min quality in a superobs
OPASS = 0.25   # min fraction opaque in CT std calc
LATMIN = -90.0
LATMAX = 90.0
LONMIN = -180.0
LONMAX = 180.0

# cloud cover observation error [%]
SDcc = 0.15   # All cloud types

# NWCSAF/Geo:
# ct:comment = "1:  Cloud-free land; 2:  Cloud-free sea; 3:  Snow over land;  4:  Sea ice; 5:  Very low clouds; 6:  Low clouds; 7:  Mid-level clouds;  8:  High opaque clouds; 9:  Very high opaque clouds;  10:  Fractional clouds; 11:  High semitransparent thin clouds;  12:  High semitransparent moderately thick clouds;  13:  High semitransparent thick clouds;  14:  High semitransparent above low or medium clouds;  15:  High semitransparent above snow/ice" ; # noqa

# NWCSAF/PPS:
# ct:flag_meanings = "Cloud-free_land Cloud-free_sea Snow_over_land Sea_ice Very_low_clouds Low_clouds Mid-level_clouds High_opaque_clouds Very_high_opaque_clouds Fractional_clouds High_semitransparent_very_thin_clouds High_semitransparent_thin_clouds High_semitransparent_thick_clouds High_semitransparent_above_low_or_medium_clouds High_semitransparent_above_snow_or_ice" ; # noqa

# ipar= 71; total cloud cover: cloud amount per type
ntctypecl = np.array([
    # 0.0,  # 00 Not processed
    np.nan,  # 00 Not processed
    0.0,  # 01 Cloud free land
    0.0,  # 02 Cloud free sea
    0.0,  # 03 Snow/ice contaminated land
    0.0,  # 04 Snow/ice contaminated sea
    1.0,  # 05 Very low clouds
    1.0,  # 06 Low clouds
    1.0,  # 07 Medium level clouds
    1.0,  # 08 High and opaque clouds
    1.0,  # 09 Very high and opaque clouds
    1.0,  # 10 Fractional clouds
    1.0,  # 11 High semitransparent thin clouds;
    1.0,  # 12 High semitransparent moderately thick clouds
    1.0,  # 13 High semitransparent thick clouds
    1.0,  # 14 High semitransparent above low or medium clouds
    1.0   # 15 High semitransparent above snow/ice
])

# ipar= 73; low level cloud cover: cloud amount per type
nlctypecl = np.array([
    0.0,  # 00 Not processed
    0.0,  # 01 Cloud free land
    0.0,  # 02 Cloud free sea
    0.0,  # 03 Snow/ice contaminated land
    0.0,  # 04 Snow/ice contaminated sea
    1.0,  # 05 Very low clouds
    1.0,  # 06 Low clouds
    0.5,  # 07 Medium level clouds
    0.5,  # 08 High and opaque clouds
    0.5,  # 09 Very high and opaque clouds
    .75,  # 10 Fractional clouds
    0.0,  # 11 High semitransparent thin clouds;
    0.0,  # 12 High semitransparent moderately thick clouds
    0.0,  # 13 High semitransparent thick clouds
    0.0,  # 14 High semitransparent above low or medium clouds
    0.5   # 15 High semitransparent above snow/ice
])

# ipar= 74; medium level cloud cover: cloud amount per type
nmctypecl = np.array([
    0.0,  # 00 Not processed
    0.0,  # 01 Cloud free land
    0.0,  # 02 Cloud free sea
    0.0,  # 03 Snow/ice contaminated land
    0.0,  # 04 Snow/ice contaminated sea
    0.0,  # 05 Very low clouds
    0.0,  # 06 Low clouds
    1.0,  # 07 Medium level clouds
    .75,  # 08 High and opaque clouds
    .75,  # 09 Very high and opaque clouds
    .25,  # 10 Fractional clouds
    0.0,  # 11 High semitransparent thin clouds;
    0.0,  # 12 High semitransparent moderately thick clouds
    .25,  # 13 High semitransparent thick clouds
    0.5,  # 14 High semitransparent above low or medium clouds
    0.5   # 15 High semitransparent above snow/ice
])

# ipar= 75; high level cloud cover: cloud amount per type
nhctypecl = np.array([
    0.0,  # 00 Not processed
    0.0,  # 01 Cloud free land
    0.0,  # 02 Cloud free sea
    0.0,  # 03 Snow/ice contaminated land
    0.0,  # 04 Snow/ice contaminated sea
    0.0,  # 05 Very low clouds
    0.0,  # 06 Low clouds
    0.0,  # 07 Medium level clouds
    1.0,  # 08 High and opaque clouds
    1.0,  # 09 Very high and opaque clouds
    .25,  # 10 Fractional clouds
    1.0,  # 11 High semitransparent thin clouds;
    1.0,  # 12 High semitransparent moderately thick clouds
    1.0,  # 13 High semitransparent thick clouds
    1.0,  # 14 High semitransparent above low or medium clouds
    1.0   # 15 High semitransparent above snow/ice
])

nctypecl = {"71": ntctypecl, "73": nlctypecl, "74": nmctypecl, "75": nhctypecl}


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
                        help="The netcdf file path of the cloud type composite.")
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


def derive_sobs(ct_comp, ipar, npix, resultfile):
    """Derive the super observations and print data to file."""
    # Get the lon,lat:
    lons, lats = ct_comp.lon, ct_comp.lat
    ctype = da.nan_to_num(ct_comp.data).astype("int32")
    clamount = nctypecl[ipar][ctype]

    # non overlapping superobservations
    # min 8x8 pixels = ca 8x8 km = 2*dlen x 2*dlen pixels for a
    # superobservation
    dlen = int(np.ceil(float(npix) / 2.0))
    dx = int(max(2 * DLENMIN, 2 * dlen))
    dy = dx
    LOG.info("\tUsing %d x %d pixels in a superobservation", dx, dy)

    clamount = xr.DataArray(data=clamount, dims=["y", "x"])
    clamount = clamount.coarsen({"y": dy, "x": dx}, boundary="trim").mean()

    so_lon = lons[int(dy/2)::dy, int(dx/2)::dx]
    so_lat = lats[int(dy/2)::dy, int(dx/2)::dx]

    with tempfile.NamedTemporaryFile(suffix=("_" + os.path.basename(resultfile)),
                                     dir=os.path.dirname(resultfile),
                                     mode="w", delete=False) as file_obj:
        write_data(file_obj, so_lon, so_lat, clamount)

    now = dt.datetime.now(dt.timezone.utc)
    fname_with_timestamp = str(resultfile) + now.strftime("_%Y%m%d%H%M%S")
    # Change the file permissions to match current umask:
    umask = os.umask(0o666)
    os.umask(umask)
    os.chmod(file_obj.name, 0o666 & ~umask)

    shutil.copy(file_obj.name, fname_with_timestamp)
    os.rename(file_obj.name, resultfile)


def write_data(fileobj, longitudes, latitudes, clamount):
    """Write the data to file name."""
    cortyp = 10
    SDcc = 0.15

    # Create a Dataset with lon, lat and cloud amount:
    shape = clamount.shape


    if USE_LEGACY_WRITING:
        for y in range(shape[0]):
            yidx = shape[0]-1-y
            for x in range(shape[1]):
                xidx = x
                if np.isnan(clamount.data[yidx, xidx]):
                    continue

                result = "%8d %7.2f %7.2f %5d %2.2d %2.2d %8.2f %8.2f\n" % \
                    (99999, latitudes[yidx, xidx], longitudes[yidx, xidx], -999, cortyp, -60,
                     clamount.data[yidx, xidx], SDcc)
                fileobj.write(result)
    else:
        # Reverse the array to fit with the old format, going from south to north (y-axis first):
        clamount_ = np.asarray(clamount)[::-1]
        latitudes_ = np.asarray(latitudes[:shape[0], :shape[1]])[::-1]
        longitudes_ = np.asarray(longitudes[:shape[0], :shape[1]])[::-1]

        valid = ~np.isnan(clamount_)

        data = np.column_stack(
            [
                np.full(valid.sum(), 99999),
                latitudes_[valid],
                longitudes_[valid],
                np.full(valid.sum(), -999),
                np.full(valid.sum(), cortyp),
                np.full(valid.sum(), -60),
                clamount_[valid],
                np.full(valid.sum(), SDcc),
            ]
        )

        np.savetxt(fileobj, data, fmt="%8d %7.2f %7.2f %5d %2.2d %2.2d %8.2f %8.2f")


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

    file_parser = Parser(config_options["cloudamount_filename"])
    bname = file_parser.compose({"area":area_id, "obstime":time_of_analysis})

    path = config_options["composite_output_dir"]
    filename = os.path.join(path, bname)

    derive_sobs(ctype, ipar, npix, filename)
    return filename


def main():
    """Generate the cloud amount super observations."""
    cmd_args = get_arguments()
    setup_logging(cmd_args)

    configuration = get_config(cmd_args.config_file)

    file_parser = Parser(configuration["ct_composite_filename"])
    filename = cmd_args.netcdf_filepath

    res = file_parser.parse(Path(filename).name.strip(".nc"))
    areaid = res["area"]
    time_of_analysis = res["obstime"]

    if not os.path.exists(filename):
        LOG.error("File " + str(filename) + " does not exist!")
        return

    resultfile = do_cloudamount(filename, time_of_analysis, areaid, configuration)
    LOG.info("Cloud amount super observations generated: %s", resultfile)


if __name__ == "__main__":
    main()
