#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014-2024 Adam.Dybbroe

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
import logging
import os
import sys
from datetime import datetime
from logging import handlers

import dask.array as da
import numpy as np
import xarray as xr

from mesan_compositer.config import get_config

FAST_AND_ROUGH = False

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

    args.logging_conf_file, args.config_file, obs_time, area_id, wsize

    Return:
      File path of the logging.ini file
      File path of the application configuration file
      Observation/Analysis time
      Area id
      Window size

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--datetime", "-d", help="Date and time of observation - yyyymmddhh",
                        required=True)
    parser.add_argument("--area_id", "-a", help="Area id",
                        required=True)
    parser.add_argument("--ipar", "-i", help="Parameter id",
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
    ipar = args.ipar
    obs_time = datetime.strptime(args.datetime, "%Y%m%d%H")
    if "template" in args.config_file:
        print("Template file given as master config, aborting!")
        sys.exit()

    return args.logging_conf_file, args.config_file, obs_time, area_id, wsize, ipar


def derive_sobs(ct_comp, ipar, npix, resultfile):
    """Derive the super observations and print data to file."""
    import shutil
    import tempfile

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
                                     mode='w', delete=False) as file_obj:
        write_data(file_obj, so_lon, so_lat, clamount)

    now = datetime.utcnow()
    fname_with_timestamp = str(resultfile) + now.strftime("_%Y%m%d%H%M%S")
    shutil.copy(file_obj.name, fname_with_timestamp)
    os.rename(file_obj.name, resultfile)


def write_data(fileobj, longitudes, latitudes, clamount):
    """Write the data to file name."""
    cortyp = 10
    SDcc = 0.15

    # Create a Dataset with lon, lat and cloud amount:
    shape = clamount.shape
    if FAST_AND_ROUGH:
        clamount_ds = xr.Dataset(data_vars={"clamount": clamount,
                                            "lon": longitudes[:shape[0], :shape[1]],
                                            # 'lat': latitudes[:shape[0], :shape[1]],
                                            "minus_sixti": xr.DataArray(data=(np.ones(shape)*-60).astype("int32"),
                                                                        dims=["y", "x"]),
                                            "minus_999": xr.DataArray(data=(np.ones(shape)*-999).astype("int32"),
                                                                      dims=["y", "x"]),
                                            "five_nines": xr.DataArray(data=(np.ones(shape)*99999).astype("int32"),
                                                                       dims=["y", "x"]),
                                            "SDcc": xr.DataArray(data=np.ones(shape)*SDcc,
                                                                 dims=["y", "x"]),
                                            "cortyp": xr.DataArray(data=(np.ones(shape)*cortyp).astype("int32"),
                                                                   dims=["y", "x"])
                                            }
                                 )

        df = clamount_ds.to_dataframe()
        df.to_csv(fileobj, columns=["five_nines", "latitude", "longitude",
                                    "minus_999", "cortyp", "minus_sixti", "clamount", "SDcc"],
                  sep=" ", index=False, header=False)

    else:
        for y in range(shape[0]):
            yidx = shape[0]-1-y
            for x in range(shape[1]):
                xidx = x
                if np.isnan(clamount.data[yidx, xidx]):
                    continue
                # print(latitudes[yidx, xidx], longitudes[yidx, xidx])
                result = "%8d %7.2f %7.2f %5d %2.2d %2.2d %8.2f %8.2f\n" % \
                    (99999, latitudes[yidx, xidx], longitudes[yidx, xidx], -999, cortyp, -60,
                     clamount.data[yidx, xidx], SDcc)
                fileobj.write(result)


if __name__ == "__main__":

    (logfile, config_filename, obstime, areaid, window_size, iparam) = get_arguments()

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

    LOG = logging.getLogger("prt_nwcsaf_cloudamount")

    log_handlers = logging.getLogger("").handlers
    for log_handle in log_handlers:
        if type(log_handle) is handlers.SMTPHandler:
            LOG.debug("Mail notifications to: %s", str(log_handle.toaddrs))

    OPTIONS = get_config(config_filename)

    values = {"area": areaid, }
    bname = obstime.strftime(OPTIONS["ct_composite_filename"]) % values
    path = OPTIONS["composite_output_dir"]
    filename = os.path.join(path, bname) + ".nc"
    if not os.path.exists(filename):
        LOG.error("File " + str(filename) + " does not exist!")
        sys.exit(-1)

    # Load the Cloud Type composite from file
    # ctype = load_ct_composite(filename, 'CT_group')
    from netcdf_io import cloudComposite

    # ctype = cloudComposite(filename, "CT", areaname=areaid)
    ctype = cloudComposite(filename, "CT_group", areaname=areaid)
    ctype.load()

    IPAR = str(iparam)
    NPIX = int(window_size)

    bname = obstime.strftime(OPTIONS["cloudamount_filename"]) % values
    path = OPTIONS["composite_output_dir"]
    filename = os.path.join(path, bname + ".dat")

    derive_sobs(ctype, IPAR, NPIX, filename)
