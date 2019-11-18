#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015 - 2019 Adam.Dybbroe

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

"""Make cloud height super observations.

From the cloud top temperature and height composite retrieve super
observations of cloud height and print to stdout
"""

import numpy as np
import argparse
from datetime import datetime
import os
import sys
import tempfile
import shutil
import logging
from logging import handlers
from mesan_compositer.netcdf_io import ncCTTHComposite
from mesan_compositer.pps_msg_conversions import get_bit_from_flags
from mesan_compositer import get_config


class cthError(Exception):
    """Cloud Top Height exception."""

    pass


LOG = logging.getLogger(__name__)


#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'


# min 8 x 8 pixels in super obs
DLENMIN = 4

# Thresholds
FPASS = 0.5  # min fraction of valid obs in a superob
QPASS = 0.05  # min quality in a superobs
OPASS = 0.25  # min fraction opaque in CT std calc


def get_arguments():
    """Get command line arguments.

    args.logging_conf_file, args.config_file, obs_time, area_id, wsize

    Return
      File path of the logging.ini file
      File path of the application configuration file
      Observation/Analysis time
      Area id
      Window size

    """
    parser = argparse.ArgumentParser()

    parser = argparse.ArgumentParser()
    parser.add_argument('--datetime', '-d', help='Date and time of observation - yyyymmddhh',
                        required=True)
    parser.add_argument('--area_id', '-a', help='Area id',
                        required=True)
    parser.add_argument('--size', '-s', help='Size of integration area in pixels',
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

    wsize = args.size
    area_id = args.area_id
    obs_time = datetime.strptime(args.datetime, '%Y%m%d%H')
    if 'template' in args.config_file:
        print("Template file given as master config, aborting!")
        sys.exit()

    return args.logging_conf_file, args.config_file, obs_time, area_id, wsize


def new_cloudtop(so_CTH, so_w):
    """Derive cloud top super observations with a new simplified approach.

    The weigting is done independent of the flags.
    """
    if so_CTH.max() == 0.0:
        return None

    # Get rid of data points which are masked out:
    so_w = np.ma.masked_array(so_w, mask=so_CTH.mask).compressed()
    so_CTH = so_CTH.compressed()
    top = np.sum(so_w*so_CTH)/np.sum(so_w)

    return top


def cloudtop(so_CTH, so_w, so_flg, num_of_datapoints):
    """Derive cloud top height super observations using the old method but not using the flags."""
    # cloud top observation error [m] sd= a*top+b
    # SDct_01a = 0.065  # 50  Opaque cloud
    # SDct_01b = 385    # 50  Opaque cloud
    # SDct_02a = 0.212  # 150 Windowing technique applied
    # SDct_02b = 1075   # 150 Windowing technique applied

    # Get rid of data points which are masked out:
    so_flg = np.ma.masked_array(so_flg, mask=so_CTH.mask).compressed()
    # Corresponds to where the weight is 0:
    so_w = np.ma.masked_array(so_w, mask=so_CTH.mask).compressed()
    so_CTH = so_CTH.compressed()

    # nfound = len(so_CTH)

    # unique top values
    u_cth = np.unique(so_CTH)

    # weight sum for each unique height
    w_cth = [np.sum(so_w[so_CTH == u_cth[i]]) for i in range(len(u_cth))]
    # n_cth = [np.sum(so_CTH == u_cth[i]) for i in range(len(u_cth))]

    # top value associated with largest weight sum
    # wsmax = np.max(w_cth)
    imax = np.argmax(w_cth)
    top = u_cth[imax]

    return top, 999.9

    # # nof obs with this cloud height
    # ntop = n_cth[imax]

    # # observation quality
    # q = wsmax / (nfound + 1e-6)

    # # flags associated with largest weight sum
    # flgs = so_flg[so_CTH == u_cth[imax]]

    # # find dominating method, opaque or non-opaque but window
    # nopaque = np.sum(get_bit_from_flags(flgs, 2))
    # nwindow = np.sum(
    #     (0 == get_bit_from_flags(flgs, 2)) & get_bit_from_flags(flgs, 8))

    # if (ntop != (nopaque + nwindow)):
    #     # LOG.warning("Inconsistency in opaque and window flags: " +
    #     #            "ntop=%d, nopaque=%d nwindow=%d", ntop, nopaque, nwindow)
    #     # LOG.info("No super obs will be generated...")
    #     return 0, 0
    # else:
    #     fopaque = nopaque / np.float(ntop)

    # # check statistics and quality
    # if (nfound / np.float(num_of_datapoints) > FPASS) and (q >= QPASS):
    #     if (fopaque > OPASS):
    #         # opaque
    #         sd = SDct_01a * top + SDct_01b
    #     else:
    #         # windowing technique
    #         sd = SDct_02a * top + SDct_02b
    # else:
    #     top = 0
    #     sd = 0

    # # LOG.debug('wsmax=%.3f, top=%.1f, fopaque=%.3f, q=%f, nfound=%d',
    # #          wsmax, top, fopaque, q, nfound)
    # return top, sd


def derive_sobs(ctth_comp, npix, resultfile):
    """Derive the super observations and print data to file."""
    tmpfname = tempfile.mktemp(suffix=('_' + os.path.basename(resultfile)),
                               dir=os.path.dirname(resultfile))

    # Get the lon,lat:
    lon, lat = ctth_comp.area_def.get_lonlats()

    # isinstance(ctth_comp.height.data, numpy.ma.core.MaskedArray)
    try:
        ctth_height = ctth_comp.height.data.compute()
    except AttributeError:
        ctth_height = ctth_comp.height.data

    if not np.ma.is_masked(ctth_height):
        ctth_height = np.ma.masked_invalid(ctth_height, np.nan)
        ctth_height.fill_value = np.nan

    flags = ctth_comp.flags.data
    weight = ctth_comp.weight.data

    # non overlapping super observations
    # min 8x8 pixels = ca 8x8 km = 2*dlen x 2*dlen pixels for a
    # superobservation
    dlen = int(np.ceil(float(npix) / 2.0))
    dx = int(max(2 * DLENMIN, 2 * dlen))
    dy = dx
    LOG.info('\tUsing %d x %d pixels in a superobservation', dx, dy)

    # initialize superobs data */
    ny, nx = np.shape(ctth_height)

    # indices to super obs "midpoints"
    lx = np.arange(dlen, nx - dlen + 1, dx)
    ly = np.arange(ny - dlen, dlen - 1, -dy)

    so_lon = lon[np.ix_(ly, lx)]
    so_lat = lat[np.ix_(ly, lx)]

    npcount1 = 0
    npcount2 = 0

    so_tot = 0
    with open(tmpfname, 'w') as fpt:
        for iy in range(len(ly)):
            for ix in range(len(lx)):
                # super ob domain is: ix-dlen:ix+dlen-1, iy-dlen:iy+dlen-1
                x = lx[ix]
                y = ly[iy]
                so_x = np.arange(x - dlen, x + dlen - 1 + 1)
                so_y = np.arange(y - dlen, y + dlen - 1 + 1)
                so_cth = ctth_height[np.ix_(so_y, so_x)]

                so_w = weight[np.ix_(so_y, so_x)]
                so_flg = flags[np.ix_(so_y, so_x)]
                ii = (so_cth.filled() != so_cth.fill_value) & (
                    get_bit_from_flags(so_flg, 0) != 1)

                # any valid data?
                if np.sum(ii) == 0:
                    npcount1 += 1
                    continue

                if so_cth[ii].compressed().shape[0] == 0:
                    npcount1 += 2
                    continue

                # # calculate top and std
                # cth, sd = cloudtop(
                #     so_cth[ii], so_w[ii], so_flg[ii], np.prod(so_w.shape))

                # Calculate cloud top height for the super obs:
                cth = new_cloudtop(so_cth[ii], so_w[ii])
                sd = 999.9

                if not cth:
                    LOG.debug("iy, ix, so_y, so_x, so_lat, so_lon: %d %d %d %d %f %f",
                              iy, ix, y, x, so_lat[iy, ix], so_lon[iy, ix])
                else:
                    result = '%8d %7.2f %7.2f %5d %d %d %8.2f %8.2f\n' % \
                             (99999, so_lat[iy, ix], so_lon[iy, ix], -999, 1, -60,
                              cth, sd)
                    fpt.write(result)
                    so_tot += 1

    LOG.info("Number of omitted observations: npcount1=%d npcount2=%d",
             npcount1, npcount2)

    LOG.info('\tCreated %d superobservations', so_tot)

    now = datetime.utcnow()
    fname_with_timestamp = str(resultfile) + now.strftime('_%Y%m%d%H%M%S')
    shutil.copy(tmpfname, fname_with_timestamp)
    os.rename(tmpfname, resultfile)

    return


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

    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)

    LOG = logging.getLogger('prt_nwcsaf_cloudheight')

    log_handlers = logging.getLogger('').handlers
    for log_handle in log_handlers:
        if type(log_handle) is handlers.SMTPHandler:
            LOG.debug("Mail notifications to: %s", str(log_handle.toaddrs))

    OPTIONS = get_config(config_filename)

    values = {"area": areaid, }
    bname = obstime.strftime(OPTIONS['ctth_composite_filename']) % values
    path = OPTIONS['composite_output_dir']
    filename = os.path.join(path, bname) + '.nc'
    if not os.path.exists(filename):
        LOG.error("File " + str(filename) + " does not exist!")
        sys.exit(-1)

    # Load the Cloud Height composite from file
    COMP = ncCTTHComposite()
    COMP.load(filename)

    NPIX = int(window_size)

    bname = obstime.strftime(OPTIONS['cloudheight_filename']) % values
    path = OPTIONS['composite_output_dir']
    filename = os.path.join(path, bname + '.dat')
    derive_sobs(COMP, NPIX, filename)
