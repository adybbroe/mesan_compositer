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

"""From the cloud top temperature and height composite retrieve super
observations of cloud height and print to stdout

"""

import numpy as np
from mesan_compositer.netcdf_io import ncCTTHComposite
import argparse
from datetime import datetime
import os
import sys
import ConfigParser
import tempfile
import shutil

import logging
LOG = logging.getLogger(__name__)

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

CFG_DIR = os.environ.get('MESAN_COMPOSITE_CONFIG_DIR', './')
MODE = os.environ.get("SMHI_MODE", 'offline')

conf = ConfigParser.ConfigParser()
configfile = os.path.join(CFG_DIR, "mesan_sat_config.cfg")
if not os.path.exists(configfile):
    raise IOError('Config file %s does not exist!' % configfile)
conf.read(configfile)

OPTIONS = {}
for option, value in conf.items(MODE, raw=True):
    OPTIONS[option] = value

_MESAN_LOG_FILE = OPTIONS.get('mesan_log_file', None)


class cthError(Exception):
    pass

# min 8 x 8 pixels in super obs
DLENMIN = 4

# Thresholds
FPASS = 0.5  # min fraction of valid obs in a superob
QPASS = 0.05  # min quality in a superobs
OPASS = 0.25  # min fraction opaque in CT std calc


def get_bit_from_flags(arr, nbit):
    res = np.bitwise_and(np.right_shift(arr, nbit), 1)
    return res.astype('b')


def cloudtop(so_CTH, so_w, so_flg, num_of_datapoints):

    # cloud top observation error [m] sd= a*top+b
    SDct_01a = 0.065  # 50  Opaque cloud
    SDct_01b = 385    # 50  Opaque cloud
    SDct_02a = 0.212  # 150 Windowing technique applied
    SDct_02b = 1075   # 150 Windowing technique applied

    # Get rid of data points which are masked out:
    so_flg = np.repeat(so_flg, so_CTH.mask == False)
    # Corresponds to where the weight is 0:
    so_w = np.repeat(so_w, so_CTH.mask == False)
    so_CTH = so_CTH.compressed()

    nfound = len(so_CTH)

    # unique top values
    u_cth = np.unique(so_CTH)

    # weight sum for each unique height
    w_cth = [np.sum(so_w[so_CTH == u_cth[i]]) for i in range(len(u_cth))]
    n_cth = [np.sum(so_CTH == u_cth[i]) for i in range(len(u_cth))]

    # top value associated with largest weight sum
    wsmax = np.max(w_cth)
    imax = np.argmax(w_cth)
    top = u_cth[imax]

    # nof obs with this cloud height
    ntop = n_cth[imax]

    # observation quality
    q = wsmax / (nfound + 1e-6)

    # flags associated with largest weight sum
    flgs = so_flg[so_CTH == u_cth[imax]]

    # find dominating method, opaque or non-opaque but window
    nopaque = np.sum(get_bit_from_flags(flgs, 2))
    nwindow = np.sum(
        (0 == get_bit_from_flags(flgs, 2)) & get_bit_from_flags(flgs, 8))

    if (ntop != (nopaque + nwindow)):
        LOG.warning("Inconsistency in opaque and window flags: " +
                    "ntop=%d, nopaque=%d nwindow=%d", ntop, nopaque, nwindow)
        LOG.info("No super obs will be generated...")
        return 0, 0
    else:
        fopaque = nopaque / np.float(ntop)

    # check statistics and quality
    if (nfound / np.float(num_of_datapoints) > FPASS) and (q >= QPASS):
        if (fopaque > OPASS):
            # opaque
            sd = SDct_01a * top + SDct_01b
        else:
            # windowing technique
            sd = SDct_02a * top + SDct_02b
    else:
        top = 0
        sd = 0

    LOG.debug('wsmax=%.3f, top=%.1f, fopaque=%.3f, q=%f, nfound=%d',
              wsmax, top, fopaque, q, nfound)
    return top, sd


def derive_sobs(ctth_comp, npix, resultfile):
    """Derive the super observations and print data to file"""

    tmpfname = tempfile.mktemp(suffix=('_' + os.path.basename(resultfile)),
                               dir=os.path.dirname(resultfile))

    # Get the lon,lat:
    lon, lat = ctth_comp.area_def.get_lonlats()

    ctth_height = ctth_comp.height.data.astype('int')
    flags = ctth_comp.flags.data
    weight = ctth_comp.weight.data

    # non overlapping superobservations
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

    so_tot = 0
    with open(tmpfname, 'w') as fpt:
        for iy in range(len(ly)):
            for ix in range(len(lx)):
                # print ix, iy
                # if iy != 0 and ix != 58:
                #    continue

                # super ob domain is: ix-dlen:ix+dlen-1, iy-dlen:iy+dlen-1
                x = lx[ix]
                y = ly[iy]
                so_x = np.arange(x - dlen, x + dlen - 1 + 1)
                so_y = np.arange(y - dlen, y + dlen - 1 + 1)
                so_cth = ctth_height[np.ix_(so_y, so_x)]
                so_w = weight[np.ix_(so_y, so_x)]
                so_flg = flags[np.ix_(so_y, so_x)]
                so_cth.fill_value = 255
                ii = (so_cth.filled() != 255) & (
                    get_bit_from_flags(so_flg, 0) != 1)

                # any valid data?
                if np.sum(ii) == 0:
                    continue

                # calculate top and std
                cth, sd = cloudtop(
                    so_cth[ii], so_w[ii], so_flg[ii], np.prod(np.shape(so_w)))

                # if not sd:
                #     LOG.debug("iy, ix, so_y, so_x, so_lat, so_lon: %d %d %d %d %f %f",
                #               iy, ix, y, x, so_lat[iy, ix], so_lon[iy, ix])
                #     raise cthError(
                #         'CTH is neither opaque nor use window tech!')

                # sd>0 means passed FPASS and QPASS
                if sd > 0:
                    # -999: no stn number, -60: satellite data */
                    # cortype = 1, correct ?
                    result = '%8d%7.2f%7.2f%5d %d %d %8.2f %8.2f\n' % \
                             (99999, so_lat[iy, ix], so_lon[iy, ix], -999, 1, -60,
                              cth, sd)
                    fpt.write(result)
                    so_tot += 1

    LOG.info('\tCreated %d superobservations', so_tot)

    now = datetime.utcnow()
    fname_with_timestamp = str(resultfile) + now.strftime('_%Y%m%d%H%M%S')
    shutil.copy(tmpfname, fname_with_timestamp)
    os.rename(tmpfname, resultfile)

    return


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--datetime', '-d', help='Date and time of observation - yyyymmddhh',
                        required=True)
    parser.add_argument('--area_id', '-a', help='Area id',
                        required=True)
    parser.add_argument('--size', '-s', help='Size of integration area in pixels',
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

    LOG = logging.getLogger('prt_nwcsaf_cloudamount')

    obstime = datetime.strptime(args.datetime, '%Y%m%d%H')
    values = {"area": args.area_id, }
    bname = obstime.strftime(OPTIONS['ctth_composite_filename']) % values
    path = OPTIONS['composite_output_dir']
    filename = os.path.join(path, bname) + '.nc'
    if not os.path.exists(filename):
        LOG.error("File " + str(filename) + " does not exist!")
        sys.exit(-1)

    # Load the Cloud Height composite from file
    COMP = ncCTTHComposite()
    COMP.load(filename)

    NPIX = int(args.size)

    bname = obstime.strftime(OPTIONS['cloudheight_filename']) % values
    path = OPTIONS['composite_output_dir']
    filename = os.path.join(path, bname + '.dat')
    derive_sobs(COMP, NPIX, filename)
