#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Adam.Dybbroe

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

"""From the cloud type composite retrieve super observations of cloud
amount/cover and print to stdout
"""

import numpy as np
from mesan_compositer.netcdf_io import ncCloudTypeComposite
import argparse
from datetime import datetime
import os
import sys
import ConfigParser

import logging
LOG = logging.getLogger(__name__)

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

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


# ipar= 71; total cloud cover: cloud amount per type
ntctypecl = np.array([
    0.0,  # 00 Not processed
    0.0,  # 01 Cloud free land
    0.0,  # 02 Cloud free sea
    0.0,  # 03 Snow/ice contaminated land
    0.0,  # 04 Snow/ice contaminated sea
    1.0,  # 05 Very low cumiliform cloud
    1.0,  # 06 Very low stratiform cloud
    1.0,  # 07 Low cumiliform cloud
    1.0,  # 08 Low stratiform cloud
    1.0,  # 09 Medium level cumiliform cloud
    1.0,  # 10 Medium level stratiform cloud
    1.0,  # 11 High and opaque cumiliform cloud
    1.0,  # 12 High and opaque stratiform cloud
    1.0,  # 13 Very high and opaque cumiliform cloud
    1.0,  # 14 Very high and opaque stratiform cloud
    1.0,  # 15 Very thin cirrus cloud
    1.0,  # 16 Thin cirrus cloud
    1.0,  # 17 Thick cirrus cloud
    1.0,  # 18 Cirrus above low or medium level cloud
    1.0,  # 19 Fractional or sub-pixel cloud
    0.0  # 20 Undefined
])

# ipar= 73; low level cloud cover: cloud amount per type
nlctypecl = np.array([
    0.0,  # 00 Not processed
    0.0,  # 01 Cloud free land
    0.0,  # 02 Cloud free sea
    0.0,  # 03 Snow/ice contaminated land
    0.0,  # 04 Snow/ice contaminated sea
    1.0,  # 05 Very low cumiliform cloud
    1.0,  # 06 Very low stratiform cloud
    1.0,  # 07 Low cumiliform cloud
    1.0,  # 08 Low stratiform cloud
    0.5,  # 09 Medium level cumiliform cloud
    0.5,  # 10 Medium level stratiform cloud
    0.5,  # 11 High and opaque cumiliform cloud
    0.5,  # 12 High and opaque stratiform cloud
    0.5,  # 13 Very high and opaque cumiliform cloud
    0.5,  # 14 Very high and opaque stratiform cloud
    0.0,  # 15 Very thin cirrus cloud
    0.0,  # 16 Thin cirrus cloud
    0.0,  # 17 Thick cirrus cloud
    0.5,  # 18 Cirrus above low or medium level cloud
    .75,  # 19 Fractional or sub-pixel cloud
    0.0  # 20 Undefined
])

# ipar= 74; medium level cloud cover: cloud amount per type
nmctypecl = np.array([
    0.0,  # 00 Not processed
    0.0,  # 01 Cloud free land
    0.0,  # 02 Cloud free sea
    0.0,  # 03 Snow/ice contaminated land
    0.0,  # 04 Snow/ice contaminated sea
    0.0,  # 05 Very low cumiliform cloud
    0.0,  # 06 Very low stratiform cloud
    0.0,  # 07 Low cumiliform cloud
    0.0,  # 08 Low stratiform cloud
    1.0,  # 09 Medium level cumiliform cloud
    1.0,  # 10 Medium level stratiform cloud
    .75,  # 11 High and opaque cumiliform cloud
    .75,  # 12 High and opaque stratiform cloud
    .75,  # 13 Very high and opaque cumiliform cloud
    .75,  # 14 Very high and opaque stratiform cloud
    0.0,  # 15 Very thin cirrus cloud
    0.0,  # 16 Thin cirrus cloud
    .25,  # 17 Thick cirrus cloud
    0.5,  # 18 Cirrus above low or medium level cloud
    .25,  # 19 Fractional or sub-pixel cloud
    0.0  # 20 Undefined
])

# ipar= 75; high level cloud cover: cloud amount per type
nhctypecl = np.array([
    0.0,  # 00 Not processed
    0.0,  # 01 Cloud free land
    0.0,  # 02 Cloud free sea
    0.0,  # 03 Snow/ice contaminated land
    0.0,  # 04 Snow/ice contaminated sea
    0.0,  # 05 Very low cumiliform cloud
    0.0,  # 06 Very low stratiform cloud
    0.0,  # 07 Low cumiliform cloud
    0.0,  # 08 Low stratiform cloud
    0.0,  # 09 Medium level cumiliform cloud
    0.0,  # 10 Medium level stratiform cloud
    1.0,  # 11 High and opaque cumiliform cloud
    1.0,  # 12 High and opaque stratiform cloud
    1.0,  # 13 Very high and opaque cumiliform cloud
    1.0,  # 14 Very high and opaque stratiform cloud
    1.0,  # 15 Very thin cirrus cloud
    1.0,  # 16 Thin cirrus cloud
    1.0,  # 17 Thick cirrus cloud
    1.0,  # 18 Cirrus above low or medium level cloud
    .25,  # 19 Fractional or sub-pixel cloud
    0.0  # 20 Undefined
])

nctypecl = {'71': ntctypecl, '73': nlctypecl, '74': nmctypecl, '75': nhctypecl}


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


def derive_sobs(ct_comp, ipar, npix, resultfile):
    """Derive the super observations and print data to file"""

    # Get the lon,lat:
    # from pyresample import utils
    # area = utils.load_area(
    #     '/local_disk/laptop/Satsa/Mesan/mesan-sat-preproc/etc/areas.def', 'mesanX')
    # lon, lat = area.get_lonlats()
    lon, lat = ct_comp.area_def.get_lonlats()

    ctype = ct_comp.cloudtype.data.astype('int')
    weight = ct_comp.weight.data
    #obstime = ct_comp.time.data
    #id = ct_comp.id.data

    # non overlapping superobservations
    # min 8x8 pixels = ca 8x8 km = 2*dlen x 2*dlen pixels for a
    # superobservation
    dlen = int(np.ceil(float(npix) / 2.0))
    dx = int(max(2 * DLENMIN, 2 * dlen))
    dy = dx
    fpt = open(resultfile, 'w')
    print('\tUsing %d x %d pixels in a superobservation' % (dx, dy))

    # initialize superobs data */
    ny, nx = np.shape(ctype)

    # indices to super obs "midpoints"
    lx = np.arange(dlen, nx - dlen + 1, dx)
    ly = np.arange(ny - dlen, dlen, -dy)

    so_lon = lon[np.ix_(ly, lx)]
    so_lat = lat[np.ix_(ly, lx)]

    so_tot = 0
    for iy in range(len(ly)):
        for ix in range(len(lx)):
            # super ob domain is: ix-dlen:ix+dlen-1, iy-dlen:iy+dlen-1
            x = lx[ix]
            y = ly[iy]
            so_x = np.arange(x - dlen, x + dlen - 1 + 1)
            so_y = np.arange(y - dlen, y + dlen - 1 + 1)
            so_ctype = ctype[np.ix_(so_y, so_x)]
            so_w = weight[np.ix_(so_y, so_x)]
            #
            # pass all but: 00 Unprocessed and 20 Unclassified
            so_ok = (so_ctype > 0) * (so_ctype < 20)
            so_wtot = np.sum(so_w[so_ok])
            so_nfound = np.sum(so_ok)
            #
            # observation quality
            so_q = so_wtot / (so_nfound + 1e-6)
            #
            # check super obs statistics
            # pdb.set_trace()
            #
            if float(so_nfound) / npix ** 2 > FPASS and so_q >= QPASS:
                # enough number of OK pixels and quality
                #      pdb.set_trace()
                so_nc = nctypecl[ipar][so_ctype]
                so_cloud = np.sum(so_nc * so_w / so_wtot)
                #
                # print data
                if ipar == '71' and so_q >= 0.95:
                    # 10 => checked uncorrelated observations
                    # 11 => checked correlated observations
                    # use 10 to override data from automatic stations
                    cortyp = 10
                else:
                    cortyp = 1  # is this correct ???
                #
                # -999: no stn number, -60: satellite data */
                result = '%8d%7.2f%7.2f%5d %2.2d %2.2d %8.2f %8.2f\n' % \
                    (99999, so_lat[iy, ix], so_lon[iy, ix], -999, cortyp, -60,
                     so_cloud, SDcc)
                fpt.write(result)
                so_tot += 1

    print('\tCreated %d superobservations' % (so_tot))
    fpt.close()

    return

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--datetime', '-d', help='Date and time of observation - yyyymmddhh',
                        required=True)
    parser.add_argument('--area_id', '-a', help='Area id',
                        required=True)
    parser.add_argument('--ipar', '-i', help='Parameter id',
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
    bname = obstime.strftime(OPTIONS['ct_composite_filename']) % values
    path = OPTIONS['composite_output_dir']
    filename = os.path.join(path, bname) + '.nc'
    if not os.path.exists(filename):
        LOG.error("File " + str(filename) + " does not exist!")
        sys.exit(-1)

    # Load the Cloud Type composite from file
    comp = ncCloudTypeComposite()
    comp.load(filename + '.nc')

    ipar = str(args.ipar)
    npix = int(args.size)

    bname = obstime.strftime(OPTIONS['cloudamount_filename']) % values
    path = OPTIONS['composite_output_dir']
    filename = os.path.join(path, bname + '.dat')
    derive_sobs(comp, ipar, npix, filename)
