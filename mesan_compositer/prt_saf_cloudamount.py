#!/usr/bin/python

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


import pdb
#, h5py

import sys
import numpy as np

if len(sys.argv) != 4:
    sys.stderr.write(
        'Usage: %s ipar npix composite-file > obsfile\n' % (sys.argv[0]))
    sys.exit(-1)

# ipar= 71; total cloud cover: cloud amount per type
ntCTcl = np.array([
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
nlCTcl = np.array([
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
nmCTcl = np.array([
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
nhCTcl = np.array([
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

nCTcl = {'71': ntCTcl, '73': nlCTcl, '74': nmCTcl, '75': nhCTcl}


# parameter to analyze
ipar = sys.argv[1]

# size of integration area in pixels
npix = max(int(sys.argv[2]), 1)

# non overlapping superobservations
# min 8x8 pixels = ca 8x8 km = 2*dlen x 2*dlen pixels for a superobservation
dlen = int(np.ceil(float(npix) / 2.0))
dx = int(max(2 * DLENMIN, 2 * dlen))
dy = dx
sys.stderr.write('\tUsing %d x %d pixels in a superobservation\n' % (dx, dy))

# load composite (simple npy format for now)

# CT, flag, w, time, id
foo = np.load(sys.argv[3])
CT = foo['cloudtype'].astype('int')
flag = foo['flag']
w = foo['weight']
time = foo['time']
id = foo['id']
lon = foo['lon']
lat = foo['lat']

# initialize superobs data */
ny, nx = np.shape(CT)

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
        so_CT = CT[np.ix_(so_y, so_x)]
        so_w = w[np.ix_(so_y, so_x)]
        #
        # pass all but: 00 Unprocessed and 20 Unclassified
        so_ok = (so_CT > 0) * (so_CT < 20)
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
            so_nc = nCTcl[ipar][so_CT]
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
            print '%8d%7.2f%7.2f%5d %2.2d %2.2d %8.2f %8.2f' % \
                (99999, so_lat[iy, ix], so_lon[iy, ix], -999, cortyp, -60,
                 so_cloud, SDcc)
            so_tot += 1

#
sys.stderr.write('\tCreated %d superobservations\n' % (so_tot))
sys.exit(0)
