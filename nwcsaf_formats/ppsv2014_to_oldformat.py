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

"""Read new PPS netCDF formattet data on swath, and remap and store in old hdf5
format. Start with Cloud Type, and extend to other products!
"""
import time
import h5py
import numpy as np
import logging

from nwcsaf_formats.pps_conversions import map_cloudtypes

LOG = logging.getLogger(__name__)


def write_cloudtype(ppsobj, filename):
    """Write the cloudtype data to hdf5"""

    # noaa18_20141111_0639_48832.euron1.cloudtype.hdf
    # Make the old style PPS filename:
    # filename = (ppsobj.satname + ppsobj.number +
    #            ppsobj.time_slot.strftime('_%Y%m%d_%H%M_') + '_'
    #            + ppsobj.orbit + '.' + ppsobj.area.area_id + '.cloudtype.hdf')

    h5f = h5py.File(filename, 'w')

    try:
        h5f.attrs['description'] = str(ppsobj.mda['title'])
    except KeyError:
        h5f.attrs['description'] = ppsobj.mda['description']
    h5f.attrs['orbit_number'] = np.int32(ppsobj.orbit)
    h5f.attrs['satellite_id'] = ppsobj.satname + ppsobj.number
    h5f.attrs['sec_1970'] = time.mktime(ppsobj.time_slot.timetuple())
    try:
        h5f.attrs['version'] = str(ppsobj.mda['source'])
    except KeyError:
        h5f.attrs['version'] = ppsobj.mda['version']

    # Create the region data:
    blockSize = 4

    comp_type = np.dtype([('area_extent', 'f8', (4,)),
                          ('xsize', 'i4'),
                          ('ysize', 'i4'),
                          ('xscale', 'f4'),
                          ('yscale', 'f4'),
                          ('lat_0', 'f4'),
                          ('lon_0', 'f4'),
                          ('lat_ts', 'f4'),
                          ('id', np.str, 64),
                          ('name', np.str_, 64),
                          ('pcs_id', np.str_, 128),
                          ('pcs_def', np.str_, 128)])

    region = h5f.create_dataset("region", (1,), comp_type)

    aobj = ppsobj.area
    pcs_def = aobj.proj4_string.replace(' +', ',').strip('+')
    data = np.array([(aobj.area_extent,
                      aobj.x_size, aobj.y_size,
                      aobj.pixel_size_x, aobj.pixel_size_y,
                      aobj.proj_dict['lat_0'], aobj.proj_dict['lon_0'],
                      aobj.proj_dict['lat_ts'],
                      aobj.area_id, aobj.name,
                      aobj.proj_id, pcs_def)], dtype=comp_type)
    region[...] = data

    # Make the palette:
    shape = (256, 3)
    palette = h5f.create_dataset("PALETTE", shape, dtype='u1')
    # data = np.ones(shape) * 5
    try:
        data = ppsobj.mda['ct_pal'].data
    except KeyError:
        data = ppsobj.mda['PALETTE']

    palette[...] = data
    palette.attrs['CLASS'] = "PALETTE"
    palette.attrs['PAL_COLORMODEL'] = "RGB"
    palette.attrs['PAL_TYPE'] = "STANDARD8"
    palette.attrs['PAL_VERSION'] = "1.2"

    # Make the cloudtype dataset:
    # shape = (2, 3)
    try:
        shape = ppsobj.ct.data.shape
    except AttributeError:
        shape = ppsobj.cloudtype.data.shape

    cloudtype = h5f.create_dataset("cloudtype", shape, dtype='u1',
                                   compression="gzip", compression_opts=6)
    try:
        cloudtype[...] = map_cloudtypes(ppsobj.ct.data.filled(0))
    except AttributeError:
        cloudtype[...] = ppsobj.cloudtype.data.filled(0)

    # cloudtype[...] = np.ones(shape, 'u1') * 8
    cloudtype.attrs['CLASS'] = "IMAGE"
    cloudtype.attrs['IMAGE_VERSION'] = "1.2"
    #cloudtype.attrs['PALETTE'] = h5f['PALETTE'].ref
    cloudtype.attrs['description'] = "Cloud type classification"

    # Outputvaluenamelist:
    comp_type = np.dtype([('outval_name', np.str, 128), ])
    vnamelist = []
    for i, item in zip(ppsobj.ct.info['flag_values'],
                       str(ppsobj.ct.info['flag_meanings']).split(' ')):
        vnamelist.append(str(i) + ":" + " " + item)
    vnamelist.insert(0, '0: Not processed')

    data = np.array(vnamelist, dtype=comp_type)
    cloudtype.attrs["output_value_namelist"] = data

    # Map the flags from new to old:

    # quality_flag:
    qualityflags = h5f.create_dataset("quality_flag", shape, dtype='u2',
                                      compression="gzip", compression_opts=6)
    try:
        qualityflags[...] = ppsobj.ct_quality.data.filled(0)
    except AttributeError:
        qualityflags[...] = ppsobj.quality_flag.data.filled(0)

    qualityflags.attrs[
        'description'] = "Bitwise quality or AVHRR Processing flag"

    vnamelist = []
    for i, item in zip(ppsobj.ct_quality.info['flag_values'],
                       str(ppsobj.ct_quality.info['flag_meanings']).split(' ')):
        vnamelist.append(str(i) + ":" + " " + item)
    #vnamelist.insert(0, '0: Not processed')

    data = np.array(vnamelist, dtype=comp_type)
    qualityflags.attrs["output_value_namelist"] = data

    h5f.close()

    return

if __name__ == '__main__':
    from mpop.satellites import PolarFactory
    import datetime

    from mpop.utils import debug_on
    debug_on()

    time_slot = datetime.datetime(2014, 11, 11, 6, 39, 59)
    # time_slot = datetime.datetime(2014, 11, 11, 6, 39)
    orbit = '48832'

    gbd = PolarFactory.create_scene("noaa", "18", "avhrr", time_slot, orbit)
    # gbd.load(['CMA', 'CT', 'CTTH', 'PC'])
    gbd.load(['CT'])

    lcd = gbd.project('euron1')
    filename = (lcd.satname + lcd.number +
                lcd.time_slot.strftime('_%Y%m%d_%H%M_') + '_'
                + lcd.orbit + '.' + lcd.area.area_id + '.cloudtype.hdf')
    #write_cloudtype(lcd["CT"], filename)
