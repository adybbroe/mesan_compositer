#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015 Adam.Dybbroe

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
from datetime import datetime

from nwcsaf_formats.pps_conversions import (map_cloudtypes,
                                            old_ctype_palette,
                                            old_ctype_palette_data,
                                            old_ctth_press_palette_data,
                                            old_ctth_temp_palette_data,
                                            old_ctth_height_palette_data,
                                            ctype_convert_flags,
                                            ctth_convert_flags,
                                            old_processing_flag_palette
                                            )

LOG = logging.getLogger(__name__)


def write_product(ppsobj, filename):
    """Write the product data to hdf5, pps v2012 format.
    """

    h5f = h5py.File(filename, 'w')
    try:
        h5f.attrs['description'] = np.string_(ppsobj.mda['title'])
    except KeyError:
        h5f.attrs['description'] = np.string_(ppsobj.mda['description'])
    try:
        h5f.attrs['orbit_number'] = np.int32(ppsobj.mda['orbit_number'])
    except KeyError:
        h5f.attrs['orbit_number'] = np.int32(ppsobj.mda['orbit'])

    try:
        h5f.attrs['satellite_id'] = np.string_(ppsobj.mda['platform'])
    except KeyError:
        h5f.attrs['satellite_id'] = np.string_(ppsobj.mda['satellite'])

    try:
        dtobj = datetime.strptime(ppsobj.mda['time_coverage_start'][:-2],
                                  '%Y%m%dT%H%M%S')
    except KeyError:
        dtobj = ppsobj.mda['time_slot']
    h5f.attrs['sec_1970'] = time.mktime(dtobj.timetuple())

    try:
        h5f.attrs['version'] = np.string_(ppsobj.mda['source'])
    except KeyError:
        h5f.attrs['version'] = np.string_(ppsobj.mda['version'])

    # Create the region data:
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

    # Save the datatype!
    h5f['Region'] = comp_type
    region = h5f.create_dataset("region", (1,), comp_type)

    # Outputvaluenamelist:
    ov_comp_type = get_output_valuenamelist_compund_dtype()
    # The datatype for the palette:
    h5f['OutputValueNameList'] = ov_comp_type

    aobj = ppsobj.area
    pcs_def = aobj.proj4_string.replace(' +', ',').strip('+')
    data = np.array([(aobj.area_extent,
                      aobj.x_size, aobj.y_size,
                      aobj.pixel_size_x, aobj.pixel_size_y,
                      aobj.proj_dict.get('lat_0', 0.0),
                      aobj.proj_dict.get('lon_0', 0.0),
                      aobj.proj_dict.get('lat_ts', 0.0),
                      aobj.area_id, aobj.name,
                      aobj.proj_id, pcs_def)], dtype=comp_type)
    region[...] = data

    product = ppsobj.name

    make_palettes[product](h5f, ppsobj)
    make_dataset[product](h5f, ppsobj)
    make_flags[product](h5f, ppsobj)
    h5f.close()

    return


def make_palettes_ct(h5f, ppsobj):
    # Make the palette:
    shape = (256, 3)
    palette = h5f.create_dataset("PALETTE", shape, dtype='u1')
    try:
        dummy = ppsobj.mda['ct_pal'].data
        palette_data = old_ctype_palette_data()
    except KeyError:
        palette_data = ppsobj.mda['PALETTE']

    palette[...] = palette_data
    palette.attrs['CLASS'] = np.string_("PALETTE")
    palette.attrs['PAL_COLORMODEL'] = np.string_("RGB")
    palette.attrs['PAL_TYPE'] = np.string_("STANDARD8")
    palette.attrs['PAL_VERSION'] = np.string_("1.2")


def make_palettes_pc(h5f, ppsobj):
    pass


def make_palettes_ctth(h5f, ppsobj):
    # Make the palette:
    shape = (256, 3)
    palette = h5f.create_dataset("HEIGHT_PALETTE", shape, dtype='u1')

    palette_data = old_ctth_height_palette_data()

    palette[...] = palette_data
    palette.attrs['CLASS'] = np.string_("PALETTE")
    palette.attrs['PAL_COLORMODEL'] = np.string_("RGB")
    palette.attrs['PAL_TYPE'] = np.string_("STANDARD8")
    palette.attrs['PAL_VERSION'] = np.string_("1.2")

    palette = h5f.create_dataset("PRESSURE_PALETTE", shape, dtype='u1')

    palette_data = old_ctth_press_palette_data()

    palette[...] = palette_data
    palette.attrs['CLASS'] = np.string_("PALETTE")
    palette.attrs['PAL_COLORMODEL'] = np.string_("RGB")
    palette.attrs['PAL_TYPE'] = np.string_("STANDARD8")
    palette.attrs['PAL_VERSION'] = np.string_("1.2")

    palette = h5f.create_dataset("TEMPERATURE_PALETTE", shape, dtype='u1')

    palette_data = old_ctth_temp_palette_data()

    palette[...] = palette_data
    palette.attrs['CLASS'] = np.string_("PALETTE")
    palette.attrs['PAL_COLORMODEL'] = np.string_("RGB")
    palette.attrs['PAL_TYPE'] = np.string_("STANDARD8")
    palette.attrs['PAL_VERSION'] = np.string_("1.2")


def get_output_valuenamelist_compund_dtype():
    """Define the compound datatype for the Outputvaluenamelist and return the
    numpy dtype"""

    # Outputvaluenamelist:
    return np.dtype([('outval_name', np.string_, 128), ])


def make_dataset_ct(h5f, ppsobj):

    # Make the cloudtype dataset:
    # shape = (2, 3)
    try:
        shape = ppsobj.ct.data.shape
    except AttributeError:
        shape = ppsobj.cloudtype.data.shape

    cloudtype = h5f.create_dataset("cloudtype", shape, dtype='u1',
                                   compression="gzip", compression_opts=6)

    # Outputvaluenamelist:
    ov_comp_type = get_output_valuenamelist_compund_dtype()

    try:
        cloudtype[...] = map_cloudtypes(ppsobj.ct.data.filled(0))
        print("Cloudtype categories mapped!")
        palette = old_ctype_palette()
    except AttributeError:
        print("Cloudtype categories *not* mapped!")
        cloudtype[...] = ppsobj.cloudtype.data.filled(0)
        vnamelist = []
        for i, item in zip(ppsobj.ct.info['flag_values'],
                           str(ppsobj.ct.info['flag_meanings']).split(' ')):
            vnamelist.append(str(i) + ":" + " " + item)
        vnamelist.insert(0, '0: Not processed')
        palette = np.array(vnamelist, dtype=ov_comp_type)

    cloudtype.attrs["output_value_namelist"] = palette
    cloudtype.attrs['CLASS'] = np.string_("IMAGE")
    cloudtype.attrs['IMAGE_VERSION'] = np.string_("1.2")
    #cloudtype.attrs['PALETTE'] = h5f['PALETTE'].ref
    cloudtype.attrs['description'] = np.string_("Cloud type classification")


def make_dataset_pc(h5f, ppsobj):
    shape = ppsobj.pc_precip_moderate.data.shape

    precipitation1 = h5f.create_dataset("precipitation1", shape, dtype='u1',
                                        compression="gzip", compression_opts=6)
    precipitation1_data = ppsobj.pc_precip_light.data.filled()
    precipitation1[...] = precipitation1_data.astype(np.uint8)
    precipitation1.attrs['description'] = np.string_(
        "Likelihood for precipitation intensity in class 1")
    precipitation1.attrs['intensity_class_lowerlimit'] = np.float32(0.1)
    precipitation1.attrs['intensity_class_upperlimit'] = np.float32(0.5)

    precipitation2 = h5f.create_dataset("precipitation2", shape, dtype='u1',
                                        compression="gzip", compression_opts=6)
    precipitation2_data = ppsobj.pc_precip_moderate.data.filled()
    precipitation2[...] = precipitation2_data.astype(np.uint8)
    precipitation2.attrs['description'] = np.string_(
        "Likelihood for precipitation intensity in class 2")
    precipitation2.attrs['intensity_class_lowerlimit'] = np.float32(0.5)
    precipitation2.attrs['intensity_class_upperlimit'] = np.float32(5.0)

    precipitation3 = h5f.create_dataset("precipitation3", shape, dtype='u1',
                                        compression="gzip", compression_opts=6)
    precipitation3_data = ppsobj.pc_precip_intense.data.filled()
    precipitation3[...] = precipitation3_data.astype(np.uint8)
    precipitation3.attrs['description'] = np.string_(
        "Likelihood for precipitation intensity in class 3")
    precipitation3.attrs['intensity_class_lowerlimit'] = np.float32(5.0)
    precipitation3.attrs['intensity_class_upperlimit'] = np.float32(1000.0)


def make_dataset_ctth(h5f, ppsobj):
    shape = ppsobj.ctth_alti.data.shape

    alti = h5f.create_dataset("height", shape, dtype='u1',
                              compression="gzip", compression_opts=6)
    alti_data = (ppsobj.ctth_alti.data.filled() / 200.0)
    alti_data[alti_data > 255] = 255
    alti[...] = alti_data.astype(np.uint8)
    alti.attrs['CLASS'] = np.string_("IMAGE")
    alti.attrs['IMAGE_VERSION'] = np.string_("1.2")
    #alti.attrs['PALETTE'] = h5f['HEIGHT_PALETTE'].ref
    alti.attrs['description'] = np.string_("scaled Height (m)")
    alti.attrs['gain'] = np.float32(200.0)
    alti.attrs['intercept'] = np.float32(0.0)
    alti.attrs['no_data_value'] = np.uint8(255)

    tempe = h5f.create_dataset("temperature", shape, dtype='u1',
                               compression="gzip", compression_opts=6)
    tempe_data = (ppsobj.ctth_tempe.data.filled() - 100.0)
    tempe_data[tempe_data > 255] = 255
    tempe[...] = tempe_data.astype(np.uint8)
    tempe.attrs['CLASS'] = np.string_("IMAGE")
    tempe.attrs['IMAGE_VERSION'] = np.string_("1.2")
    #tempe.attrs['PALETTE'] = h5f['TEMPERATURE_PALETTE'].ref
    tempe.attrs['description'] = np.string_("scaled Temperature (K)")
    tempe.attrs['gain'] = np.float32(1.0)
    tempe.attrs['intercept'] = np.float32(100.0)
    tempe.attrs['no_data_value'] = np.uint8(255)

    pres = h5f.create_dataset("pressure", shape, dtype='u1',
                              compression="gzip", compression_opts=6)
    pres_data = (ppsobj.ctth_pres.data.filled() / 2500.0)  # scale 25, Pa->hPa
    pres_data[pres_data > 255] = 255
    pres[...] = pres_data.astype(np.uint8)
    pres.attrs['CLASS'] = np.string_("IMAGE")
    pres.attrs['IMAGE_VERSION'] = np.string_("1.2")
    #pres.attrs['PALETTE'] = h5f['PRESSURE_PALETTE'].ref
    pres.attrs['description'] = np.string_("scaled Pressure (hPa)")
    pres.attrs['gain'] = np.float32(25.0)
    pres.attrs['intercept'] = np.float32(0.0)
    pres.attrs['no_data_value'] = np.uint8(255)


def make_flags_ct(h5f, ppsobj):
    """Map the cloudtype quality flags from new to old"""

    shape = ppsobj.ct.data.shape

    # quality_flag:
    qualityflags = h5f.create_dataset("quality_flag", shape, dtype='u2',
                                      compression="gzip", compression_opts=6)

    sflags = ppsobj.ct_status_flag.data.filled(0)
    cflags = ppsobj.ct_conditions.data.filled(0)
    qflags = ppsobj.ct_quality.data.filled(0)
    oldflags = ctype_convert_flags(sflags, cflags, qflags)

    qualityflags[...] = oldflags

    qualityflags.attrs[
        'description'] = np.string_("Bitwise quality or AVHRR Processing flag")
    qualityflags.attrs[
        "output_value_namelist"] = old_processing_flag_palette('cloudtype')


def make_flags_ctth(h5f, ppsobj):
    """Map the ctth flags from new to old"""

    shape = ppsobj.ctth_tempe.data.shape

    # processing_flag:
    qualityflags = h5f.create_dataset("processing_flag", shape, dtype='u2',
                                      compression="gzip", compression_opts=6)
    sflags = ppsobj.ctth_status_flag.data.filled(0)
    cflags = ppsobj.ctth_conditions.data.filled(0)
    qflags = ppsobj.ctth_quality.data.filled(0)
    oldflags = ctth_convert_flags(sflags, cflags, qflags)

    qualityflags[...] = oldflags

    qualityflags.attrs[
        'description'] = np.string_("16 bit Processing flag")
    qualityflags.attrs[
        "output_value_namelist"] = old_processing_flag_palette('ctth')

    return


def make_flags_pc(h5f, ppsobj):
    pass

make_palettes = {
    "CT": make_palettes_ct,
    "CTTH": make_palettes_ctth,
    "PC": make_palettes_pc,
}

make_dataset = {
    "CT": make_dataset_ct,
    "CTTH": make_dataset_ctth,
    "PC": make_dataset_pc,
}

make_flags = {
    "CT": make_flags_ct,
    "CTTH": make_flags_ctth,
    "PC": make_flags_pc,
}

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
    # gbd.load(['CT'])

    # lcd = gbd.project('euron1')
    # filename = (lcd.satname + lcd.number +
    #             lcd.time_slot.strftime('_%Y%m%d_%H%M_') +
    #             lcd.orbit + '.' + lcd.area.area_id + '.cloudtype.hdf')
    # write_product(lcd["CT"], filename)

    gbd.load(['CTTH'])
    lcd = gbd.project('euron1')
    filename = (lcd.satname + lcd.number +
                lcd.time_slot.strftime('_%Y%m%d_%H%M_') +
                lcd.orbit + '.' + lcd.area.area_id + '.ctth.hdf')
    write_product(lcd["CTTH"], filename)

    # gbd.load(['PC'])
    # lcd = gbd.project('euron1')
    # filename = (lcd.satname + lcd.number +
    #             lcd.time_slot.strftime('_%Y%m%d_%H%M_') +
    #             lcd.orbit + '.' + lcd.area.area_id + '.precipclouds.hdf')
    # write_product(lcd["PC"], filename)
