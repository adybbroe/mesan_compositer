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

"""Functions for converting from new to old PPS product outputs
"""

from mesan_compositer.pps_msg_conversions import (get_bit_from_flags,
                                                  bits2value,
                                                  value2bits)
import numpy as np


OLD_CTYPE_NAMES = ["Not processed",
                   "Cloud free land",
                   "Cloud free sea",
                   "Snow/ice contaminated land",
                   "Snow/ice contaminated sea",
                   "Very low cumiliform cloud",
                   "Very low stratiform cloud",
                   "Low cumiliform cloud",
                   "Low stratiform cloud",
                   "Medium level cumiliform cloud",
                   "Medium level stratiform cloud",
                   "High and opaque cumiliform cloud",
                   "High and opaque stratiform cloud",
                   "Very high and opaque cumiliform cloud",
                   "Very high and opaque stratiform cloud",
                   "Very thin cirrus cloud",
                   "Thin cirrus cloud",
                   "Thick cirrus cloud",
                   "Cirrus above low or medium level cloud",
                   "Fractional or sub-pixel cloud",
                   "Undefined"]

OLD_CTTH_PFLAG_NAMES = ["1: Not processed",
                        "2: Cloudy",
                        "4: Opaque cloud",
                        "8: RTTOV IR simulations available",
                        "16: Missing NWP data",
                        "32: thermal inversion avaliable",
                        "64: Missing AVHRR data",
                        "128: RTTOV IR simulation applied",
                        "256: Windowing technique applied",
                        "512: bit not defined",
                        "1024: bit not defined",
                        "2048: bit not defined",
                        "4096: bit not defined",
                        "8192: bit not defined",
                        "16384: Quality estimation avaliable",
                        "32768: Low confidence"]

OLD_CTYPE_QFLAG_NAMES = ["Land",
                         "Coast",
                         "Night",
                         "Twilight",
                         "Sunglint",
                         "High terrain",
                         "Low level inversion",
                         "Nwp data present",
                         "Avhrr channel missing",
                         "Low quality",
                         "Reclassified after spatial smoothing",
                         "Stratiform-Cumuliform Distinction performed",
                         "Spare bit: Content TBD",
                         "External sea-ice information applied",
                         "Sea-ice derived from NWP (otherwise derived from OSISAF product)",
                         "Sea-ice concentration above threshold"
                         ]


def ctth_convert_flags(status_flag, conditions_flag, quality_flag):
    """Convert from new v2014 ctth flags to the old ones"""

    shape = status_flag.shape
    ones = np.ones(shape, "int32")

    # First bit in quality_flag: Containing no data. Cloudfree pixel or pixel
    # where no cloud height could be retrieved.
    is_bit0_set = get_bit_from_flags(quality_flag, 0)
    arr = np.where(is_bit0_set, np.left_shift(ones, 0), 0)
    retv = np.array(arr)

    is_bit3_set = get_bit_from_flags(quality_flag, 3)
    is_bit4_set = get_bit_from_flags(quality_flag, 4)
    is_bit5_set = get_bit_from_flags(quality_flag, 5)
    # Check if any of these three bits are set:
    arr = np.where(np.logical_or(np.logical_or(is_bit3_set,
                                               is_bit4_set),
                                 is_bit5_set),
                   np.left_shift(ones, 14), 0)
    retv = np.add(retv, arr)
    # Low confidence: (Questionable)
    arr = np.where(np.logical_and(np.logical_and(is_bit3_set == False,
                                                 is_bit4_set),
                                  is_bit5_set == False),
                   np.left_shift(ones, 15), 0)
    # Low confidence: (Bad)
    arr = np.where(np.logical_and(np.logical_and(is_bit3_set,
                                                 is_bit4_set),
                                  is_bit5_set == False),
                   np.left_shift(ones, 15), arr)
    # Low confidence: (Interpolated)
    arr = np.where(np.logical_and(np.logical_and(is_bit3_set == False,
                                                 is_bit4_set == False),
                                  is_bit5_set), np.left_shift(ones, 15), arr)
    retv = np.add(retv, arr)

    #
    # Status flags:
    # Cloudfree?
    is_bit_nonproc_set = get_bit_from_flags(quality_flag, 0)
    is_bit0_set = get_bit_from_flags(status_flag, 0)
    arr = np.where(is_bit0_set, np.left_shift(ones, 0), 0)
    retv = np.add(retv, arr)
    # Not Cloudfree and not non-proc = Cloudy
    arr = np.where(np.logical_and(is_bit_nonproc_set == False,
                                  is_bit0_set == False),
                   np.left_shift(ones, 1), arr)
    retv = np.add(retv, arr)
    # Opaque clouds?
    is_bit2_set = get_bit_from_flags(status_flag, 2)
    arr = np.where(is_bit2_set, np.left_shift(ones, 2), 0)
    retv = np.add(retv, arr)

    # Inverion:
    is_bit4_set = get_bit_from_flags(status_flag, 4)
    arr = np.where(is_bit4_set, np.left_shift(ones, 5), 0)
    retv = np.add(retv, arr)

    # NWP low quality => NWP missing:
    is_bit5_set = get_bit_from_flags(status_flag, 5)
    arr = np.where(is_bit5_set, np.left_shift(ones, 4), 0)
    retv = np.add(retv, arr)

    # RTTOV
    is_bit6_set = get_bit_from_flags(status_flag, 6)
    arr = np.where(is_bit6_set, np.left_shift(ones, 7), 0)
    retv = np.add(retv, arr)

    # Histogram technique applied
    is_bit7_set = get_bit_from_flags(status_flag, 7)
    arr = np.where(is_bit7_set, np.left_shift(ones, 8), 0)
    retv = np.add(retv, arr)

    #
    # Conditions flags:
    # Pixel is out of swath or points to space:
    is_bit0_set = get_bit_from_flags(conditions_flag, 0)
    arr = np.where(is_bit0_set, np.left_shift(ones, 0), 0)
    retv = np.add(retv, arr)

    # Satellite input data:
    is_bit8_set = get_bit_from_flags(conditions_flag, 8)
    is_bit9_set = get_bit_from_flags(conditions_flag, 9)
    # At least one mandatory channel is missing:
    arr = np.where(np.logical_and(is_bit8_set, is_bit9_set),
                   np.left_shift(ones, 6), 0)
    # At least one useful channel is missing:
    arr = np.where(np.logical_and(is_bit8_set == False, is_bit9_set),
                   np.left_shift(ones, 6), arr)
    retv = np.add(retv, arr)

    # NWP input data:
    is_bit10_set = get_bit_from_flags(conditions_flag, 10)
    is_bit11_set = get_bit_from_flags(conditions_flag, 11)
    # Mandatory data missing:
    arr = np.where(np.logical_and(is_bit10_set, is_bit11_set),
                   np.left_shift(ones, 4), 0)
    # Useful data missing:
    arr = np.where(np.logical_and(is_bit10_set == False, is_bit11_set),
                   np.left_shift(ones, 4), arr)
    retv = np.add(retv, arr)

    return retv


def ctype_convert_flags(status_flag, conditions_flag, quality_flag):
    """Convert from new v2014 cloudtype flags to old ones"""
    # New status flag (ct_status_flag):
    # Bit 0: Low level thermal inversion in NWP field
    # Bit 1: NWP data suspected low quality
    # Bit 2: Sea ice map is available
    # Bit 3: Sea ice, according to external map

    shape = status_flag.shape
    ones = np.ones(shape, "int32")

    is_bit0_set = get_bit_from_flags(status_flag, 0)
    is_bit2_set = get_bit_from_flags(status_flag, 2)
    is_bit3_set = get_bit_from_flags(status_flag, 3)
    arr = np.where(is_bit0_set, np.left_shift(ones, 6), 0)
    retv = np.array(arr)
    arr = np.where(is_bit2_set, np.left_shift(ones, 13), 0)
    retv = np.add(retv, arr)
    arr = np.where(is_bit3_set, np.left_shift(ones, 15), 0)
    retv = np.add(retv, arr)

    # ct_conditions_flag:
    is_bit1_set = get_bit_from_flags(conditions_flag, 1)
    is_bit2_set = get_bit_from_flags(conditions_flag, 2)
    # Twilight
    arr = np.where(np.logical_and(is_bit1_set, is_bit2_set),
                   np.left_shift(ones, 3), 0)
    retv = np.add(retv, arr)
    # Night
    arr = np.where(np.logical_and(is_bit1_set, is_bit2_set == 0),
                   np.left_shift(ones, 2), 0)
    retv = np.add(retv, arr)
    # Day = no illumination flags are set in the old format!

    # Sunglint:
    is_bit3_set = get_bit_from_flags(conditions_flag, 3)
    arr = np.where(is_bit3_set,
                   np.left_shift(ones, 4), 0)
    retv = np.add(retv, arr)

    # Land, sea and coast:
    is_bit4_set = get_bit_from_flags(conditions_flag, 4)
    is_bit5_set = get_bit_from_flags(conditions_flag, 5)
    # Land
    arr = np.where(is_bit4_set,
                   np.left_shift(ones, 0), 0)
    retv = np.add(retv, arr)
    # Land and coast
    arr = np.where(is_bit5_set,
                   np.left_shift(ones, 1), 0)
    retv = np.add(retv, arr)

    return retv


def map_cloudtypes(newctype):
    """Map the v2014 cloudtype classes to the old (v2012 and before) cloud type
    categories"""

    print("Map new cloud type to old one...")
    retv = newctype.copy()

    retv[newctype == 5] = 6
    retv[newctype == 6] = 8
    retv[newctype == 7] = 10
    retv[newctype == 8] = 12
    retv[newctype == 9] = 14
    retv[newctype == 10] = 19
    retv[newctype == 11] = 15
    retv[newctype == 12] = 16
    retv[newctype == 13] = 17
    retv[newctype == 14] = 18
    retv[newctype == 255] = 20
    # Class 15 (High_semistransparent_above_snow_or_ice) does not exist in the
    # old one!
    retv[newctype == 15] = 13

    return retv


def old_processing_flag_palette(product):
    """Make the old name list with cloudtype or ctth processing flag descriptions"""

    if product == 'ctth':
        names = OLD_CTTH_PFLAG_NAMES
    elif product == 'cloudtype':
        names = OLD_CTYPE_QFLAG_NAMES
    else:
        raise NotImplementedError('Processing flag conversion for ' +
                                  'product name ' + str(product) +
                                  ' not supported!')

    # Outputvaluenamelist:
    comp_type = np.dtype([('outval_name', np.str, 128), ])
    vnamelist = []
    for i, item in enumerate(names):
        bitvalue = 2 ** i
        vnamelist.append(str(bitvalue) + ": " + item)

    return np.array(vnamelist, dtype=comp_type)


def old_ctype_palette():
    """Make the old cloudtype output_value_namelist for hdf5 file"""

    # Outputvaluenamelist:
    comp_type = np.dtype([('outval_name', np.str, 128), ])
    vnamelist = []
    for i, item in enumerate(OLD_CTYPE_NAMES):
        vnamelist.append(str(i) + ": " + item)

    return np.array(vnamelist, dtype=comp_type)


def old_ctype_palette_data():
    """Make the old cloudtype PALETTE data array for hdf5 file"""

    # PALETTE:
    pal = []
    for i in range(256):
        pal.append([0, 0, 0])

    pal[0] = [100, 100, 100]
    pal[1] = [0, 120, 0]
    pal[2] = [0, 0, 0]
    pal[3] = [250, 190, 250]
    pal[4] = [220, 160, 220]
    pal[5] = [255, 150, 0]
    pal[6] = [255, 100, 0]
    pal[7] = [255, 220, 0]
    pal[8] = [255, 180, 0]
    pal[9] = [255, 255, 140]
    pal[10] = [240, 240, 0]
    pal[11] = [250, 240, 200]
    pal[12] = [215, 215, 150]
    pal[13] = [255, 255, 255]
    pal[14] = [230, 230, 230]
    pal[15] = [0, 80, 215]
    pal[16] = [0, 180, 230]
    pal[17] = [0, 240, 240]
    pal[18] = [90, 200, 160]
    pal[19] = [200, 0, 200]
    pal[20] = [95, 60, 30]

    return np.array(pal, dtype='u1')


def old_ctth_height_palette_data():
    return np.array([
        [255, 0, 216],
        [255, 0, 216],
        [255, 0, 216],
        [126, 0, 43],
        [126, 0, 43],
        [153, 20, 47],
        [153, 20, 47],
        [153, 20, 47],
        [178, 51, 0],
        [178, 51, 0],
        [255, 76, 0],
        [255, 76, 0],
        [255, 76, 0],
        [255, 102, 0],
        [255, 102, 0],
        [255, 164, 0],
        [255, 164, 0],
        [255, 164, 0],
        [255, 216, 0],
        [255, 216, 0],
        [255, 255, 0],
        [255, 255, 0],
        [255, 255, 0],
        [216, 255, 0],
        [216, 255, 0],
        [178, 255, 0],
        [178, 255, 0],
        [178, 255, 0],
        [153, 255, 0],
        [153, 255, 0],
        [0, 255, 0],
        [0, 255, 0],
        [0, 255, 0],
        [0, 140, 48],
        [0, 140, 48],
        [0, 178, 255],
        [0, 178, 255],
        [0, 178, 255],
        [0, 216, 255],
        [0, 216, 255],
        [0, 255, 255],
        [0, 255, 255],
        [0, 255, 255],
        [238, 214, 210],
        [238, 214, 210],
        [239, 239, 223],
        [239, 239, 223],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0]], dtype="u1")


def old_ctth_press_palette_data():
    return np.array([
        [255, 0, 128],
        [255, 0, 128],
        [255, 0, 128],
        [255, 0, 128],
        [255, 0, 128],
        [255, 0, 128],
        [239, 239, 223],
        [239, 239, 223],
        [238, 214, 210],
        [238, 214, 210],
        [0, 255, 255],
        [0, 255, 255],
        [0, 216, 255],
        [0, 216, 255],
        [0, 178, 255],
        [0, 178, 255],
        [0, 140, 48],
        [0, 140, 48],
        [0, 255, 0],
        [0, 255, 0],
        [153, 255, 0],
        [153, 255, 0],
        [178, 255, 0],
        [178, 255, 0],
        [216, 255, 0],
        [216, 255, 0],
        [255, 255, 0],
        [255, 255, 0],
        [255, 216, 0],
        [255, 216, 0],
        [255, 164, 0],
        [255, 164, 0],
        [255, 102, 0],
        [255, 102, 0],
        [255, 76, 0],
        [255, 76, 0],
        [178, 51, 0],
        [178, 51, 0],
        [153, 20, 47],
        [153, 20, 47],
        [126, 0, 43],
        [126, 0, 43],
        [255, 0, 216],
        [255, 0, 216],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0]], dtype="u1")


def old_ctth_temp_palette_data():
    return np.array([
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [255, 255, 255],
        [250, 250, 250],
        [246, 246, 246],
        [246, 246, 246],
        [242, 242, 242],
        [242, 242, 242],
        [238, 238, 238],
        [238, 238, 238],
        [234, 234, 234],
        [234, 234, 234],
        [230, 230, 230],
        [230, 230, 230],
        [226, 226, 226],
        [226, 226, 226],
        [222, 222, 222],
        [222, 222, 222],
        [218, 218, 218],
        [218, 218, 218],
        [214, 214, 214],
        [214, 214, 214],
        [210, 210, 210],
        [210, 210, 210],
        [206, 206, 206],
        [206, 206, 206],
        [202, 202, 202],
        [202, 202, 202],
        [198, 198, 198],
        [198, 198, 198],
        [194, 194, 194],
        [194, 194, 194],
        [190, 190, 190],
        [190, 190, 190],
        [186, 186, 186],
        [186, 186, 186],
        [182, 182, 182],
        [182, 182, 182],
        [178, 178, 178],
        [178, 178, 178],
        [174, 174, 174],
        [174, 174, 174],
        [170, 170, 170],
        [170, 170, 170],
        [166, 166, 166],
        [166, 166, 166],
        [162, 162, 162],
        [162, 162, 162],
        [158, 158, 158],
        [158, 158, 158],
        [154, 154, 154],
        [154, 154, 154],
        [150, 150, 150],
        [150, 150, 150],
        [146, 146, 146],
        [146, 146, 146],
        [142, 142, 142],
        [142, 142, 142],
        [138, 138, 138],
        [138, 138, 138],
        [134, 134, 134],
        [134, 134, 134],
        [130, 130, 130],
        [130, 130, 130],
        [126, 126, 126],
        [126, 126, 126],
        [122, 122, 122],
        [122, 122, 122],
        [118, 118, 118],
        [118, 118, 118],
        [114, 114, 114],
        [114, 114, 114],
        [110, 110, 110],
        [110, 110, 110],
        [106, 106, 106],
        [106, 106, 106],
        [102, 102, 102],
        [102, 102, 102],
        [98, 98, 98],
        [98, 98, 98],
        [94, 94, 94],
        [94, 94, 94],
        [90, 90, 90],
        [90, 90, 90],
        [86, 86, 86],
        [86, 86, 86],
        [82, 82, 82],
        [82, 82, 82],
        [78, 78, 78],
        [78, 78, 78],
        [74, 74, 74],
        [74, 74, 74],
        [70, 70, 70],
        [70, 70, 70],
        [66, 66, 66],
        [66, 66, 66],
        [62, 62, 62],
        [62, 62, 62],
        [58, 58, 58],
        [58, 58, 58],
        [54, 54, 54],
        [54, 54, 54],
        [50, 50, 50],
        [50, 50, 50],
        [46, 46, 46],
        [46, 46, 46],
        [42, 42, 42],
        [42, 42, 42],
        [38, 38, 38],
        [38, 38, 38],
        [34, 34, 34],
        [34, 34, 34],
        [30, 30, 30],
        [30, 30, 30],
        [26, 26, 26],
        [26, 26, 26],
        [22, 22, 22],
        [22, 22, 22],
        [18, 18, 18],
        [18, 18, 18],
        [14, 14, 14],
        [14, 14, 14],
        [10, 10, 10],
        [10, 10, 10],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [0, 0, 0],
        [78, 119, 145]], dtype="u1")
