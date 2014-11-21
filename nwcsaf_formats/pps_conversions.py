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
