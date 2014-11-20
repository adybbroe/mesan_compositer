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
    arr = np.where(np.logical_and(is_bit4_set, is_bit5_set == 0),
                   np.left_shift(ones, 0), 0)
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
