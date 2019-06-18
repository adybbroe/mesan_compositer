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

"""Utilities to go from new pps format to old and from msg (old) format to pps
old format
"""

import numpy as np

import logging
LOG = logging.getLogger(__name__)


def bits2value(bitlist):
    """Enter a list of zeros and ones as a bit list and calculate the value"""
    val = 0
    for i, bit in enumerate(bitlist):
        val = val + bit * 2 ** i

    return val


def value2bits(val):
    """Converting decimal to binary: Take an integer and calculate the bits"""
    bits = []
    tmp = 1 * val
    while tmp != 0:
        val = 1 * tmp
        tmp = val / 2
        res = val - tmp * 2
        bits.append(res)

    return bits


def get_bit_from_flags(arr, nbit):
    """Check an array if a given bit is set. It outputs 1 when the bit is set
    and 0 if it is not set
    """
    res = np.bitwise_and(np.right_shift(arr, nbit), 1)
    return res.astype('b')


def ctth_procflags2pps_old(data):
    """Convert ctth processing flags from MSG to PPS format.
    """

    ones = np.ones(data.shape, "h")

    # 2 bits to define processing status
    # (maps to pps bits 0 and 1:)
    is_bit0_set = get_bit_from_flags(data, 0)
    is_bit1_set = get_bit_from_flags(data, 1)
    proc = (is_bit0_set * np.left_shift(ones, 0) +
            is_bit1_set * np.left_shift(ones, 1))
    del is_bit0_set
    del is_bit1_set

    # Non-processed?
    # If non-processed in msg (0) then set pps bit 0 and nothing else.
    # If non-processed in msg due to FOV is cloud free (1) then do not set any
    # pps bits.
    # If processed (because cloudy) with/without result in msg (2&3) then set
    # pps bit 1.

    arr = np.where(np.equal(proc, 0), np.left_shift(ones, 0), 0)
    arr = np.where(np.equal(proc, 2), np.left_shift(ones, 1), 0)
    arr = np.where(np.equal(proc, 3), np.left_shift(ones, 1), 0)
    retv = np.array(arr)
    del proc

    # 1 bit to define if RTTOV-simulations are available?
    # (maps to pps bit 3:)
    is_bit2_set = get_bit_from_flags(data, 2)
    proc = is_bit2_set

    # RTTOV-simulations available?

    arr = np.where(np.equal(proc, 1), np.left_shift(ones, 3), 0)
    retv = np.add(retv, arr)
    del is_bit2_set

    # 3 bits to describe NWP input data
    # (maps to pps bits 4&5:)
    is_bit3_set = get_bit_from_flags(data, 3)
    is_bit4_set = get_bit_from_flags(data, 4)
    is_bit5_set = get_bit_from_flags(data, 5)
    # Put together the three bits into a nwp-flag:
    nwp_bits = (is_bit3_set * np.left_shift(ones, 0) +
                is_bit4_set * np.left_shift(ones, 1) +
                is_bit5_set * np.left_shift(ones, 2))
    arr = np.where(np.logical_and(np.greater_equal(nwp_bits, 3),
                                  np.less_equal(nwp_bits, 5)),
                   np.left_shift(ones, 4),
                   0)
    arr = np.add(arr, np.where(np.logical_or(np.equal(nwp_bits, 2),
                                             np.equal(nwp_bits, 4)),
                               np.left_shift(ones, 5),
                               0))

    retv = np.add(retv, arr)
    del is_bit3_set
    del is_bit4_set
    del is_bit5_set

    # 2 bits to describe SEVIRI input data
    # (maps to pps bits 6:)
    is_bit6_set = get_bit_from_flags(data, 6)
    is_bit7_set = get_bit_from_flags(data, 7)
    # Put together the two bits into a seviri-flag:
    seviri_bits = (is_bit6_set * np.left_shift(ones, 0) +
                   is_bit7_set * np.left_shift(ones, 1))
    arr = np.where(np.greater_equal(seviri_bits, 2),
                   np.left_shift(ones, 6), 0)

    retv = np.add(retv, arr)
    del is_bit6_set
    del is_bit7_set

    # 4 bits to describe which method has been used
    # (maps to pps bits 7&8 and bit 2:)
    is_bit8_set = get_bit_from_flags(data, 8)
    is_bit9_set = get_bit_from_flags(data, 9)
    is_bit10_set = get_bit_from_flags(data, 10)
    is_bit11_set = get_bit_from_flags(data, 11)
    # Put together the four bits into a method-flag:
    method_bits = (is_bit8_set * np.left_shift(ones, 0) +
                   is_bit9_set * np.left_shift(ones, 1) +
                   is_bit10_set * np.left_shift(ones, 2) +
                   is_bit11_set * np.left_shift(ones, 3))
    arr = np.where(np.logical_or(
        np.logical_and(np.greater_equal(method_bits, 1),
                       np.less_equal(method_bits, 2)),
        np.equal(method_bits, 13)),
        np.left_shift(ones, 2),
        0)
    arr = np.add(arr,
                 np.where(np.equal(method_bits, 1),
                          np.left_shift(ones, 7),
                          0))
    arr = np.add(arr,
                 np.where(np.logical_and(
                     np.greater_equal(method_bits, 3),
                     np.less_equal(method_bits, 12)),
                     np.left_shift(ones, 8),
                     0))

    # (Maps directly - as well - to the spare bits 9-12)
    arr = np.add(arr, np.where(is_bit8_set, np.left_shift(ones, 9), 0))
    arr = np.add(arr, np.where(is_bit9_set,
                               np.left_shift(ones, 10),
                               0))
    arr = np.add(arr, np.where(is_bit10_set,
                               np.left_shift(ones, 11),
                               0))
    arr = np.add(arr, np.where(is_bit11_set,
                               np.left_shift(ones, 12),
                               0))
    retv = np.add(retv, arr)
    del is_bit8_set
    del is_bit9_set
    del is_bit10_set
    del is_bit11_set

    # 2 bits to describe the quality of the processing itself
    # (maps to pps bits 14&15:)
    is_bit12_set = get_bit_from_flags(data, 12)
    is_bit13_set = get_bit_from_flags(data, 13)
    # Put together the two bits into a quality-flag:
    qual_bits = (is_bit12_set * np.left_shift(ones, 0) +
                 is_bit13_set * np.left_shift(ones, 1))
    arr = np.where(np.logical_and(np.greater_equal(qual_bits, 1),
                                  np.less_equal(qual_bits, 2)),
                   np.left_shift(ones, 14), 0)
    arr = np.add(arr,
                 np.where(np.equal(qual_bits, 2),
                          np.left_shift(ones, 15),
                          0))

    retv = np.add(retv, arr)
    del is_bit12_set
    del is_bit13_set

    return retv.astype('h')


def ctth_procflags2pps(data):
    """Convert ctth processing flags from MSG to PPS format.
    """

    ones = np.ones(data.shape, "int32")

    # 2 bits to define processing status
    # (maps to pps bits 0 and 1:)
    is_bit0_set = get_bit_from_flags(data, 0)
    is_bit1_set = get_bit_from_flags(data, 1)
    proc = (is_bit0_set * np.left_shift(ones, 0) +
            is_bit1_set * np.left_shift(ones, 1))
    del is_bit0_set
    del is_bit1_set

    # Non-processed?
    # If non-processed in msg (0) then set pps bit 0 and nothing else.
    # If non-processed in msg due to FOV is cloud free (1) then also only set
    # pps bit 0
    # If processed (because cloudy) with result in msg (3) then set
    # pps bit 1.
    # If processed (because cloudy) without result in msg (2) then set
    # pps bit 0 (non-processed) and pps bit 1.
    arr = np.where(np.equal(proc, 0), np.left_shift(ones, 0), 0)
    arr = np.where(np.equal(proc, 1), np.left_shift(ones, 0), arr)
    arr = np.where(np.equal(proc, 3), np.left_shift(ones, 1), arr)
    arr = np.where(np.equal(proc, 2), (np.left_shift(ones, 0) +
                                       np.left_shift(ones, 1)), arr)
    retv = np.array(arr)
    del proc

    # 1 bit to define if RTTOV-simulations are available?
    # (maps to pps bit 3:)
    is_bit2_set = get_bit_from_flags(data, 2)
    proc = is_bit2_set

    # RTTOV-simulations available?
    arr = np.where(np.equal(proc, 1), np.left_shift(ones, 3), 0)
    retv = np.add(retv, arr)
    del is_bit2_set

    # 3 bits to describe NWP input data (maps to pps bits 4&5:)
    is_bit3_set = get_bit_from_flags(data, 3)
    is_bit4_set = get_bit_from_flags(data, 4)
    is_bit5_set = get_bit_from_flags(data, 5)
    # Put together the three bits into a nwp-flag:
    nwp_bits = (is_bit3_set * np.left_shift(ones, 0) +
                is_bit4_set * np.left_shift(ones, 1) +
                is_bit5_set * np.left_shift(ones, 2))
    arr = np.where(np.equal(nwp_bits, 5), np.left_shift(ones, 4), 0)
    arr = np.add(arr, np.where(np.logical_or(np.equal(nwp_bits, 2),
                                             np.equal(nwp_bits, 4)),
                               np.left_shift(ones, 5), 0))

    retv = np.add(retv, arr)

    del is_bit3_set
    del is_bit4_set
    del is_bit5_set

    # 2 bits to describe SEVIRI input data
    # (maps to pps bits 6:)
    is_bit6_set = get_bit_from_flags(data, 6)
    is_bit7_set = get_bit_from_flags(data, 7)
    # Put together the two bits into a seviri-flag:
    seviri_bits = (is_bit6_set * np.left_shift(ones, 0) +
                   is_bit7_set * np.left_shift(ones, 1))
    arr = np.where(np.greater_equal(seviri_bits, 2),
                   np.left_shift(ones, 6), 0)

    retv = np.add(retv, arr)
    del is_bit6_set
    del is_bit7_set

    # 4 bits to describe which method has been used
    # (maps to pps bits 7&8 and bit 2:)
    is_bit8_set = get_bit_from_flags(data, 8)
    is_bit9_set = get_bit_from_flags(data, 9)
    is_bit10_set = get_bit_from_flags(data, 10)
    is_bit11_set = get_bit_from_flags(data, 11)
    # Put together the four bits into a method-flag:
    method_bits = (is_bit8_set * np.left_shift(ones, 0) +
                   is_bit9_set * np.left_shift(ones, 1) +
                   is_bit10_set * np.left_shift(ones, 2) +
                   is_bit11_set * np.left_shift(ones, 3))
    arr = np.where(np.logical_or(
        np.logical_and(np.greater_equal(method_bits, 1),
                       np.less_equal(method_bits, 2)),
        np.equal(method_bits, 13)),
        np.left_shift(ones, 2),
        0)
    arr = np.add(arr, np.where(
                 np.logical_or(np.equal(method_bits, 1),
                               np.equal(method_bits, 13)),
                 np.left_shift(ones, 7),
                 0))  # Using rttov
    arr = np.add(arr, np.where(
                 np.logical_or(np.equal(method_bits, 1),
                               np.equal(method_bits, 13)),
                 np.left_shift(ones, 3),
                 0))  # rttov available

    arr = np.add(arr,
                 np.where(np.equal(method_bits, 13),
                          np.left_shift(ones, 5),
                          0))  # Thermal inversion
    arr = np.add(arr,
                 np.where(np.logical_and(
                     np.greater_equal(method_bits, 3),
                     np.less_equal(method_bits, 12)),
                     np.left_shift(ones, 8),
                     0))

    # (Maps directly - as well - to the spare bits 9-12)
    arr = np.add(arr, np.where(is_bit8_set, np.left_shift(ones, 9), 0))
    arr = np.add(arr, np.where(is_bit9_set,
                               np.left_shift(ones, 10),
                               0))
    arr = np.add(arr, np.where(is_bit10_set,
                               np.left_shift(ones, 11),
                               0))
    arr = np.add(arr, np.where(is_bit11_set,
                               np.left_shift(ones, 12),
                               0))
    retv = np.add(retv, arr)
    del is_bit8_set
    del is_bit9_set
    del is_bit10_set
    del is_bit11_set

    # 2 bits to describe the quality of the processing itself
    # (maps to pps bits 14&15:)
    is_bit12_set = get_bit_from_flags(data, 12)
    is_bit13_set = get_bit_from_flags(data, 13)
    # Put together the two bits into a quality-flag:
    qual_bits = (is_bit12_set * np.left_shift(ones, 0) +
                 is_bit13_set * np.left_shift(ones, 1))
    arr = np.where(np.logical_and(np.greater_equal(qual_bits, 1),
                                  np.less_equal(qual_bits, 2)),
                   np.left_shift(ones, 14), 0)
    arr = np.add(arr,
                 np.where(np.equal(qual_bits, 2),
                          np.left_shift(ones, 15),
                          0))

    retv = np.add(retv, arr)
    del is_bit12_set
    del is_bit13_set

    return retv.astype('int32')


def ctype_procflags2pps(data):
    """Converting cloud type processing flags to the PPS format, in order to
    have consistency between PPS and MSG cloud type contents.
    """

    ones = np.ones(data.shape, "h")

    # msg illumination bit 0,1,2 (undefined,night,twilight,day,sunglint) maps
    # to pps bits 2, 3 and 4:
    is_bit0_set = get_bit_from_flags(data, 0)
    is_bit1_set = get_bit_from_flags(data, 1)
    is_bit2_set = get_bit_from_flags(data, 2)
    illum = is_bit0_set * np.left_shift(ones, 0) + \
        is_bit1_set * np.left_shift(ones, 1) + \
        is_bit2_set * np.left_shift(ones, 2)
    del is_bit0_set
    del is_bit1_set
    del is_bit2_set
    # Night?
    # If night in msg then set pps night bit and nothing else.
    # If twilight in msg then set pps twilight bit and nothing else.
    # If day in msg then unset both the pps night and twilight bits.
    # If sunglint in msg unset both the pps night and twilight bits and set the
    # pps sunglint bit.
    arr = np.where(np.equal(illum, 1), np.left_shift(ones, 2), 0)
    arr = np.where(np.equal(illum, 2), np.left_shift(ones, 3), arr)
    arr = np.where(np.equal(illum, 3), 0, arr)
    arr = np.where(np.equal(illum, 4), np.left_shift(ones, 4), arr)
    retv = np.array(arr)
    del illum

    # msg nwp-input bit 3 (nwp present?) maps to pps bit 7:
    # msg nwp-input bit 4 (low level inversion?) maps to pps bit 6:
    is_bit3_set = get_bit_from_flags(data, 3)
    is_bit4_set = get_bit_from_flags(data, 4)
    nwp = (is_bit3_set * np.left_shift(ones, 0) +
           is_bit4_set * np.left_shift(ones, 1))
    del is_bit3_set
    del is_bit4_set

    arr = np.where(np.equal(nwp, 1), np.left_shift(ones, 7), 0)
    arr = np.where(np.equal(nwp, 2), np.left_shift(ones, 7) +
                   np.left_shift(ones, 6), arr)
    arr = np.where(np.equal(nwp, 3), 0, arr)
    retv = np.add(arr, retv)
    del nwp

    # msg seviri-input bits 5&6 maps to pps bit 8:
    is_bit5_set = get_bit_from_flags(data, 5)
    is_bit6_set = get_bit_from_flags(data, 6)
    seviri = (is_bit5_set * np.left_shift(ones, 0) +
              is_bit6_set * np.left_shift(ones, 1))
    del is_bit5_set
    del is_bit6_set

    retv = np.add(retv,
                  np.where(np.logical_or(np.equal(seviri, 2),
                                         np.equal(seviri, 3)),
                           np.left_shift(ones, 8), 0))
    del seviri

    # msg quality bits 7&8 maps to pps bit 9&10:
    is_bit7_set = get_bit_from_flags(data, 7)
    is_bit8_set = get_bit_from_flags(data, 8)
    quality = (is_bit7_set * np.left_shift(ones, 0) +
               is_bit8_set * np.left_shift(ones, 1))
    del is_bit7_set
    del is_bit8_set

    arr = np.where(np.equal(quality, 2), np.left_shift(ones, 9), 0)
    arr = np.where(np.equal(quality, 3), np.left_shift(ones, 10), arr)
    retv = np.add(arr, retv)
    del quality

    # msg bit 9 (stratiform-cumuliform distinction?) maps to pps bit 11:
    is_bit9_set = get_bit_from_flags(data, 9)
    retv = np.add(retv,
                  np.where(is_bit9_set,
                           np.left_shift(ones, 11),
                           0))
    del is_bit9_set

    return retv.astype('h')
