#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015, 2019 Adam.Dybbroe

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
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Unit testing the pps format conversions
"""

from nwcsaf_formats.pps_conversions import (map_cloudtypes,
                                            ctype_convert_flags,
                                            ctth_convert_flags,
                                            old_processing_flag_palette
                                            )
from mesan_compositer.pps_msg_conversions import (get_bit_from_flags,
                                                  bits2value,
                                                  value2bits)
import unittest
import numpy as np


CTYPES_2012 = np.array(
    [0, 1, 2, 3, 4, 6, 8, 10, 12, 14, 15, 16, 17, 18, 19, 20], np.uint8)
CTYPES_2014 = np.array(
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 10, 255], np.uint8)


class TestCtypeConversions(unittest.TestCase):

    """Unit testing the functions to convert from new pps format content to
    old"""

    def setUp(self):
        """Set up"""
        return

    def test_map_cloudtypes(self):
        """Test mapping from new to old cloudtypes"""

        res = map_cloudtypes(CTYPES_2014)
        self.assertEqual(res.tolist(), CTYPES_2012.tolist())

    def test_map_cloudtype_flags(self):
        """Test mapping the flags from new to old cloudtype"""

        # Low level inv, sea ice available and sea ice acc to ext map
        res = ctype_convert_flags(np.array([13], 'int32'),
                                  np.array([0], 'int32'),
                                  np.array([0], 'int32'))

        bits = get_bit_from_flags(res[0], range(16))
        np.testing.assert_allclose(
            bits, np.array([0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 1], dtype=np.int8))

        # [0, 1, 1, 1]  # Twilight + sunglint
        res = 14
        res = ctype_convert_flags(np.array([0], 'int32'),
                                  np.array([res], 'int32'),
                                  np.array([0], 'int32'))
        bits = get_bit_from_flags(res[0], range(5))
        np.testing.assert_allclose(bits, np.array([0, 0, 0, 1, 1], dtype=np.int8))

        res = 4  # [0, 0, 1, 0]) Day and no sunglint
        res = ctype_convert_flags(np.array([0], 'int32'),
                                  np.array([res], 'int32'),
                                  np.array([0], 'int32'))

        bits = get_bit_from_flags(res[0], range(5))
        np.testing.assert_allclose(bits, np.array([0, 0, 0, 0, 0], dtype=np.int8))

        res = 2  # [0, 1, 0, 0]  Night and no sunglint
        res = ctype_convert_flags(np.array([0], 'int32'),
                                  np.array([res], 'int32'),
                                  np.array([0], 'int32'))

        bits = get_bit_from_flags(res[0], range(3))
        np.testing.assert_allclose(bits, np.array([0, 0, 1], dtype=np.int8))

    def tearDown(self):
        """Clean up"""
        return


class TestCtthConversions(unittest.TestCase):

    """Unit testing the functions to convert from new pps format content to
    old, CTTH product"""

    def setUp(self):
        """Set up"""
        return

    def test_map_ctth_flags(self):
        """Test mapping the flags from new to old ctth product"""

        res = 1  # [1, 0, 0, ]   Non processed
        res = ctth_convert_flags(np.array([0], 'int32'),
                                 np.array([0], 'int32'),
                                 np.array([res], 'int32'))
        bits = get_bit_from_flags(res[0], range(3))
        np.testing.assert_allclose(bits, np.array([1, 0, 0], dtype=np.int8))

        res = 32  # [0, 0, 0, 0, 0, 1] Quality assessment - Interpolated => low confidence
        res = ctth_convert_flags(np.array([0], 'int32'),
                                 np.array([0], 'int32'),
                                 np.array([32], 'int32'))

        bits = get_bit_from_flags(res[0], range(16))
        np.testing.assert_allclose(bits, np.array([0, 1, 0, 0, 0, 0, 0, 0,
                                                   0, 0, 0, 0, 0, 0, 1, 1], dtype=np.int8))

        res = 24  # [0, 0, 0, 1, 1, 0]  Quality assessment - Bad => low confidence
        res = ctth_convert_flags(np.array([0], 'int32'),
                                 np.array([0], 'int32'),
                                 np.array([24], 'int32'))
        bits = get_bit_from_flags(res[0], range(16))
        expected = np.array([0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1], dtype=np.int8)
        np.testing.assert_allclose(bits, expected)

        res = 16  # [0, 0, 0, 0, 1, 0]  # Quality assessment - Questionable => low confidence
        res = ctth_convert_flags(np.array([0], 'int32'),
                                 np.array([0], 'int32'),
                                 np.array([16], 'int32'))
        bits = get_bit_from_flags(res[0], range(16))
        expected = np.array([0, 1, 0, 0, 0, 0, 0, 0,
                             0, 0, 0, 0, 0, 0, 1, 1], dtype=np.int8)
        np.testing.assert_allclose(bits, expected)

        res = 12  # [0, 0, 1, 1, ]  # Cloudy and Opaque clouds:
        res = ctth_convert_flags(np.array([12], 'int32'),
                                 np.array([0], 'int32'),
                                 np.array([0], 'int32'))
        bits = get_bit_from_flags(res[0], range(3))
        expected = np.array([0, 1, 1], dtype=np.int8)
        np.testing.assert_allclose(bits, expected)

        res = 208  # [0, 0, 0, 0, 1, 0, 1, 1, ]  # Inversion, RTTOV and window technique:
        res = ctth_convert_flags(np.array([res], 'int32'),
                                 np.array([0], 'int32'),
                                 np.array([0], 'int32'))
        bits = get_bit_from_flags(res[0], range(16))
        expected = np.array([0, 1, 0, 0, 0, 1, 0, 1,
                             1, 0, 0, 0, 0, 0, 0, 0], dtype=np.int8)
        np.testing.assert_allclose(bits, expected)

        # condition flags:
        # value2bits(21794)
        # [0, 1, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0, 1, 0, 1]

        # quality flags:
        # value2bits(32)
        # [0, 0, 0, 0, 0, 1] - interpolated

        # Status flags:
        # value2bits(192)
        # [0, 0, 0, 0, 0, 0, 1, 1] - rttov and window tech

        # Result:
        # result = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1]
        res = ctth_convert_flags(np.array([192], 'int32'),
                                 np.array([21794], 'int32'),
                                 np.array([32], 'int32'))
        bits = get_bit_from_flags(res[0], range(16))
        expected = np.array([0, 1, 0, 0, 0, 0, 0, 1,
                             1, 0, 0, 0, 0, 0, 1, 1], dtype=np.int8)
        np.testing.assert_allclose(bits, expected)

    def tearDown(self):
        """Clean up"""
        return


class TestFlagConversions(unittest.TestCase):

    """Unit testing the functions to get the old flag palettes etc"""

    def setUp(self):
        """Set up"""
        return

    def test_old_flags(self):
        """Test retrieving the old flag palettes"""

        retv = old_processing_flag_palette('cloudtype')
        self.assertEqual(retv[3][0], '8: Twilight')
        retv = old_processing_flag_palette('ctth')
        self.assertEqual(retv[4][0], '16: 16: Missing NWP data')
        self.assertRaises(
            NotImplementedError, old_processing_flag_palette, 'pc')

    def tearDown(self):
        """Clean up"""
        return


def suite():
    """The suite for test_pps_conversions
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestFlagConversions))
    mysuite.addTest(loader.loadTestsFromTestCase(TestCtypeConversions))
    mysuite.addTest(loader.loadTestsFromTestCase(TestCtthConversions))

    return mysuite
