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
        res = bits2value([1, 0, 1, 1])
        self.assertEqual(res, 13)
        res = ctype_convert_flags(np.array([13], 'int32'),
                                  np.array([0], 'int32'),
                                  np.array([0], 'int32'))
        bits = value2bits(res[0])
        self.assertEqual(
            bits, [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 1])

        res = bits2value([0, 1, 1, 1])  # Twilight + sunglint
        self.assertEqual(res, 14)
        res = ctype_convert_flags(np.array([0], 'int32'),
                                  np.array([res], 'int32'),
                                  np.array([0], 'int32'))
        bits = value2bits(res[0])
        self.assertEqual(bits, [0, 0, 0, 1, 1])

        res = bits2value([0, 0, 1, 0])  # Day and no sunglint
        self.assertEqual(res, 4)
        res = ctype_convert_flags(np.array([0], 'int32'),
                                  np.array([res], 'int32'),
                                  np.array([0], 'int32'))
        bits = value2bits(res[0])
        self.assertEqual(bits, [])

        res = bits2value([0, 1, 0, 0])  # Night and no sunglint
        self.assertEqual(res, 2)
        res = ctype_convert_flags(np.array([0], 'int32'),
                                  np.array([res], 'int32'),
                                  np.array([0], 'int32'))
        bits = value2bits(res[0])
        self.assertEqual(bits, [0, 0, 1])

        res = bits2value([0, 1, 0, 0])  # Land and coast

        self.assertEqual(res, 2)
        res = ctype_convert_flags(np.array([0], 'int32'),
                                  np.array([res], 'int32'),
                                  np.array([0], 'int32'))
        bits = value2bits(res[0])
        self.assertEqual(bits, [0, 0, 1])

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

        # Non processed:
        res = bits2value([1, 0, 0, ])
        self.assertEqual(res, 1)
        res = ctth_convert_flags(np.array([0], 'int32'),
                                 np.array([0], 'int32'),
                                 np.array([1], 'int32'))
        bits = value2bits(res[0])
        self.assertEqual(bits, [1, ])

        # Quality assessment - Interpolated => low confidence
        res = bits2value([0, 0, 0, 0, 0, 1])
        self.assertEqual(res, 32)
        res = ctth_convert_flags(np.array([0], 'int32'),
                                 np.array([0], 'int32'),
                                 np.array([32], 'int32'))
        bits = value2bits(res[0])
        self.assertEqual(bits, [0, 1, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 1, 1, ])

        # Quality assessment - Bad => low confidence
        res = bits2value([0, 0, 0, 1, 1, 0])
        self.assertEqual(res, 24)
        res = ctth_convert_flags(np.array([0], 'int32'),
                                 np.array([0], 'int32'),
                                 np.array([24], 'int32'))
        bits = value2bits(res[0])
        self.assertEqual(bits, [0, 1, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 1, 1, ])
        # Quality assessment - Questionable => low confidence
        res = bits2value([0, 0, 0, 0, 1, 0])
        self.assertEqual(res, 16)
        res = ctth_convert_flags(np.array([0], 'int32'),
                                 np.array([0], 'int32'),
                                 np.array([16], 'int32'))
        bits = value2bits(res[0])
        self.assertEqual(bits, [0, 1, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 1, 1, ])

        # Cloudy and Opaque clouds:
        res = bits2value([0, 0, 1, 1, ])
        self.assertEqual(res, 12)
        res = ctth_convert_flags(np.array([12], 'int32'),
                                 np.array([0], 'int32'),
                                 np.array([0], 'int32'))
        bits = value2bits(res[0])
        self.assertEqual(bits, [0, 1, 1, ])

        # Inversion, RTTOV and window technique:
        res = bits2value([0, 0, 0, 0, 1, 0, 1, 1, ])
        self.assertEqual(res, 208)
        res = ctth_convert_flags(np.array([208], 'int32'),
                                 np.array([0], 'int32'),
                                 np.array([0], 'int32'))

        bits = value2bits(res[0])
        self.assertEqual(bits, [0, 1, 0, 0, 0, 1, 0, 1, 1, ])

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
        bits = value2bits(res[0])
        self.assertEqual(
            bits, [0, 1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1])

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
