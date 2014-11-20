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
                                            ctype_convert_flags)
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


def suite():
    """The suite for test_pps_conversions
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestCtypeConversions))

    return mysuite
