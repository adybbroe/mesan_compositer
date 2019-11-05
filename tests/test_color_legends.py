#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

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

"""Testing the color legends for the cloudtype and ctth imagery."""

import unittest
from mesan_compositer import ctth_height
from mesan_compositer import nwcsaf_cloudtype

CTYPE_PALETTE = [(100.0, 100.0, 100.0), (0.0, 120.0, 0.0), (0.0, 0.0, 0.0),
                 (250.0, 190.0, 250.0), (220.0, 160.0, 220.0), (255.0, 150.0, 0.0),
                 (255.0, 100.0, 0.0), (255.0, 220.0, 0.0), (255.0, 180.0, 0.0),
                 (255.0, 255.0, 140.0), (240.0, 240.0, 0.0), (250.0, 240.0, 200.0),
                 (215.0, 215.0, 150.0), (255.0, 255.0, 255.0), (230.0, 230.0, 230.0),
                 (0.0, 80.0, 215.0), (0.0, 180.0, 230.0), (0.0, 240.0, 240.0), (90.0, 200.0, 160.0),
                 (200.0, 0.0, 200.0), (95.0, 60.0, 30.0)]

CTTH_HEIGHT_PALETTE = [(0.0, 0.0, 0.0), (255.0, 0.0, 216.0), (126.0, 0.0, 43.0),
                       (153.0, 20.0, 47.0), (178.0, 51.0, 0.0), (255.0, 76.0, 0.0),
                       (255.0, 102.0, 0.0), (255.0, 164.0, 0.0), (255.0, 216.0, 0.0),
                       (216.0, 255.0, 0.0), (178.0, 255.0, 0.0), (153.0, 255.0, 0.0),
                       (0.0, 255.0, 0.0), (0.0, 140.0, 48.0), (0.0, 178.0, 255.0),
                       (0.0, 216.0, 255.0), (0.0, 255.0, 255.0), (238.0, 214.0, 210.0),
                       (239.0, 239.0, 223.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (255.0, 255.0, 255.0), (255.0, 255.0, 255.0), (255.0, 255.0, 255.0),
                       (224.0, 224.0, 224.0)]


class TestColorPalettes(unittest.TestCase):
    """Test the functions to retrieve the color legends."""

    def setUp(self):
        """Set up."""
        return

    def test_cloudtype_palette(self):
        """Test the retrieval of the cloudtype palette."""
        retv = nwcsaf_cloudtype()
        self.assertEqual(len(retv), 21)
        self.assertListEqual(retv, CTYPE_PALETTE)

    def test_ctth_height_palette(self):
        """Test the retrieval of the cloudtype palette."""
        retv = ctth_height()
        self.assertEqual(len(retv), 100)
        self.assertListEqual(retv, CTTH_HEIGHT_PALETTE)

    def tearDown(self):
        """Clean up."""
        return


def suite():
    """Run all the tests."""
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestColorPalettes))

    return mysuite
