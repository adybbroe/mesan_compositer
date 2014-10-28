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

"""Unit testing the composite generation
"""

import unittest
import numpy as np
from mesan_compositer.composite_tools import get_weight_cloudtype
from datetime import datetime, timedelta

CTYPE_MSG = np.array([[6, 6, 6, 6, 6, 6, 6, 6, 6, 6],
                      [6, 6, 6, 6, 6, 6, 6, 19, 6, 6],
                      [6, 6, 6, 6, 6, 6, 19, 19, 19, 6],
                      [6, 6, 6, 6, 1, 19, 19, 19, 19, 6],
                      [6, 6, 1, 1, 1, 19, 19, 19, 19, 6],
                      [1, 1, 1, 1, 1, 19, 19, 6, 6, 6],
                      [1, 1, 1, 1, 1, 6, 6, 6, 6, 6],
                      [1, 1, 1, 6, 6, 6, 6, 6, 6, 6],
                      [1, 6, 6, 6, 6, 6, 6, 6, 6, 6],
                      [6, 6, 6, 6, 6, 6, 6, 6, 6, 6]], 'uint8')

CTYPE_MSG_FLAG = np.array([[128, 128, 128, 128, 128, 128, 128, 128, 128, 128],
                           [128, 128, 128, 128, 128, 128, 128, 640, 128, 128],
                           [128, 128, 128, 128, 128, 128, 640, 640, 640, 128],
                           [128, 128, 128, 128, 640, 640, 640, 640, 640, 128],
                           [128, 128, 640, 640, 640, 640, 640, 640, 640, 128],
                           [640, 640, 640, 640, 640, 640, 640, 128, 128, 128],
                           [640, 640, 640, 640, 640, 128, 128, 128, 128, 128],
                           [640, 640, 640, 128, 128, 128, 128, 128, 128, 128],
                           [640, 128, 128, 128, 128, 128, 128, 128, 128, 128],
                           [128, 128, 128, 128, 128, 128, 128, 128, 128, 128]], 'int16')


LAT_MSG = np.ones((10, 10), 'float') * 51.5
LAT_MSG2 = np.ones((10, 10), 'float') * 60.0

TDIFF_MSG = timedelta(seconds=0)
IS_MSG_MSG = np.array([[True, True, True, True, True,
                        True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True, True, True, True, True, True]])

WEIGHT_MSG = np.array([[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                       [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.475, 1.0, 1.0],
                       [1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
                           0.475, 0.475, 0.475, 1.0],
                       [1.0, 1.0, 1.0, 1.0, 0.475, 0.475,
                           0.475, 0.475, 0.475, 1.0],
                       [1.0, 1.0, 0.475, 0.475, 0.475,
                           0.475, 0.475, 0.475, 0.475, 1.0],
                       [0.475, 0.475, 0.475, 0.475,
                           0.475, 0.475, 0.475, 1.0, 1.0, 1.0],
                       [0.475, 0.475, 0.475,
                           0.475, 0.475, 1.0, 1.0, 1.0, 1.0, 1.0],
                       [0.475, 0.475,
                           0.475, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                       [0.475, 1.0, 1.0, 1.0,
                           1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                       [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]], 'float')
WEIGHT_MSG2 = np.array([[0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391,
                         0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391],
                        [0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391,
                         0.65217391,  0.65217391,  0.30978261,  0.65217391,  0.65217391],
                        [0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391,
                         0.65217391,  0.30978261,  0.30978261,  0.30978261,  0.65217391],
                        [0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.30978261,
                         0.30978261,  0.30978261,  0.30978261,  0.30978261,  0.65217391],
                        [0.65217391,  0.65217391,  0.30978261,  0.30978261,  0.30978261,
                         0.30978261,  0.30978261,  0.30978261,  0.30978261,  0.65217391],
                        [0.30978261,  0.30978261,  0.30978261,  0.30978261,  0.30978261,
                         0.30978261,  0.30978261,  0.65217391,  0.65217391,  0.65217391],
                        [0.30978261,  0.30978261,  0.30978261,  0.30978261,  0.30978261,
                         0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391],
                        [0.30978261,  0.30978261,  0.30978261,  0.65217391,  0.65217391,
                         0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391],
                        [0.30978261,  0.65217391,  0.65217391,  0.65217391,  0.65217391,
                         0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391],
                        [0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391,
                         0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391]], 'float')


class TestCloudTypeWeights(unittest.TestCase):

    """Unit testing the functions to convert msg flags to pps (old) flags"""

    def setUp(self):
        """Set up"""
        return

    def test_cloudtype_weights(self):
        """Test the derivation of weights for a given cloudtype , flags, obs times etc"""

        retv = get_weight_cloudtype(
            CTYPE_MSG, CTYPE_MSG_FLAG, LAT_MSG, TDIFF_MSG, IS_MSG_MSG)
        self.assertTrue(np.allclose(retv, WEIGHT_MSG))

        retv = get_weight_cloudtype(
            CTYPE_MSG, CTYPE_MSG_FLAG, LAT_MSG2, TDIFF_MSG, IS_MSG_MSG)
        self.assertTrue(np.allclose(retv, WEIGHT_MSG2))

    def tearDown(self):
        """Clean up"""
        return


def suite():
    """The suite for test_compositer.
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestCloudTypeWeights))

    return mysuite
