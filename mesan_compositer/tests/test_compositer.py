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

LAT_MSG = np.array([[51.953189120658557, 51.946698247301342, 51.940201848748003, 51.933699927860346, 51.927192487500562, 51.920679530531395, 51.914161059816038, 51.907637078218166, 51.901107588601903, 51.894572593831853],
                    [51.937914555953903, 51.931426048275668, 51.924932016556454, 51.918432463655222, 51.911927392431402,
                        51.905416805744927, 51.898900706456203, 51.892379097426101, 51.885851981515948, 51.879319361587577],
                    [51.92263904657873, 51.916152903189932, 51.909661236915603, 51.903164050611934, 51.896661347135556,
                        51.890153129343588, 51.883639400093649, 51.877120162243834, 51.870595418652691, 51.864065172179217],
                    [51.907362593694295, 51.900878813206717, 51.894389510989406, 51.887894689895766, 51.88139435277963,
                        51.874888502495338, 51.868377141897703, 51.861860273842026, 51.855337901184093, 51.848810026780107],
                    [51.892085198460009, 51.885603779486793, 51.879116839939982, 51.872624382670189, 51.866126410528452,
                        51.85962292636632, 51.853113933035843, 51.846599433389521, 51.840079430280333, 51.833553926561748],
                    [51.87680686203354, 51.870327803189177, 51.863843224927692, 51.857353130096897, 51.850857521545052,
                        51.84435640212093, 51.837849774673785, 51.831337642053342, 51.824820007109807, 51.818296872693857],
                    [51.861527585570776, 51.855050885471094, 51.848568667111095, 51.842080933335801, 51.835587686990685,
                        51.829088930921756, 51.82258466797547, 51.816074900998785, 51.809559632839118, 51.80303886634438],
                    [51.846247370225882, 51.839773027488064, 51.833293167647042, 51.826807793545093, 51.820316908024893,
                        51.813820513929663, 51.807318614103096, 51.800811211389366, 51.794298308633131, 51.787779908679518],
                    [51.8309662171512, 51.824494230393746, 51.818016727690562, 51.811533711881133, 51.805045185805362,
                        51.798551152303688, 51.792051614217044, 51.785546574386821, 51.77903603565489, 51.772520000863622],
                    [51.815684127497327, 51.809214495340122, 51.802739348394965, 51.796258689498572, 51.789772521488082, 51.783280847201162, 51.776783669475954, 51.770280991151097, 51.763772815065693, 51.757259144059354]], 'float')

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
                       [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
                           0.47499999999999998, 1.0, 1.0],
                       [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.47499999999999998,
                           0.47499999999999998, 0.47499999999999998, 1.0],
                       [1.0, 1.0, 1.0, 1.0, 0.47499999999999998, 0.47499999999999998,
                           0.47499999999999998, 0.47499999999999998, 0.47499999999999998, 1.0],
                       [1.0, 1.0, 0.47499999999999998, 0.47499999999999998, 0.47499999999999998,
                           0.47499999999999998, 0.47499999999999998, 0.47499999999999998, 0.47499999999999998, 1.0],
                       [0.47499999999999998, 0.47499999999999998, 0.47499999999999998, 0.47499999999999998,
                           0.47499999999999998, 0.47499999999999998, 0.47499999999999998, 1.0, 1.0, 1.0],
                       [0.47499999999999998, 0.47499999999999998, 0.47499999999999998,
                           0.47499999999999998, 0.47499999999999998, 1.0, 1.0, 1.0, 1.0, 1.0],
                       [0.47499999999999998, 0.47499999999999998,
                           0.47499999999999998, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                       [0.47499999999999998, 1.0, 1.0, 1.0,
                           1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                       [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]], 'float')


class TestCloudTypeWeights(unittest.TestCase):

    """Unit testing the functions to convert msg flags to pps (old) flags"""

    def setUp(self):
        """Set up"""
        return

    def test_cloudtype_weights(self):
        """Test the derivation of weights for a given cloudtype , flags, obs times etc"""

        retv = get_weight_cloudtype(
            CTYPE_MSG, CTYPE_MSG_FLAG, LAT_MSG, TDIFF_MSG, IS_MSG_MSG)
        self.assertTrue(np.alltrue(retv == WEIGHT_MSG))

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
