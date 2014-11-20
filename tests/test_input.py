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

"""
"""


from mesan_compositer.composite_tools import (get_ppslist,
                                              get_msglist,
                                              MSGSATS)

PPS_CTYPES = ['noaa16_20120625_1133_60621_satproj_00000_04059_cloudtype.h5',
              'noaa19_20120625_1222_17419_satproj_00000_05500_cloudtype.h5',
              'npp_20120625_1148_03421_satproj_00000_09215_cloudtype.h5'
              ]
PPS_CTYPES2 = ['noaa19_20120625_1222_17419_satproj_00000_05500_cloudtype.h5',
               'npp_20120625_1148_03421_satproj_00000_09215_cloudtype.h5'
               ]

MSG_CTYPES = ['SAFNWC_MSG2_CT___201206251130_EuropeCanary.PLAX.CTTH.0.h5',
              'SAFNWC_MSG2_CT___201206251200_EuropeCanary.PLAX.CTTH.0.h5',
              'SAFNWC_MSG2_CT___201206251215_EuropeCanary.PLAX.CTTH.0.h5',
              'SAFNWC_MSG2_CT___201206251230_EuropeCanary.PLAX.CTTH.0.h5',
              'SAFNWC_MSG2_CT___201206251145_EuropeCanary.PLAX.CTTH.0.h5'
              ]

PPS_BASEDIR = "/media/satdata/mesan/satin/pps/"
PPS_CT_FILES = [PPS_BASEDIR + s for s in PPS_CTYPES]
MSG_BASEDIR = "/media/satdata/mesan/satin/msg/"
MSG_CT_FILES = [MSG_BASEDIR + s for s in MSG_CTYPES]

from datetime import datetime, timedelta
OBS_TIME1 = datetime(2012, 6, 25, 12, 0)

import unittest
import numpy as np


class TestGetCloudtypes(unittest.TestCase):

    """Unit testing the function to get pps/msg cloudtypes"""

    def setUp(self):
        """Set up"""
        return

    def test_pps_ctypes(self):
        """Test get the pps cloud type metadata from a list of files"""

        delta_t = timedelta(minutes=40)
        twindow = OBS_TIME1 - delta_t, OBS_TIME1 + delta_t
        ppslist = get_ppslist(PPS_CT_FILES, twindow)
        self.assertEqual(len(ppslist), 3)
        prenames = []
        for item in ppslist:
            fname = ('%s%s_' % (item.platform, item.number) +
                     item.timeslot.strftime('%Y%m%d_%H%M') + '_%s' % str(item.orbit))
            prenames.append(fname)

        nlist = [s.split('_satproj')[0] for s in PPS_CTYPES]
        for item in prenames:
            self.assertTrue(item in nlist)

        delta_t = timedelta(minutes=25)
        twindow = OBS_TIME1 - delta_t, OBS_TIME1 + delta_t
        ppslist = get_ppslist(PPS_CT_FILES, twindow)
        self.assertEqual(len(ppslist), 2)
        prenames = []
        for item in ppslist:
            fname = ('%s%s_' % (item.platform, item.number) +
                     item.timeslot.strftime('%Y%m%d_%H%M') + '_%s' % str(item.orbit))
            prenames.append(fname)

        nlist = [s.split('_satproj')[0] for s in PPS_CTYPES2]
        for item in prenames:
            self.assertTrue(item in nlist)

        self.assertFalse('noaa16_20120625_1133_60621' in prenames)

    def test_msg_ctypes(self):
        """Test get the msg cloud type metadata from a list of files"""

        delta_t = timedelta(minutes=40)
        twindow = OBS_TIME1 - delta_t, OBS_TIME1 + delta_t
        msglist = get_msglist(MSG_CT_FILES, twindow, 'EuroCanary')
        self.assertEqual(len(msglist), 0)
        msglist = get_msglist(MSG_CT_FILES, twindow, 'EuropeCanary')
        self.assertEqual(len(msglist), 5)
        prenames = []

        # SAFNWC_MSG2_CT___201206251130_EuropeCanary
        for item in msglist:
            fname = ('SAFNWC_%s_CT___' % MSGSATS.get(str(item.platform) + str(item.number), 'MSGx') +
                     item.timeslot.strftime('%Y%m%d%H%M') + '_%s' % str(item.areaid))
            prenames.append(fname)

        nlist = [s.split('.PLAX')[0] for s in MSG_CTYPES]
        for item in prenames:
            self.assertTrue(item in nlist)

    def tearDown(self):
        """Clean up"""
        return


def suite():
    """The suite for test_blackbody.
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestGetCloudtypes))

    return mysuite
