#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019, 2023 Adam.Dybbroe

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

"""Test the generation of the cloudtype composite."""

import sys
from datetime import datetime, timedelta

from mesan_compositer.make_ct_composite import ctCompositer
from mesan_compositer.composite_tools import PpsMetaData
from mesan_compositer.composite_tools import GeoMetaData

if sys.version_info < (3,):
    from mock import patch
else:
    from unittest.mock import patch

_PATTERN = ('S_NWC_{product:s}_{platform_name:s}_{orbit:05d}_' +
            '{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z.nc')

CONFIG_OPTIONS = {'ct_composite_filename': 'mesan_composite_%(area)s_%Y%m%d_%H%M_ct',
                  'ctth_composite_filename': 'mesan_composite_%(area)s_%Y%m%d_%H%M_ctth',
                  'cloudamount_filename': 'mesan_composite_%(area)s_%Y%m%d_%H%M_clamount',
                  'cloudheight_filename': 'mesan_composite_%(area)s_%Y%m%d_%H%M_clheight',
                  'pps_filename': _PATTERN,
                  'msg_satellites': 'Meteosat-11 Meteosat-10 Meteosat-9 Meteosat-8',
                  'msg_cty_filename': 'SAFNWC_%(satellite)s_CT___%Y%m%d%H%M_%(area)s.PLAX.CTTH.0.h5',
                  'msg_cty_file_ext': 'PLAX.CTTH.0.h5',
                  'msg_ctth_filename': 'SAFNWC_%(satellite)s_CTTH_%Y%m%d%H%M_%(area)s.PLAX.CTTH.0.h5',
                  'msg_ctth_file_ext': 'PLAX.CTTH.0.h5',
                  'cloud_amount_ipar': 71,
                  'number_of_pixels': 24,
                  'absolute_time_threshold_minutes': 35,
                  'mesan_area_id': 'mesanEx',
                  'polar_satellites': 'NOAA-20 Metop-C Metop-B NOAA-19 Metop-A NOAA-18 NOAA-15 Suomi-NPP EOS-Aqua',
                  'min_num_of_pps_dr_files': 10,
                  'composite_output_dir': '/home/a000680/data/mesan/output',
                  'pps_direct_readout_dir': '/home/a000680/data/mesan/satin/pps',
                  'pps_metop_gds_dir': '/home/a000680/data/mesan/satin',
                  'msg_dir': '/home/a000680/data/mesan/satin/msg',
                  'msg_areaname': 'MSG-N'}

FAKE_PPS_FILES = ['/tmp/pps1.nc',
                  '/tmp/pps2.nc',
                  '/tmp/pps3.nc',
                  '/tmp/pps4.nc',
                  '/tmp/pps5.nc',
                  '/tmp/pps6.nc',
                  '/tmp/pps7.nc',
                  '/tmp/pps8.nc',
                  '/tmp/pps9.nc',
                  '/tmp/pps10.nc',
                  '/tmp/pps11.nc']


class TestctCompositor:
    """Test the ctCompositor class."""

    def setup_method(self):
        """Set up the tests."""
        filename = '/tmp/my_pps_testfile.nc'
        geofilename = '/tmp/my_pps_geo_testfile.nc'
        platform_name = 'Metop-B'
        orbit = "37011"
        timeslot1 = datetime(2019, 11, 5, 19, 23, 32, 550000)
        variant = None
        pps_metadata_obj = PpsMetaData(filename, geofilename, platform_name, orbit, timeslot1, variant)
        self.ppsdr = [pps_metadata_obj]

        filename = '/tmp/my_msg_testfile.nc'
        platform_name = 'Meteosat-11'
        timeslot = datetime(2019, 11, 5, 19, 0)
        areaid = 'MSG-N'
        msg_meta_obj1 = GeoMetaData(filename, platform_name, areaid, timeslot)
        filename = '/tmp/my_msg_testfile.nc'
        platform_name = 'Meteosat-11'
        timeslot = datetime(2019, 11, 5, 19, 15)
        areaid = 'MSG-N'
        msg_meta_obj2 = GeoMetaData(filename, platform_name, areaid, timeslot)
        filename = '/tmp/my_msg_testfile.nc'
        platform_name = 'Meteosat-11'
        timeslot = datetime(2019, 11, 5, 19, 30)
        areaid = 'MSG-N'
        msg_meta_obj3 = GeoMetaData(filename, platform_name, areaid, timeslot)

        self.msg_scenes = [msg_meta_obj1, msg_meta_obj2, msg_meta_obj3]

    @patch('mesan_compositer.make_ct_composite.ctCompositer._get_all_geo_files')
    @patch('mesan_compositer.make_ct_composite.ctCompositer._get_all_pps_files')
    def test_make_composite(self, get_all_pps, get_all_geo):
        """Test make a cloudtype composite of the list of pps&Geo scenes."""
        get_all_pps.return_value = FAKE_PPS_FILES
        get_all_geo.return_value = []

        t_analysis = datetime(2019, 11, 5, 19, 0)
        delta_t = timedelta(seconds=2100)
        areaid = 'mesanEx'

        with patch.object(ctCompositer, 'get_pps_scenes') as get_pps_scenes:
            ctcomp = ctCompositer(t_analysis, delta_t, areaid, CONFIG_OPTIONS)
            ctcomp.get_catalogue()
            get_pps_scenes.assert_called()

            ctcomp.pps_scenes = self.ppsdr
            ctcomp.msg_scenes = self.msg_scenes

            # TODO: Here we should try test the generation of the composite
