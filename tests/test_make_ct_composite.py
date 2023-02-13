#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 - 2023 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <Firstname.Lastname @ smhi.se>

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

from datetime import datetime, timedelta
import h5netcdf
import numpy as np
import pytest

from mesan_compositer.make_ct_composite import ctCompositer
from mesan_compositer.composite_tools import PpsMetaData
from mesan_compositer.composite_tools import GeoMetaData
from mesan_compositer.config import get_config

from satpy.tests.reader_tests.test_nwcsaf_nc import create_nwcsaf_geo_ct_file

from unittest.mock import patch


@pytest.fixture
def nwcsaf_geo_ct_filename(tmp_path_factory):
    """Create a CT file and return the filename."""
    return create_nwcsaf_geo_ct_file(tmp_path_factory.mktemp("data"))


PROJ_KM = {'gdal_projection': '+proj=geos +a=6378.137000 +b=6356.752300 +lon_0=0.000000 +h=35785.863000',
           'gdal_xgeo_up_left': -5569500.0,
           'gdal_ygeo_up_left': 5437500.0,
           'gdal_xgeo_low_right': 5566500.0,
           'gdal_ygeo_low_right': 2653500.0}

dimensions = {"nx": 1530,
              "ny": 928,
              "pal_colors_250": 250,
              "pal_rgb": 3}


START_TIME_PPS = "20230118T103917000Z"
END_TIME_PPS = "20230118T104222000Z"

GLOBAL_ATTRIBUTES = {"source": "NWC/PPS version v2021",
                     "platform": "Suomi-NPP",
                     "orbit_number": 0,
                     "time_coverage_start": START_TIME_PPS,
                     "time_coverage_end": END_TIME_PPS}

CT_PALETTE_MEANINGS = ("1 2 3 4 5 6 7 8 9 10 11 12 13 14 15")

CT_ARRAY = np.random.randint(0, 16, size=(928, 1530), dtype=np.uint8)
PAL_ARRAY = np.random.randint(0, 255, size=(250, 3), dtype=np.uint8)


@pytest.fixture
def nwcsaf_pps_ct_filename(tmp_path_factory):
    """Create a NWCSAF/PPS Cloud Type file."""
    attrs = GLOBAL_ATTRIBUTES.copy()
    attrs.update(PROJ_KM)
    attrs["time_coverage_start"] = START_TIME_PPS
    attrs["time_coverage_end"] = END_TIME_PPS
    filename = create_ct_file(tmp_path_factory.mktemp("data"), filetype="ct", attrs=attrs)
    return filename


def create_ct_file(path, filetype, attrs=GLOBAL_ATTRIBUTES):
    """Create a NWCSAF/PPS ct file."""
    filename = path / f"S_NWC_{filetype.upper()}_npp_00000_20230118T1039170Z_20230118T1042220Z.nc"
    with h5netcdf.File(filename, mode="w") as nc_file:
        nc_file.dimensions = dimensions
        nc_file.attrs.update(attrs)
        create_ct_variable(nc_file, f"{filetype}")
        create_ct_pal_variable(nc_file, f"{filetype}_pal")

    return filename


def create_ct_pal_variable(nc_file, var_name):
    """Create a Cloud Type palette variable."""
    var = nc_file.create_variable(var_name, ("pal_colors_250", "pal_rgb"), np.uint8)
    var[:] = PAL_ARRAY
    var.attrs["palette_meanings"] = CT_PALETTE_MEANINGS


def create_ct_variable(nc_file, var_name):
    """Create a CT variable."""
    var = nc_file.create_variable(var_name, ("ny", "nx"), np.uint16, chunks=(256, 256))
    var[:] = CT_ARRAY
    var.attrs["valid_range"] = np.array([1, 15], dtype=np.uint8)


_PATTERN = ('S_NWC_{product:s}_{platform_name:s}_{orbit:05d}_' +
            '{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z.nc')
_MSG_CT_PATTERN = "S_NWC_CT_{satellite:s}_{area:s}-VISIR_{nominal_time:%Y%m%dT%H%M%SZ}_PLAX.nc"

CONFIG_OPTIONS = {'ct_composite_filename': 'mesan_composite_%(area)s_%Y%m%d_%H%M_ct',
                  'ctth_composite_filename': 'mesan_composite_%(area)s_%Y%m%d_%H%M_ctth',
                  'cloudamount_filename': 'mesan_composite_%(area)s_%Y%m%d_%H%M_clamount',
                  'cloudheight_filename': 'mesan_composite_%(area)s_%Y%m%d_%H%M_clheight',
                  'pps_filename': _PATTERN,
                  'msg_satellites': 'Meteosat-11 Meteosat-10 Meteosat-9 Meteosat-8',
                  # 'msg_cty_filename': 'SAFNWC_%(satellite)s_CT___%Y%m%d%H%M_%(area)s.PLAX.CTTH.0.h5',
                  'msg_cty_filename': _MSG_CT_PATTERN,
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


def test_setup_ct_compositer(fake_yamlconfig_file,
                             nwcsaf_geo_ct_filename,
                             nwcsaf_pps_ct_filename):
    """Test set up the CT compositer."""
    config = get_config(fake_yamlconfig_file)

    area_id = 'mesanEx'
    time_of_analysis = datetime.strptime('2023011811', '%Y%m%d%H')
    delta_time_window = timedelta(minutes=35)

    config['msg_dir'] = str(nwcsaf_geo_ct_filename.parent)
    config['pps_direct_readout_dir'] = str(nwcsaf_pps_ct_filename.parent)
    ctcomp = ctCompositer(time_of_analysis, delta_time_window, area_id, config)
    ctcomp.get_catalogue()

    assert len(ctcomp.pps_scenes) == 1
    assert len(ctcomp.msg_scenes) == 1
    expected_msg_scene = ['filename={filepath}'.format(filepath=str(nwcsaf_geo_ct_filename)),
                          'platform_name=Meteosat-11',
                          'areaid=MSG-N',
                          'timeslot=2023-01-18 10:30:00']
    assert str(ctcomp.msg_scenes[0]).split('\n') == expected_msg_scene

    geofilename = str(nwcsaf_pps_ct_filename).replace('_CT_', '_CMA_')
    expected_pps_scene = ['filename={filepath}'.format(filepath=str(nwcsaf_pps_ct_filename)),
                          'geofilename={filepath}'.format(filepath=geofilename),
                          'platform_name=Suomi-NPP',
                          'orbit=00000',
                          'timeslot=2023-01-18 10:40:49.500000',
                          'variant=None']
    assert str(ctcomp.pps_scenes[0]).split('\n') == expected_pps_scene
