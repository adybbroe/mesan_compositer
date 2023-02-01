#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>

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

"""Fixtures for unittests."""

import pytest
import os

TEST_YAML_CONFIG_CONTENT = """
ct_composite_filename: mesan_composite_%(area)s_%Y%m%d_%H%M_ct
ctth_composite_filename: mesan_composite_%(area)s_%Y%m%d_%H%M_ctth
cloudamount_filename: mesan_composite_%(area)s_%Y%m%d_%H%M_clamount
cloudheight_filename: mesan_composite_%(area)s_%Y%m%d_%H%M_clheight

cloud_amount_ipar: 71
number_of_pixels: 24
absolute_time_threshold_minutes: 35
mesan_area_id: mesanEx

composite_output_dir: /path/to/cloud/composites/output

# Example: S_NWC_CT_metopb_14320_20150622T1642261Z_20150622T1654354Z.nc
pps_filename: "S_NWC_{product:s}_{platform_name:s}_{orbit:05d}_{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z.nc"

polar_satellites:
  - NOAA-20
  - Metop-C
  - Metop-B
  - NOAA-19
  - Metop-A
  - NOAA-18
  - NOAA-15
  - Suomi-NPP
  - EOS-Aqua

min_num_of_pps_dr_files: 10

pps_direct_readout_dir: /path/to/nwcsaf/pps/cloud/products

msg_satellites:
  - Meteosat-11
  - Meteosat-10
  - Meteosat-9
  - Meteosat-8

msg_dir: /path/to/nwcsaf/geo/cloud/products

# Meteosat area name for the NWCSAF Geo products
msg_areaname: MSG-N

# S_NWC_CT_MSG4_MSG-N-VISIR_20230118T103000Z_PLAX.nc
msg_cty_filename: "S_NWC_CT_{satellite:s}_{area:s}-VISIR_{nominal_time:%Y%m%dT%H%M%SZ}_PLAX.nc"
# SAFNWC_MSG4_CT___202301161145_MSG-N_______.PLAX.CTTH.0.h5
#msg_cty_filename:  "SAFNWC_{satellite:s}_CT___{nominal_time:%Y%m%d%H%M}_{area:s}_______.PLAX.CTTH.0.h5"
msg_ctth_filename: "SAFNWC_{satellite:s}_CTTH_{nominal_time:%Y%m%d%H%M}_{area:s}_______.PLAX.CTTH.0.h5"

"""  # noqa


@pytest.fixture
def fake_yamlconfig_file(tmp_path):
    """Write fake yaml config file."""
    file_path = tmp_path / 'test_mesan_sat_config.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_YAML_CONFIG_CONTENT)
    yield file_path
    os.remove(file_path)


PPS_CTYPES = ['S_NWC_CT_npp_18920_20150623T0030123Z_20150623T0044251Z.nc',
              'S_NWC_CT_noaa19_32830_20150622T2354597Z_20150623T0008100Z.nc',
              'S_NWC_CT_noaa20_00001_20150622T2324597Z_20150622T2338100Z.nc']

MSG_CTYPES_OLD = ['SAFNWC_MSG2_CT___201206251130_EuropeCanary.PLAX.CTTH.0.h5',
                  'SAFNWC_MSG2_CT___201206251200_EuropeCanary.PLAX.CTTH.0.h5',
                  'SAFNWC_MSG2_CT___201206251215_EuropeCanary.PLAX.CTTH.0.h5',
                  'SAFNWC_MSG2_CT___201206251230_EuropeCanary.PLAX.CTTH.0.h5',
                  'SAFNWC_MSG2_CT___201206251145_EuropeCanary.PLAX.CTTH.0.h5'
                  ]

MSG_CTYPES = ['S_NWC_CT_MSG4_MSG-N-VISIR_20230116T101500Z.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T101500Z_PLAX.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T103000Z.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T103000Z_PLAX.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T104500Z.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T104500Z_PLAX.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T110000Z.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T110000Z_PLAX.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T111500Z.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T111500Z_PLAX.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T113000Z.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T113000Z_PLAX.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T114500Z.nc',
              'S_NWC_CT_MSG4_MSG-N-VISIR_20230116T114500Z_PLAX.nc']


@pytest.fixture
def fake_empty_nwcsaf_pps_files(tmp_path):
    """Create a list of empty fake nwcsaf/pps files."""
    files = _create_empty_nwcsaf_files_fromlist(tmp_path, PPS_CTYPES)
    yield files
    for pps_file in files:
        os.remove(pps_file)


@pytest.fixture
def fake_empty_old_nwcsaf_geo_files(tmp_path):
    """Create a list of empty fake nwcsaf/geo files."""
    files = _create_empty_nwcsaf_files_fromlist(tmp_path, MSG_CTYPES_OLD)
    yield files
    for filename in files:
        os.remove(filename)


@pytest.fixture
def fake_empty_nwcsaf_geo_files(tmp_path):
    """Create a list of empty fake nwcsaf/geo files."""
    files = _create_empty_nwcsaf_files_fromlist(tmp_path, MSG_CTYPES)
    yield files
    for filename in files:
        os.remove(filename)


def _create_empty_nwcsaf_files_fromlist(basedir, filelist):
    """Create empty NWCSAF cloud product files from list."""
    files = []
    for ctype_name in filelist:
        file_path = basedir / ctype_name
        file_path.touch()
        files.append(file_path)
    return files
