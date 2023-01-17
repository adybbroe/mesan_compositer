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


TEST_YAML_CONFIG_CONTENT = """
ct_composite_filename: mesan_composite_%(area)s_%Y%m%d_%H%M_ct
ctth_composite_filename: mesan_composite_%(area)s_%Y%m%d_%H%M_ctth
cloudamount_filename: mesan_composite_%(area)s_%Y%m%d_%H%M_clamount
cloudheight_filename: mesan_composite_%(area)s_%Y%m%d_%H%M_clheight

# Example: S_NWC_CT_metopb_14320_20150622T1642261Z_20150622T1654354Z.nc
pps_filename: "S_NWC_{product:s}_{platform_name:s}_{orbit:05d}_{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z.nc"

msg_satellites:
  - Meteosat-11
  - Meteosat-10
  - Meteosat-9
  - Meteosat-8

msg_cty_filename: "SAFNWC_%(satellite)s_CT___%Y%m%d%H%M_%(area)s.PLAX.CTTH.0.h5"
msg_cty_file_ext: PLAX.CTTH.0.h5
msg_ctth_filename: "SAFNWC_%(satellite)s_CTTH_%Y%m%d%H%M_%(area)s.PLAX.CTTH.0.h5"
msg_ctth_file_ext: PLAX.CTTH.0.h5

cloud_amount_ipar: 71
number_of_pixels: 24
absolute_time_threshold_minutes: 35
mesan_area_id: mesanEx


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

composite_output_dir: /path/to/cloud/composites/output

pps_direct_readout_dir: /path/to/nwcsaf/pps/cloud/products


msg_dir: /path/to/nwcsaf/geo/cloud/products
hrit_path: /path/to/seviri/hrit/files
# Meteosat area name for the NWCSAF Geo products
msg_areaname: MSG-N

"""  # noqa


@pytest.fixture
def fake_yamlconfig_file(tmp_path):
    """Write fake yaml config file."""
    file_path = tmp_path / 'test_mesan_sat_config.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_YAML_CONFIG_CONTENT)

    yield file_path
