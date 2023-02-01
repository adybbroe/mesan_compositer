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

"""Test reading and extraction of config variables."""

from mesan_compositer.config import get_config


def test_get_yaml_configuration(fake_yamlconfig_file):
    """Test read and get the yaml configuration from file."""
    config = get_config(fake_yamlconfig_file)

    assert config['ct_composite_filename'] == 'mesan_composite_%(area)s_%Y%m%d_%H%M_ct'
    assert config['ctth_composite_filename'] == 'mesan_composite_%(area)s_%Y%m%d_%H%M_ctth'
    assert config['cloudamount_filename'] == 'mesan_composite_%(area)s_%Y%m%d_%H%M_clamount'
    assert config['cloudheight_filename'] == 'mesan_composite_%(area)s_%Y%m%d_%H%M_clheight'
    assert config['pps_filename'] == 'S_NWC_{product:s}_{platform_name:s}_{orbit:05d}_{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z.nc'  # noqa
    assert config['msg_satellites'] == ['Meteosat-11', 'Meteosat-10', 'Meteosat-9', 'Meteosat-8']
    assert config['polar_satellites'] == ['NOAA-20', 'Metop-C', 'Metop-B',
                                          'NOAA-19', 'Metop-A', 'NOAA-18',
                                          'NOAA-15', 'Suomi-NPP', 'EOS-Aqua']
    assert config['min_num_of_pps_dr_files'] == 10

    assert config['msg_dir'] == '/path/to/nwcsaf/geo/cloud/products'
    assert config['composite_output_dir'] == '/path/to/cloud/composites/output'
    assert config['pps_direct_readout_dir'] == '/path/to/nwcsaf/pps/cloud/products'
