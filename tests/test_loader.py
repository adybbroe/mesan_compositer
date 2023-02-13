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

"""Test loading the NWCSAF scenes."""

from satpy.tests.reader_tests.test_nwcsaf_nc import create_nwcsaf_geo_ct_file
import pytest

from mesan_compositer.load_cloud_products import GeoCloudProductsLoader
from pyresample import parse_area_file
# from satpy.readers.nwcsaf_nc import NcNWCSAF, read_nwcsaf_time


AREA_YAML_DEF = """euro4:
  description: Euro 4km area - Europe
  projection:
    ellps: bessel
    lon_0: 14
    proj: stere
    lat_ts: 60
    lat_0: 90
  shape:
    height: 1024
    width: 1024
  area_extent:
    lower_left_xy: [-2717181.7304994687, -5571048.14031214]
    upper_right_xy: [1378818.2695005313, -1475048.1403121399]
    units: m
"""

TEST_AREADEF = parse_area_file(AREA_YAML_DEF, 'euro4')[0]


@pytest.fixture(scope="session")
def nwcsaf_geo_ct_filename(tmp_path_factory):
    """Create a NWCSAF/Geo CT file and return the filename."""
    return create_nwcsaf_geo_ct_file(tmp_path_factory.mktemp("data"))


def test_geo_clouds_loader_init(nwcsaf_geo_ct_filename):
    """Test create the geo cloud product loader instance from list of fake files."""
    scn = GeoCloudProductsLoader([nwcsaf_geo_ct_filename])
    assert scn.scene is None
    assert len(scn._cloud_files) == 1
    assert scn._cloud_files[0].name == nwcsaf_geo_ct_filename.name


def test_geo_clouds_loader_load(nwcsaf_geo_ct_filename):
    """Test create the geo cloud product loader instance from list of fake files."""
    scn = GeoCloudProductsLoader([nwcsaf_geo_ct_filename])
    scn.load()
    dset_names = scn.scene.available_dataset_names()
    for pname in scn._composites_and_datasets_to_load:
        assert pname in dset_names
