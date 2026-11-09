#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023, 2026 Adam.Dybbroe

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

import numpy as np
import pytest
import xarray as xr
from pyresample import parse_area_file
from satpy.tests.reader_tests.test_nwcsaf_nc import create_nwcsaf_geo_ct_file

from mesan_compositer.load_cloud_products import (
    CLOUD_FREE_SENTINEL,
    OBSERVATION_VALIDITY,
    CloudProductsLoader,
    _mask_weight_where_no_observation,
    add_observation_validity,
    prepare_ctth_for_resampling,
    restore_cloud_free_height,
)

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

TEST_AREADEF = parse_area_file(AREA_YAML_DEF, "euro4")[0]


@pytest.fixture(scope="session")
def nwcsaf_geo_ct_filename(tmp_path_factory):
    """Create a NWCSAF/Geo CT file and return the filename."""
    return create_nwcsaf_geo_ct_file(tmp_path_factory.mktemp("data"))


def test_geo_clouds_loader_init(nwcsaf_geo_ct_filename):
    """Test create the geo cloud product loader instance from list of fake files."""
    scn = CloudProductsLoader([nwcsaf_geo_ct_filename])
    assert scn.scene is None
    assert len(scn._cloud_files) == 1
    assert scn._cloud_files[0].name == nwcsaf_geo_ct_filename.name


def test_geo_clouds_loader_load(nwcsaf_geo_ct_filename):
    """Test create the geo cloud product loader instance from list of fake files."""
    scn = CloudProductsLoader([nwcsaf_geo_ct_filename])
    scn.load(["ct"])
    dset_names = scn.scene.available_dataset_names()
    assert "ct" in dset_names


def test_prepare_ctth_for_resampling_uses_height_and_status_flag():
    """Use status bit 0 only to classify missing CTTH as cloud-free."""
    scene = {
        "ctth_alti": xr.DataArray(
            [[np.nan, 2050.0, np.nan, 3100.0, 4200.0]], dims=("y", "x")
        ),
        "ctth_status_flag": xr.DataArray(
            [[1.0, 1.0, 255.0, 4.0, 5.0]],
            dims=("y", "x"),
            attrs={"_FillValue": 255.0},
        ),
    }

    assert prepare_ctth_for_resampling(scene)

    expected = xr.DataArray(
        [[CLOUD_FREE_SENTINEL, 2050.0, np.nan, 3100.0, 4200.0]],
        dims=("y", "x"),
    )
    xr.testing.assert_equal(scene["ctth_alti"].drop_attrs(), expected)


def test_prepare_ctth_for_resampling_handles_decoded_status_fill():
    """Decoded NaN status values must remain no-observation pixels."""
    scene = {
        "ctth_alti": xr.DataArray([[np.nan, 2200.0]], dims=("y", "x")),
        "ctth_status_flag": xr.DataArray([[np.nan, 0.0]], dims=("y", "x")),
    }

    assert prepare_ctth_for_resampling(scene)

    expected = xr.DataArray([[np.nan, 2200.0]], dims=("y", "x"))
    xr.testing.assert_equal(scene["ctth_alti"].drop_attrs(), expected)


def test_prepare_ctth_for_resampling_without_status_is_noop():
    """Legacy/minimal inputs without a status flag keep the fallback path available."""
    original = xr.DataArray([[np.nan, 2200.0]], dims=("y", "x"))
    scene = {"ctth_alti": original.copy()}

    assert not prepare_ctth_for_resampling(scene)
    xr.testing.assert_identical(scene["ctth_alti"], original)


def test_restore_cloud_free_height_also_masks_error():
    """A selected cloud-free CTTH pixel must not retain a cloudy altitude error."""
    scene = {
        "ctth_alti": xr.DataArray(
            [[CLOUD_FREE_SENTINEL, 2050.0]], dims=("y", "x")
        ),
        "ctth_alti_err": xr.DataArray([[150.0, 120.0]], dims=("y", "x")),
    }

    restore_cloud_free_height(scene, "ctth_alti", "ctth_alti_err")

    expected_height = xr.DataArray([[np.nan, 2050.0]], dims=("y", "x"))
    expected_error = xr.DataArray([[np.nan, 120.0]], dims=("y", "x"))
    xr.testing.assert_equal(scene["ctth_alti"], expected_height)
    xr.testing.assert_equal(scene["ctth_alti_err"], expected_error)


def test_add_observation_validity_handles_fill_value_and_nan():
    """Represent valid CMA samples as one and all missing samples as NaN."""
    cma = xr.DataArray(
        [[0.0, 1.0, 255.0, np.nan]],
        dims=("y", "x"),
        attrs={"_FillValue": 255.0, "area": "dummy-area"},
    )
    scene = {"cma": cma.copy()}

    add_observation_validity(scene)

    expected = xr.DataArray([[1.0, 1.0, np.nan, np.nan]], dims=("y", "x"))
    xr.testing.assert_equal(scene[OBSERVATION_VALIDITY].drop_attrs(), expected)
    assert scene[OBSERVATION_VALIDITY].attrs["area"] == "dummy-area"
    assert "_FillValue" not in scene[OBSERVATION_VALIDITY].attrs
    xr.testing.assert_identical(scene["cma"], cma)


def test_mask_weight_where_no_observation_uses_resampled_validity():
    """Only target pixels reached by a valid observation should get weight."""
    scene = {
        OBSERVATION_VALIDITY: xr.DataArray(
            [[1.0, np.nan, 1.0]], dims=("y", "x")
        )
    }
    weight = xr.DataArray([[0.5, 0.25, 0.125]], dims=("y", "x"))

    result = _mask_weight_where_no_observation(scene, weight)

    expected = xr.DataArray([[0.5, 0.0, 0.125]], dims=("y", "x"))
    xr.testing.assert_equal(result, expected)


def test_mask_weight_for_prepared_ctth_uses_height_validity():
    """Cloud-free sentinels are selectable while true no-observation CTTH is not."""
    scene = {
        "ctth_alti": xr.DataArray(
            [[CLOUD_FREE_SENTINEL, np.nan, 2050.0]], dims=("y", "x")
        )
    }
    weight = xr.DataArray([[0.5, 0.25, 0.125]], dims=("y", "x"))

    result = _mask_weight_where_no_observation(scene, weight, "ctth_alti")

    expected = xr.DataArray([[0.5, 0.0, 0.125]], dims=("y", "x"))
    xr.testing.assert_equal(result, expected)


def test_mask_weight_without_observation_validity_is_unchanged():
    """Scenes without the internal validity field keep their original weight."""
    weight = xr.DataArray([[0.5, 0.25]], dims=("y", "x"))

    result = _mask_weight_where_no_observation({}, weight)

    xr.testing.assert_identical(result, weight)
