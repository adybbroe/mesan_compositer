#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2026 Adam Dybbroe

# Author(s):

#   Adam Dybbroe <Firstname.Lastname @ smhi.se>

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

"""A system/integration test used to check the handling of nodata."""


import os
from pathlib import Path

import pytest
import xarray as xr

from mesan_compositer.load_cloud_products import (
    CLOUD_FREE_SENTINEL,
    RADIUS_OF_INFLUENCE,
    CloudProductsLoader,
    blend_cloud_products,
)

TARGET_AREA = "meps4km"
REAL_DATA_DIR = os.environ.get("MESAN_TEST_DATA")


@pytest.mark.system
@pytest.mark.skipif(
    REAL_DATA_DIR is None,
    reason="MESAN_TEST_DATA is not set",
)
def test_single_polar_ctth_preserves_cloudfree_during_resampling(monkeypatch, tmp_path):
    """Cloud-free CTTH pixels must take part in nearest-neighbour selection."""
    monkeypatch.setenv("TLES", str(REAL_DATA_DIR))

    POLAR_CTTH_FILE = [*Path(REAL_DATA_DIR).glob("S_NWC_CTTH_noaa21_00000_20260315T1041*Z_20260315T104*Z.nc")][0]
    POLAR_CMA_FILE = [*Path(REAL_DATA_DIR).glob("S_NWC_CMA_noaa21_00000_20260315T1041*Z_20260315T104*Z.nc")][0]

    files = [POLAR_CTTH_FILE, POLAR_CMA_FILE]

    # Build an independent reference from the raw CTTH and status flag.
    # A finite CTTH value is a valid cloud height.  A missing CTTH value is
    # only a valid cloud-free observation when status bit 0 is set.
    loader = CloudProductsLoader(files)
    loader.load(["ctth_alti"], optional=["ctth_status_flag"])
    height = loader.scene["ctth_alti"]
    status = loader.scene["ctth_status_flag"]

    status_fill = status.attrs.get("_FillValue")
    if status_fill is None:
        status_fill = status.encoding.get("_FillValue")
    status_valid = status.notnull()
    if status_fill is not None:
        status_valid = status_valid & (status != status_fill)

    height_valid = height.notnull()
    cloud_free = (~height_valid) & status_valid & ((status.fillna(0) % 2) == 1)

    reference = height.where(height_valid | cloud_free)
    reference = reference.where(~cloud_free, CLOUD_FREE_SENTINEL)
    reference.attrs = height.attrs.copy()
    loader.scene["ctth_alti"] = reference

    expected = loader.scene.resample(
        TARGET_AREA,
        radius_of_influence=RADIUS_OF_INFLUENCE,
        reduce_data=False,
        cache_dir=str(tmp_path / "reference"),
        mask_area=True,
    )
    expected_ctth = expected["ctth_alti"].where(
        expected["ctth_alti"] != CLOUD_FREE_SENTINEL
    ).reset_coords(drop=True)

    actual, dataset_name = blend_cloud_products(
        "CTTH", [], TARGET_AREA, files,
        cache_dir=str(tmp_path / "mesan"),
    )
    actual_ctth = actual[dataset_name].reset_coords(drop=True)

    xr.testing.assert_allclose(actual_ctth, expected_ctth)

    # This test is specifically about behaviour that differs from plain
    # ctth_alti resampling: clear-sky pixels must not be filled from a farther
    # cloudy neighbour.
    plain_loader = CloudProductsLoader(files)
    plain_loader.load(["ctth_alti"])
    plain = plain_loader.scene.resample(
        TARGET_AREA,
        radius_of_influence=RADIUS_OF_INFLUENCE,
        reduce_data=False,
        cache_dir=str(tmp_path / "plain_satpy"),
        mask_area=True,
    )
    differs_at_clear_sky = expected_ctth.isnull() & plain["ctth_alti"].notnull()
    assert differs_at_clear_sky.any().compute().item()
