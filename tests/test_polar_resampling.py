#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2026 Adam.Dybbroe

"""Integration tests for resampling polar NWCSAF/PPS cloud products."""

from pathlib import Path

import pytest
from pyresample import AreaDefinition

from mesan_compositer.load_cloud_products import CloudProductsLoader

DATA_DIR = Path(__file__).parent / "data"
POLAR_CTTH_FILE = DATA_DIR / "S_NWC_CTTH_noaa21_test_20260315T1045371Z_20260315T1047018Z.nc"

# Small geographic target grid covering the cropped NOAA-21 swath fixture.
TARGET_AREA = AreaDefinition(
    "polar_ctth_test",
    "Polar CTTH regression-test area",
    "latlon",
    {"proj": "latlong", "datum": "WGS84"},
    32,
    24,
    (-36.2, 77.4, -33.3, 78.7),
)


@pytest.mark.system
def test_polar_satz_is_resampled_with_ctth(monkeypatch, tmp_path):
    """Satellite zenith angles must follow CTTH onto the target grid."""
    monkeypatch.setenv("TLES", str(DATA_DIR))

    loader = CloudProductsLoader([POLAR_CTTH_FILE])
    loader.load(["ctth_alti"])
    loader.prepare_satz_angles_on_area("ctth_alti")

    # Pyorbital creates the angle DataArray itself.  Its source area must be
    # carried over from CTTH or Satpy won't know that it needs resampling.
    assert loader.scene["satz"].attrs["area"] == loader.scene["ctth_alti"].attrs["area"]

    resampled = loader.scene.resample(
        TARGET_AREA,
        radius_of_influence=6000,
        reduce_data=False,
        cache_dir=str(tmp_path),
        mask_area=True,
    )

    assert resampled["ctth_alti"].shape == TARGET_AREA.shape
    assert resampled["satz"].shape == TARGET_AREA.shape
