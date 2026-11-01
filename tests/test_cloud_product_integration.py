#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2026 Adam.Dybbroe

"""Integration tests for NWCSAF/PPS cloud-product resampling and blending."""

from pathlib import Path

import pytest
import xarray as xr
from pyresample import AreaDefinition

from mesan_compositer.load_cloud_products import CloudProductsLoader, blend_cloud_products

DATA_DIR = Path(__file__).parent / "data"
POLAR_CTTH_FILE = DATA_DIR / "S_NWC_CTTH_noaa21_test_20260315T1045371Z_20260315T1047018Z.nc"
POLAR_CMA_FILE = DATA_DIR / "S_NWC_CMA_noaa21_test_20260315T1045371Z_20260315T1047018Z.nc"
POLAR_CTTH_FILE_2 = DATA_DIR / "S_NWC_CTTH_noaa21_test_20260315T1044112Z_20260315T1045358Z.nc"
POLAR_CMA_FILE_2 = DATA_DIR / "S_NWC_CMA_noaa21_test_20260315T1044112Z_20260315T1045358Z.nc"
POLAR_CBH_FILE_2 = DATA_DIR / "S_NWC_CBH_noaa21_test_20260315T1044112Z_20260315T1045358Z.nc"
GEO_CTTH_FILE = DATA_DIR / "S_NWC_CTTH_meteosat12_test_20260315T1100070Z_20260315T1109230Z.nc"
GEO_CTTH_LEGACY_FILE = (
    DATA_DIR / "S_NWC_CTTH_meteosat12_legacy_test_20260315T1100070Z_20260315T1109230Z.nc"
)

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

# Small fixed-grid area matching the cropped remapped Meteosat fixture.
GEO_TARGET_AREA = AreaDefinition(
    "geo_ctth_test",
    "Remapped Meteosat CTTH regression-test area",
    "lcc",
    {
        "proj": "lcc",
        "lat_0": 63.3,
        "lon_0": 15.0,
        "lat_1": 63.3,
        "lat_2": 63.3,
        "ellps": "WGS84",
        "units": "m",
    },
    32,
    32,
    (-550552.016863395, 1211528.311377246, -422659.942664404, 1339432.502994012),
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


@pytest.mark.system
def test_single_polar_ctth_scene_resamples(monkeypatch, tmp_path):
    """A single polar CTTH+CMA scene must use the non-MultiScene path."""
    monkeypatch.setenv("TLES", str(DATA_DIR))

    scene = [POLAR_CTTH_FILE, POLAR_CMA_FILE]

    resampled, dataset_name = blend_cloud_products("CTTH", [], TARGET_AREA, scene, cache_dir=str(tmp_path))

    assert dataset_name == "ctth_alti"
    assert resampled[dataset_name].shape == TARGET_AREA.shape
    assert resampled["ctth_alti_err"].shape == TARGET_AREA.shape
    assert resampled[dataset_name].attrs["ancillary_variables"][0].attrs["name"] == "ctth_alti_err"
    assert resampled["satz"].shape == TARGET_AREA.shape


@pytest.mark.system
def test_single_polar_cbh_scene_propagates_altitude_error(monkeypatch, tmp_path):
    """A polar CBH composite must retain its altitude standard-error field."""
    monkeypatch.setenv("TLES", str(DATA_DIR))

    resampled, dataset_name = blend_cloud_products("CBH", [], TARGET_AREA,
                                                   [POLAR_CBH_FILE_2, POLAR_CMA_FILE_2], cache_dir=str(tmp_path))

    assert dataset_name == "cbh_alti"
    assert resampled[dataset_name].shape == TARGET_AREA.shape
    assert resampled["cbh_alti_err"].shape == TARGET_AREA.shape
    assert resampled[dataset_name].attrs["ancillary_variables"][0].attrs["name"] == "cbh_alti_err"


@pytest.mark.system
def test_two_polar_ctth_scenes_blend_on_target_area(monkeypatch, tmp_path):
    """Multiple polar scenes must resample CTTH and weights to one grid."""
    monkeypatch.setenv("TLES", str(DATA_DIR))

    scene_1 = [POLAR_CTTH_FILE, POLAR_CMA_FILE]
    scene_2 = [POLAR_CTTH_FILE_2, POLAR_CMA_FILE_2]

    blended, group_name = blend_cloud_products("CTTH", [], TARGET_AREA,
                                               scene_1, scene_2, cache_dir=str(tmp_path), )

    assert group_name == "CTTH_ALTI_group"
    assert blended[group_name].shape == TARGET_AREA.shape
    assert blended["CTTH_ALTI_ERR_group"].shape == TARGET_AREA.shape
    assert blended[group_name].attrs["ancillary_variables"][0].attrs["name"] == "CTTH_ALTI_ERR_group"


@pytest.mark.system
def test_single_geo_ctth_scene_resamples(monkeypatch, tmp_path):
    """A single remapped Meteosat CTTH scene must use the fixed-grid path."""
    monkeypatch.setattr(
        "mesan_compositer.load_cloud_products.get_area_def",
        lambda _area_id: GEO_TARGET_AREA,
    )

    resampled, dataset_name = blend_cloud_products("CTTH", [], GEO_TARGET_AREA,
                                                   [GEO_CTTH_FILE], cache_dir=str(tmp_path), )

    assert dataset_name == "ctth_alti"
    assert resampled[dataset_name].shape == GEO_TARGET_AREA.shape
    assert resampled["ctth_alti_err"].shape == GEO_TARGET_AREA.shape
    assert resampled[dataset_name].attrs["ancillary_variables"][0].attrs["name"] == "ctth_alti_err"
    assert resampled["satz"].shape == GEO_TARGET_AREA.shape


@pytest.mark.system
def test_mixed_geo_polar_ctth_propagates_common_error(monkeypatch, tmp_path):
    """Mixed Geo/polar scenes should propagate a common CTTH error field."""
    monkeypatch.setenv("TLES", str(DATA_DIR))
    monkeypatch.setattr(
        "mesan_compositer.load_cloud_products.get_area_def",
        lambda _area_id: GEO_TARGET_AREA,
    )

    geo_scene = [GEO_CTTH_FILE]
    polar_scene = [POLAR_CTTH_FILE, POLAR_CMA_FILE]

    blended, group_name = blend_cloud_products("CTTH", [], GEO_TARGET_AREA,
                                               geo_scene, polar_scene, cache_dir=str(tmp_path))

    assert group_name == "CTTH_ALTI_group"
    assert blended[group_name].shape == GEO_TARGET_AREA.shape
    assert blended["CTTH_ALTI_ERR_group"].shape == GEO_TARGET_AREA.shape
    assert blended[group_name].attrs["ancillary_variables"][0].attrs["name"] == "CTTH_ALTI_ERR_group"


@pytest.mark.system
def test_mixed_geo_polar_ctth_skips_error_if_not_available_for_all(monkeypatch, tmp_path):
    """Mixed legacy/new scenes must blend CTTH even if only one has an error field."""
    monkeypatch.setenv("TLES", str(DATA_DIR))
    monkeypatch.setattr(
        "mesan_compositer.load_cloud_products.get_area_def",
        lambda _area_id: GEO_TARGET_AREA,
    )

    geo_scene = [GEO_CTTH_LEGACY_FILE]
    polar_scene = [POLAR_CTTH_FILE, POLAR_CMA_FILE]

    blended, group_name = blend_cloud_products("CTTH", [], GEO_TARGET_AREA,
                                               geo_scene, polar_scene, cache_dir=str(tmp_path))

    assert group_name == "CTTH_ALTI_group"
    assert blended[group_name].shape == GEO_TARGET_AREA.shape
    assert "CTTH_ALTI_ERR_group" not in blended
    assert "ancillary_variables" not in blended[group_name].attrs



@pytest.mark.system
def test_single_polar_ctth_scene_writes_ancillary_error(monkeypatch, tmp_path):
    """A CTTH composite with an error field must be writable as CF NetCDF."""
    monkeypatch.setenv("TLES", str(DATA_DIR))

    scene = [POLAR_CTTH_FILE, POLAR_CMA_FILE]

    resampled, dataset_name = blend_cloud_products("CTTH", [], TARGET_AREA, scene, cache_dir=str(tmp_path))

    output_file = tmp_path / "ctth_composite.nc"

    resampled.save_datasets(datasets=[dataset_name, "ctth_alti_err"], filename=str(output_file), writer="cf")

    assert output_file.exists()

    with xr.open_dataset(output_file) as ds:
        assert "ctth_alti" in ds
        assert "ctth_alti_err" in ds
        assert ds["ctth_alti"].attrs["ancillary_variables"] == "ctth_alti_err"
