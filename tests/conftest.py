#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023 - 2026 Adam.Dybbroe

# Author(s):

#   Adam Dybbroe <Firstname.Lastname at smhi.se>

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

from __future__ import annotations

import datetime as dt
import os
from itertools import count
from queue import Queue
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from mesan_compositer import mesan_composite_runner as runner

TEST_YAML_CONFIG_CONTENT = """
ct_composite_filename: mesan_composite_{area}_{obstime:%Y%m%d_%H%M}_ct
ctth_composite_filename: mesan_composite_{area}_{obstime:%Y%m%d_%H%M}_ctth

cloudamount_filename: mesan_composite_{area}_{obstime:%Y%m%d_%H%M}_clamount
cloudheight_filename: mesan_composite_{area}_{obstime:%Y%m%d_%H%M}_clheight

cloud_amount_ipar: 71
number_of_pixels: 24
absolute_time_threshold_minutes: 35
mesan_area_id: mesanEx

composite_output_dir: /path/to/cloud/composites/output

generate_superobservations_live_runner:
  cloudtype:
    name: CT
    generate: true

  ctth:
    name: CTTH
    generate: true

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
def fake_msg_dir_path(tmp_path):
    """Write fake msg_dir path directive."""
    file_path = tmp_path / "{product:s}"
    return file_path


@pytest.fixture
def fake_yamlconfig_file(tmp_path):
    """Write fake yaml config file."""
    file_path = tmp_path / "test_mesan_sat_config.yaml"
    with open(file_path, "w") as fpt:
        fpt.write(TEST_YAML_CONFIG_CONTENT)
    yield file_path
    os.remove(file_path)


@pytest.fixture
def fake_yamlconfig_file_with_msg_dir(tmp_path, fake_msg_dir_path):
    """Write fake yaml config file - with a realistic msg_dir directive."""
    file_path = tmp_path / "test_mesan_sat_config_with_msg_dir.yaml"
    with open(file_path, "w") as fpt:
        fpt.write(TEST_YAML_CONFIG_CONTENT)
        fpt.write("msg_dir: " + str(fake_msg_dir_path))

    yield file_path
    os.remove(file_path)


PPS_CTYPES = ["S_NWC_CT_npp_18920_20150623T0030123Z_20150623T0044251Z.nc",
              "S_NWC_CT_noaa19_32830_20150622T2354597Z_20150623T0008100Z.nc",
              "S_NWC_CT_noaa20_00001_20150622T2324597Z_20150622T2338100Z.nc"]

MSG_CTYPES_OLD = ["SAFNWC_MSG2_CT___201206251130_EuropeCanary.PLAX.CTTH.0.h5",
                  "SAFNWC_MSG2_CT___201206251200_EuropeCanary.PLAX.CTTH.0.h5",
                  "SAFNWC_MSG2_CT___201206251215_EuropeCanary.PLAX.CTTH.0.h5",
                  "SAFNWC_MSG2_CT___201206251230_EuropeCanary.PLAX.CTTH.0.h5",
                  "SAFNWC_MSG2_CT___201206251145_EuropeCanary.PLAX.CTTH.0.h5"
                  ]

MSG_CTYPES = ["S_NWC_CT_MSG4_MSG-N-VISIR_20230116T101500Z.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T101500Z_PLAX.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T103000Z.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T103000Z_PLAX.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T104500Z.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T104500Z_PLAX.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T110000Z.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T110000Z_PLAX.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T111500Z.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T111500Z_PLAX.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T113000Z.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T113000Z_PLAX.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T114500Z.nc",
              "S_NWC_CT_MSG4_MSG-N-VISIR_20230116T114500Z_PLAX.nc"]


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
    files = _create_empty_nwcsaf_files_fromlist(tmp_path, MSG_CTYPES, product="CT")
    yield files
    for filename in files:
        os.remove(filename)


def _create_empty_nwcsaf_files_fromlist(basedir, filelist, product=None):
    """Create empty NWCSAF cloud product files from list."""
    files = []
    if product:
        (basedir / product).mkdir()
        rootdir = basedir / product
    else:
        rootdir = basedir
    for ctype_name in filelist:
        file_path = rootdir / ctype_name
        file_path.touch()
        files.append(file_path)
    return files


@pytest.fixture
def cloud_type_netcdf(tmp_path):
    """Create a small cloud-type NetCDF file for super observation creation tests."""
    shape = (16, 8)

    ct = np.full(shape, 5, dtype=np.uint8)

    # First 8x8 block becomes invalid for cloud amount.
    ct[:8, :] = 0

    lons = np.tile(np.arange(shape[1], dtype=float), (shape[0], 1))
    lats = np.tile(np.arange(shape[0], dtype=float)[:, None], (1, shape[1]))

    ds = xr.Dataset(
        {
            "ct": (
                ("y", "x"),
                ct,
            ),
        },
        coords={
            "lon": (
                ("y", "x"),
                lons,
            ),
            "lat": (
                ("y", "x"),
                lats,
            ),
        },
    )

    filename = tmp_path / "ct.nc"
    ds.to_netcdf(filename)

    return filename


@pytest.fixture
def cloud_top_height_netcdf(tmp_path):
    """Create a small cloud-top-height NetCDF file for super observation creation tests."""
    shape = (24, 8)

    height = np.full(shape, 1000.0, dtype=np.float32)

    # Middle 8x8 block is invalid and should not produce a superob.
    height[8:16, :] = np.nan

    # Last 8x8 block has a different height.
    height[16:24, :] = 3000.0

    lons = np.tile(np.arange(shape[1], dtype=np.float32), (shape[0], 1))
    lats = np.tile(np.arange(shape[0], dtype=np.float32)[:, None], (1, shape[1]))

    ds = xr.Dataset(
        {
            "ctth_alti": (
                ("y", "x"),
                height,
            ),
        },
        coords={
            "lon": (
                ("y", "x"),
                lons,
            ),
            "lat": (
                ("y", "x"),
                lats,
            ),
        },
    )

    filename = tmp_path / "ctth.nc"
    ds.to_netcdf(filename)

    return filename

@pytest.fixture
def geo_ct_message():
    """Return a representative GEO CT file message."""
    start_time = dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc)
    return SimpleNamespace(
        type="file",
        data={
            "platform_name": "Meteosat-10",
            "sensor": ["seviri"],
            "start_time": start_time,
            "end_time": None,
            "uri": "/CT/S_NWC_CT_MSG3_MSG-N-VISIR_20260803T220000Z_PLAX.nc",
            "uid": "S_NWC_CT_MSG3_MSG-N-VISIR_20260803T220000Z_PLAX.nc",
            "pge": "CT",
        },
    )


@pytest.fixture
def geo_ctth_message():
    """Return a representative GEO CTTH file message."""
    start_time = dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc)
    return SimpleNamespace(
        type="file",
        data={
            "platform_name": "Meteosat-10",
            "sensor": ["seviri"],
            "start_time": start_time,
            "end_time": None,
            "uri": "/CTTH/S_NWC_CTTH_MSG3_MSG-N-VISIR_20260803T220000Z_PLAX.nc",
            "uid": "S_NWC_CTTH_MSG3_MSG-N-VISIR_20260803T220000Z_PLAX.nc",
            "pge": "CTTH",
        },
    )


@pytest.fixture
def polar_ct_message():
    """Return a representative polar CT file message."""
    start_time = dt.datetime(2026, 8, 3, 22, 0, tzinfo=dt.timezone.utc)
    return SimpleNamespace(
        type="file",
        data={
            "platform_name": "NOAA-20",
            "sensor": ["viirs"],
            "start_time": start_time,
            "end_time": start_time + dt.timedelta(minutes=8),
            "orbit_number": 12345,
            "uri": "/CT/S_NWC_CT_NOAA20_12345_20260803T220000Z.nc",
            "uid": "S_NWC_CT_NOAA20_12345_20260803T220000Z.nc",
            "pge": "CT",
        },
    )


@pytest.fixture
def runner_state():
    """Return minimal mutable state for process_message/run_message_loop tests."""
    return runner.RunnerState(
        pool=None,
        publisher_q=Queue(),
        completion_q=Queue(),
        composite_files={},
        jobs_dict={},
        pending_jobs={},
        token_counter=count(1),
        config_options={},
    )
