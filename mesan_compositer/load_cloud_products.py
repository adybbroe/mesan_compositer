#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2026 Adam Dybbroe

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

"""Utilities to load and prepare cloud products on area."""

import logging
import os
import pathlib
from datetime import timedelta
from tempfile import gettempdir

import dask.array as da
import numpy as np
import xarray as xr
from pyorbital.orbital import Orbital
from satpy import DataQuery, MultiScene, Scene
from satpy import config as satpy_config
from satpy.area import get_area_def
from satpy.modifiers.angles import get_satellite_zenith_angle
from satpy.multiscene.blend_funcs import stack
from satpy.utils import debug_on
from trollsift import Parser, globify

debug_on()

LOG = logging.getLogger(__name__)


HEIGHT_FILL_VALUE = 65535.0
HEIGHT_PRODUCTS = {"ctth_alti", "cbh_alti"}

HOME = os.environ.get("HOME")

class CloudProductsLoader:
    """Class to load and prepare cloud products on area."""

    def __init__(self, cloud_files):
        """Set up the instance."""
        self._cloud_files = cloud_files
        self.scene = None
        if len(self._cloud_files) > 0 and "meteosat12" in [f.name for f in self._cloud_files][0]:
            self._reader = "nwcsaf-pps_nc-remapped"
        else:
            self._reader = "nwcsaf-pps_nc"

        if len(self._cloud_files) > 0 and "MSG" in str(self._cloud_files[0].name):
            self._reader = "nwcsaf-geo"

    def load(self, to_load):
        """Load the cloud products."""
        self.scene = Scene({self._reader: self._cloud_files})
        self.scene.load(to_load)
        # Drop the time dimension if it is there:
        for key in self.scene.keys():
            try:
                if "time" in self.scene[key].dims:
                    self.scene[key] = self.scene[key].squeeze("time", drop=True)
            except Exception as e:
                LOG.exception(e)
                LOG.debug("Did not manage to drop time dimension.")

        # GEO and PPS cloud top heights are not scaled the same!
        # Convert Geo cloud top height to PPS cloud top height?
        # FIXME!
        hist = None
        # this = self.scene["ctth_alti"].data.compute()
        # hist = np.histogram(np.nan_to_num(this, nan=27000), bins=29)
        if "ctth_alti" in to_load and self._reader == "nwcsaf-geo":
            LOG.debug("Trying to load 'ctth_alti' with the reader 'nwcsaf-geo'")

        return hist

    def prepare_satz_angles_on_area(self, product, areaid=None):
        """Derive satellite zenith angles and attach them to the scene."""
        if self._reader == "nwcsaf-pps_nc-remapped":
            if areaid is None:
                raise ValueError("areaid required for remapped PPS data")
            self.scene[product].attrs["area"] = get_area_def(areaid)

        satz = self._get_satz_angles(product)

        # The Pyorbital fallback doesn't attach a Satpy area definition.
        # Copy the source product area so that satz is resampled together
        # with the product.
        if "area" not in satz.attrs and "area" in self.scene[product].attrs:
            satz.attrs["area"] = self.scene[product].attrs["area"]

        self.scene["satz"] = satz

    def _get_satz_angles(self, product):
        """Calculate the satellite zenith angles using Pyorbital."""
        data_array = self.scene[product]
        if "time" in data_array.dims:
            data_array = data_array.squeeze("time", drop=True)

        try:
            return get_satellite_zenith_angle(data_array)
        except KeyError:
            with satpy_config.set(cache_sensor_angles=True):
                return compute_satz_with_pyorbital(data_array)


def get_tle_file(start_time, end_time):
    """Get the TLE file with TLEs that are closest in time to the actual observation time."""
    tles_env = os.getenv("TLES")
    if tles_env is None:
        LOG.warning("No valid environment set for offline TLEs!")
        return None
    tles_path = pathlib.Path(tles_env)

    dtobj = start_time + (end_time - start_time)/2
    TLE_FILE_PATTERN = "tle-{datetime:%Y%m%d%H%M}.txt"

    tle_filepaths = tles_path.glob(globify(TLE_FILE_PATTERN))
    p__ = Parser(TLE_FILE_PATTERN)
    tdelta = timedelta(days=30)
    found_tlefile = None
    for tle_path in tle_filepaths:
        res = p__.parse(tle_path.name)

        if abs(res["datetime"] - dtobj) < tdelta:
            tdelta = abs(res["datetime"] - dtobj)
            found_tlefile = tle_path

    return found_tlefile


def compute_satz_with_pyorbital(data_array):
    """Compute the sat zenith angles with pyorbital."""
    try:
        satname = data_array.attrs["platform_name"]
    except KeyError:
        LOG.error("Failed determining platform name!")
        raise

    LOG.debug(f"Platform name = {satname}")

    obs_time = generate_observation_time_array(data_array)

    lon, lat = data_array.attrs["area"].get_lonlats()

    LOG.debug("Get satellite elevation via Pyorbital")
    tlefilepath = get_tle_file(data_array.attrs["start_time"], data_array.attrs["end_time"])
    if tlefilepath:
        orb = Orbital(satname, tle_file=str(tlefilepath))
    else:
        orb = Orbital(satname)

    azimuth, elevation = orb.get_observer_look(obs_time, lon, lat, 0.)
    del azimuth
    sat_zen = 90. - elevation
    LOG.debug("Satellite zenith angles derived on data")

    LOG.debug("data_array dims/shape: %s %s", data_array.dims, data_array.shape)
    LOG.debug("obs_time shape: %s", obs_time.shape)
    LOG.debug("lon/lat shapes: %s %s", lon.shape, lat.shape)

    LOG.debug("sat_zen shape: %s", sat_zen.shape)

    satz = xr.DataArray(data=sat_zen, dims=["y", "x"],
                        coords=dict(
                            lon=(["y", "x"], lon),
                            lat=(["y", "x"], lat)),
                        attrs=dict(
                            description="Satellite zenith angle.",
                            units="deg"))

    return satz


def generate_observation_time_array(data_array):
    """Generate observation time for each scan line."""
    nlines = data_array.sizes["y"]

    start_time = data_array.attrs["start_time"]
    end_time = data_array.attrs["end_time"]

    time_step = (end_time - start_time) / nlines
    obs_time = start_time + time_step * np.arange(nlines)

    return obs_time[:, np.newaxis]


def prepare_height_for_resampling(scene, product):
    """Prepare CTTH/CBH altitude for resampling using CMA validity."""
    data = scene[product]
    valid_observation = scene["cma"].notnull()

    # Preserve valid cloud-free pixels through nearest-neighbour resampling.
    # Missing heights at pixels with a valid CMA observation are represented
    # temporarily by a sentinel, while pixels without a CMA observation stay
    # missing and are ignored by the resampler.
    data = data.fillna(HEIGHT_FILL_VALUE)
    scene[product] = data.where(valid_observation)


def restore_height_missing_values(scene, product):
    """Restore temporary fill values to NaN after resampling."""
    data = scene[product]
    scene[product] = data.where(data != HEIGHT_FILL_VALUE)


def blend_cloud_products(product, inputfile_patterns, areaid, *scenes, cache_dir=None):
    """Blend NWCSAF Geo and PPS cloud product scenes."""
    loaded_scenes = []
    satpy_composite_name = {"CT": "ct", "CTTH": "ctth_alti", "CBH": "cbh_alti"}.get(product)

    del inputfile_patterns  # Retained in the API for compatibility with callers.

    for _idx, files in enumerate(scenes):
        LOG.debug("Files: %s", str(files))
        loader = CloudProductsLoader(files)

        datasets = [satpy_composite_name]
        is_polar = loader._reader == "nwcsaf-pps_nc"
        if satpy_composite_name in HEIGHT_PRODUCTS and is_polar:
            datasets.append("cma")

        loader.load(datasets)

        if satpy_composite_name in HEIGHT_PRODUCTS and is_polar:
            prepare_height_for_resampling(loader.scene, satpy_composite_name)

        LOG.debug("%s after load: dims=%s shape=%s", satpy_composite_name,
                  loader.scene[satpy_composite_name].dims,
                  loader.scene[satpy_composite_name].shape)
        loader.prepare_satz_angles_on_area(satpy_composite_name, areaid)
        loaded_scenes.append(loader.scene)

    RADIUS_OF_INFLUENCE = 6000
    group_name = satpy_composite_name.upper() + "_group"
    if len(loaded_scenes) == 1:
        local = loaded_scenes[0].resample(areaid, radius_of_influence=RADIUS_OF_INFLUENCE,
                                          reduce_data=False, cache_dir=cache_dir,
                                          mask_area=True)
        if satpy_composite_name  in HEIGHT_PRODUCTS:
            restore_height_missing_values(local, satpy_composite_name)
            local[satpy_composite_name].attrs["_FillValue"] = HEIGHT_FILL_VALUE

        return local, satpy_composite_name

    mscn = MultiScene(loaded_scenes)
    groups = {DataQuery(name=group_name): [satpy_composite_name]}
    mscn.group(groups)

    LOG.debug("Before call to resample on MultiScene...")
    resampled = mscn.resample(areaid, radius_of_influence=RADIUS_OF_INFLUENCE,
                              reduce_data=False, cache_dir=cache_dir, mask_area=True)

    # Convert our temporary sentinel back to actual missing CTTH before
    # the scenes are blended.
    if satpy_composite_name  in HEIGHT_PRODUCTS:
        for scene in resampled.scenes:
            restore_height_missing_values(scene, satpy_composite_name)

    LOG.debug("Getting the weights...")
    weights = []
    for scene in resampled.scenes:
        wgt = 1 / scene["satz"]
        weights.append(da.nan_to_num(wgt))

    from functools import partial
    stack_with_weights = partial(stack, weights=weights)

    LOG.debug("Before blending...")
    blended = resampled.blend(blend_function=stack_with_weights)

    if satpy_composite_name in HEIGHT_PRODUCTS:
        try:
            blended[group_name] = blended[group_name].fillna(HEIGHT_FILL_VALUE)
            blended[group_name].attrs["_FillValue"] = HEIGHT_FILL_VALUE
        except KeyError:
            LOG.exception("Failed setting fill value for %s.", group_name)

    LOG.debug("Before returning the blended scene")
    return blended, group_name


if __name__ == "__main__":
    # base_dir = pathlib.Path("/data/lang/satellit/mesan/")
    base_dir = pathlib.Path(f"{HOME}/data/mesan")

    GEO_DIR = base_dir / "geo_in/20231006"

    GEO_FILES = [*GEO_DIR.glob("S_NWC_*MSG3_MSG-N-VISIR_20231006T0700*_PLAX.nc")]
    GEO_FILES2 = [*GEO_DIR.glob("S_NWC_*MSG3_MSG-N-VISIR_20231006T0715*_PLAX.nc")]

    # areaid = "mesanEx"
    areaid = "euro4"

    POLAR_DIR = base_dir / "polar_in/20231006"
    # POES_FILES = [*POLAR_DIR.glob("S_NWC_*noaa19_72055_20230201T1651106Z*nc")]
    POES_FILES = [*POLAR_DIR.glob("S_NWC_*_noaa18_00000_20231006T0756*Z*.nc")]
    # NPP_FILES = [*POLAR_DIR.glob("S_NWC_*npp_00000_20230116T11*nc")]
    #METOP_FILES = [*POLAR_DIR.glob("S_NWC_*metopc_21988_20230201T1657001Z*nc")]
    #METOP_FILES = [*POLAR_DIR.reglob("S_NWC_*metopc_00000_20231006T0650001Z*nc")]
    METOP_FILES = [*POLAR_DIR.glob("S_NWC_*metopb_00000_20231006T0737000Z*nc")]

    POLAR_FILES = [POES_FILES, METOP_FILES]
    # blended, group_name = blend_ct_products("ct", areaid, GEO_FILES, POES_FILES,  # NPP_FILES,
    #                                           cache_dir=gettempdir())

    # blended, group_name = blend_cloud_products("ct", areaid, GEO_FILES, METOP_FILES,
    #                                            cache_dir=gettempdir())
    # blended.save_dataset(group_name,
    #                      filename="./blended_stack_weighted_geo_polar_{area}_ct.nc".format(area=areaid))

    #blended, group_name = blend_cloud_products("ctth_alti", areaid, GEO_FILES, METOP_FILES,
    blended, group_name = blend_cloud_products("ctth_alti", areaid, METOP_FILES,
                                               cache_dir=gettempdir())
    blended.save_dataset(group_name,
                         filename="./blended_stack_weighted_geo_polar_{area}_ctth.nc".format(area=areaid))
    # filename="./blended_stack_weighted_geo_polar_{area}.nc".format(area=areaid))
