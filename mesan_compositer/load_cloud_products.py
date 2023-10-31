#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <Firstname.Lastname @ smhi.se>

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
from satpy.modifiers.angles import get_satellite_zenith_angle
from satpy.multiscene import stack
from satpy.utils import debug_on
from trollsift import Parser, globify

debug_on()

LOG = logging.getLogger(__name__)


class CloudProductsLoader:
    """Class to load and prepare cloud products on area."""

    def __init__(self, cloud_files):
        """Set up the instance."""
        self._cloud_files = cloud_files
        self.scene = None
        if "MSG" in str(self._cloud_files[0].name):
            self._reader = "nwcsaf-geo"
        else:
            self._reader = "nwcsaf-pps_nc"

    def load(self, to_load):
        """Load the cloud products."""
        self.scene = Scene({self._reader: self._cloud_files})
        self.scene.load(to_load)
        # GEO and PPS cloud top heights are not scaled the same!
        # Convert Geo cloud top height to PPS cloud top height
        # FIXME!
        this = self.scene["ctth_alti"].data.compute()
        hist = np.histogram(np.nan_to_num(this, nan=27000), bins=29)
        if "ctth_alti" in to_load and self._reader == "nwcsaf-geo":
            # breakpoint()
            pass

        return hist

    def prepare_satz_angles_on_area(self, product):
        """Derive the satellite zenith angles and attach data to Satpy scene object."""
        self.scene["satz"] = self._get_satz_angles(product)
        # shape = self.scene["satz"].shape
        # ndim = shape[0] * shape[1]
        # print("Relative percentage of  nan's in satz: %f" % (100 *
        #                                                      np.sum(da.isnan(
        #                                                          self.scene["satz"])).data.compute() /
        #                                                      ndim))

        self.scene["satz"].attrs["area"] = self.scene[product].attrs["area"]

    def _get_satz_angles(self, product):
        """Calculate the satellite zenith angles using Pyorbital."""
        data_array = self.scene[product]
        try:
            return get_satellite_zenith_angle(data_array)
        except KeyError:
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
        print("Failed determining platform name!")
        raise

    print("Platform name = {platform}".format(platform=satname))

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

    satz = xr.DataArray(data=sat_zen, dims=["y", "x"],
                        coords=dict(
                            lon=(["y", "x"], lon),
                            lat=(["y", "x"], lat)),
                        attrs=dict(
                            description="Satellite zenith angle.",
                            units="deg"))

    return satz


def generate_observation_time_array(data_array):
    """Generate the observation time array."""
    shape = data_array.shape
    time_step = (data_array.attrs["end_time"] - data_array.attrs["start_time"]) / shape[0]
    starttime = data_array.attrs["start_time"]
    obs_time = starttime + time_step * np.arange(shape[0])
    return obs_time[:, np.newaxis]


def blend_cloud_products(product, areaid, *scenes, cache_dir=None):
    """Blend NWCSAF Geo and PPS cloud product scenes."""
    loaded_scenes = []
    for _idx, files in enumerate(scenes):
        LOG.debug("Files: %s", str(files))
        loader = CloudProductsLoader(files)
        loader.load([product])
        # import matplotlib.pyplot as plt
        # plt.bar(h_[1][1::], height=h_[0], width=800)
        # plt.savefig('barplot_{idx}'.format(idx=idx))
        # plt.clf()
        loader.prepare_satz_angles_on_area(product)
        loaded_scenes.append(loader.scene)

    mscn = MultiScene(loaded_scenes)
    group_name = product.upper() + "_group"
    groups = {DataQuery(name=group_name): [product]}
    mscn.group(groups)

    LOG.debug("Before call to reaample on Multiscene...")
    resampled = mscn.resample(areaid, radius_of_influence=10000,
                              reduce_data=False, cache_dir=cache_dir, mask_area=False)

    LOG.debug("Getting the weights...")
    weights = []
    for _i, scene in enumerate(resampled.scenes):
        wgt = 1 / scene["satz"]
        weights.append(da.nan_to_num(wgt))

    from functools import partial
    stack_with_weights = partial(stack, weights=weights)
    # stack_no_weights = partial(stack)
    LOG.debug("Before resampling...")
    blended = resampled.blend(blend_function=stack_with_weights)
    # blended = resampled.blend(blend_function=stack_no_weights)

    LOG.debug("Before returning the blended scene")
    return blended, group_name


if __name__ == "__main__":
    # base_dir = pathlib.Path("/data/lang/satellit/mesan/")
    base_dir = pathlib.Path("/home/a000680/data/mesan")

    GEO_DIR = base_dir / "geo_in/20231006"

    GEO_FILES = [*GEO_DIR.glob("S_NWC_*MSG3_MSG-N-VISIR_20231006T0700*_PLAX.nc")]
    GEO_FILES2 = [*GEO_DIR.glob("S_NWC_*MSG3_MSG-N-VISIR_20231006T0715*_PLAX.nc")]

    # areaid = "mesanEx"
    areaid = "euro4"

    POLAR_DIR = base_dir / "polar_in/20231006"
    # POES_FILES = [*POLAR_DIR.glob("S_NWC_*noaa19_72055_20230201T1651106Z*nc")]
    POES_FILES = [*POLAR_DIR.glob("S_NWC_*_noaa18_00000_20231006T0756*Z*.nc")]
    # NPP_FILES = [*POLAR_DIR.glob("S_NWC_*npp_00000_20230116T11*nc")]
    # METOP_FILES = [*POLAR_DIR.glob("S_NWC_*metopc_21988_20230201T1657001Z*nc")]
    METOP_FILES = [*POLAR_DIR.glob("S_NWC_*metopc_00000_20231006T0650001Z*nc")]

    POLAR_FILES = [POES_FILES, METOP_FILES]
    # blended, group_name = blend_ct_products("ct", areaid, GEO_FILES, POES_FILES,  # NPP_FILES,
    #                                           cache_dir=gettempdir())

    # blended, group_name = blend_cloud_products("ct", areaid, GEO_FILES, METOP_FILES,
    #                                            cache_dir=gettempdir())
    # blended.save_dataset(group_name,
    #                      filename="./blended_stack_weighted_geo_polar_{area}_ct.nc".format(area=areaid))

    blended, group_name = blend_cloud_products("ctth_alti", areaid, GEO_FILES, METOP_FILES,
                                               cache_dir=gettempdir())
    blended.save_dataset(group_name,
                         filename="./blended_stack_weighted_geo_polar_{area}_ctth.nc".format(area=areaid))
    # filename="./blended_stack_weighted_geo_polar_{area}.nc".format(area=areaid))
