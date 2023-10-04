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
from glob import glob

import numpy as np
import xarray as xr

# from trollimage.xrimage import XRImage
from pyorbital.orbital import Orbital
from satpy import DataQuery, MultiScene, Scene
from satpy.modifiers.angles import get_satellite_zenith_angle
from satpy.multiscene import stack
from satpy.utils import debug_on

debug_on()

LOG = logging.getLogger(__name__)


class CloudProductsLoader:
    """Class to load and prepare cloud products on area."""

    def __init__(self, cloud_files):
        """Set up the instance."""
        self._cloud_files = cloud_files
        self.scene = None
        if "MSG" in cloud_files[0]:
            self._reader = "nwcsaf-geo"
        else:
            self._reader = "nwcsaf-pps_nc"

    def load(self, to_load):
        """Load the cloud products."""
        self.scene = Scene({self._reader: self._cloud_files})
        self.scene.load(to_load)

    def prepare_satz_angles_on_area(self):
        """Derive the satellite zenith angles and attach data to Satpy scene object."""
        self.scene["satz"] = self._get_satz_angles()
        self.scene["satz"].attrs["area"] = self.scene["ct"].attrs["area"]

    def _get_satz_angles(self):
        """Calculate the satellite zenith angles using Pyorbital."""
        data_array = self.scene["ct"]
        try:
            return get_satellite_zenith_angle(data_array)
        except KeyError:
            return compute_satz_with_pyorbital(data_array)


def compute_satz_with_pyorbital(data_array):
    """Compute the sat zenith angles with pyorbital."""
    satname = data_array.attrs["platform_name"]

    obs_time = generate_observation_time_array(data_array)

    lon, lat = data_array.attrs["area"].get_lonlats()
    LOG.debug("Get satellite elevation via Pyorbital")

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
    obs_time = np.dstack([obs_time]*shape[1])[0]
    return obs_time


def blend_ct_products(areaid, *scenes):
    """Blend Geo and PPS cloud product scenes."""
    product = "ct"

    loaded_scenes = []

    for files in scenes:
        loader = CloudProductsLoader(files)
        loader.load([product])
        loader.prepare_satz_angles_on_area()
        loaded_scenes.append(loader.scene)

    mscn = MultiScene(loaded_scenes)
    group_name = product.upper() + "_group"
    groups = {DataQuery(name=group_name): [product]}
    mscn.group(groups)

    resampled = mscn.resample(areaid, reduce_data=False)

    weights = [1. / scene["satz"] for scene in resampled.scenes]

    from functools import partial
    stack_with_weights = partial(stack, weights=weights)
    blended = resampled.blend(blend_function=stack_with_weights)

    # polar_sats = (polar.scene['ct'].attrs['platform_name'].lower())
    blended.save_dataset(group_name,
                         filename="./blended_stack_weighted_geo_polar_{area}.nc".format(area=areaid))


if __name__ == "__main__":
    base_dir = "/data/lang/satellit/mesan/"

    GEO_DIR = os.path.join(base_dir, "geo_in/v2021")
    # GEO_FILES = glob(os.path.join(GEO_DIR, 'S_NWC_*MSG4_MSG-N-VISIR_20230116T1100*PLAX.nc'))
    GEO_FILES = glob(os.path.join(GEO_DIR, "S_NWC_*MSG4_MSG-N-VISIR_20230201T1700*_PLAX.nc"))

    # areaid = "mesanEx"
    areaid = "euro4"

    POLAR_DIR = os.path.join(base_dir, "polar_in/v2021")
    # N18_FILES = glob(os.path.join(POLAR_DIR, 'S_NWC_*noaa18_91014*nc'))
    POES_FILES = glob(os.path.join(POLAR_DIR, "S_NWC_*noaa19_72055_20230201T1651106Z*nc"))
    NPP_FILES = glob(os.path.join(POLAR_DIR, "S_NWC_*npp_00000_20230116T11*nc"))
    METOP_FILES = glob(os.path.join(POLAR_DIR, "S_NWC_*metopc_21988_20230201T1657001Z*nc"))

    blend_ct_products(areaid, GEO_FILES, POES_FILES, NPP_FILES)
