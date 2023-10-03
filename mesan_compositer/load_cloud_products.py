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

import os
from glob import glob
import xarray as xr
import numpy as np

# from trollimage.xrimage import XRImage
from pyorbital.orbital import Orbital
from satpy import Scene
from satpy.utils import debug_on
from satpy import MultiScene, DataQuery
from satpy.multiscene import stack
from satpy.modifiers.angles import get_satellite_zenith_angle
import logging

debug_on()

LOG = logging.getLogger(__name__)


class GeoCloudProductsLoader:
    """Class to load and prepare a Geo Cloud product on area."""

    def __init__(self, cloud_files, reader='nwcsaf-geo'):
        """Initialize the class."""
        self._cloud_files = cloud_files
        self.scene = None
        self._composites_and_datasets_to_load = ['ct']
        self._reader = reader

    def load(self):
        """Load the cloud products."""
        self.scene = Scene({self._reader: self._cloud_files})
        self.scene.load(self._composites_and_datasets_to_load)


class PPSCloudProductsLoader:
    """Class to load and prepare a NWCSAF/PPS Cloud product on area."""

    def __init__(self, cloud_files):
        """Initialize the class."""
        self._cloud_files = cloud_files
        self.scene = None
        self._composites_and_datasets_to_load = ['cma', 'ct']

    def load(self):
        """Load the cloud products."""
        self.scene = Scene(filenames=self._cloud_files, reader='nwcsaf-pps_nc')
        self.scene.load(self._composites_and_datasets_to_load)

    def prepare_satz_angles_on_area(self):
        """Derive the satellite zenith angles and attach data to Satpy scene object."""
        self.scene['satz'] = self._get_satz_angles()
        self.scene['satz'].attrs['area'] = self.scene['cma'].attrs['area']

    def _get_satz_angles(self):
        """Calculate the satellite zenith angles using Pyorbital."""
        self._shape = self.scene['cma'].shape

        satname = self.scene['cma'].attrs['platform_name']
        orb = Orbital(satname)

        time_step = (self.scene['cma'].attrs['end_time'] -
                     self.scene['cma'].attrs['start_time'])/self._shape[0]
        starttime = self.scene['cma'].attrs['start_time']
        obs_time = starttime + time_step * np.arange(self._shape[0])
        obs_time = np.dstack([obs_time]*self._shape[1])[0]

        lon, lat = self.scene['cma'].attrs['area'].get_lonlats()
        LOG.debug('Get satellite elevation via Pyorbital')
        _, elevation = orb.get_observer_look(obs_time, lon, lat, 0.)
        del _
        sat_zen = 90. - elevation
        LOG.debug('Satellite zenith angles derived on data')

        satz = xr.DataArray(data=sat_zen, dims=["y", "x"],
                            coords=dict(
                                lon=(["y", "x"], lon),
                                lat=(["y", "x"], lat)),
                            attrs=dict(
                                description="Satellite zenith angle.",
                                units="deg",
        ),)

        return satz


def blend_ct_products(areaid, geo_files, list_of_polar_scenes: list[list[str]]):
    """Blend Geo and PPS cloud product scenes."""
    # area_def = load_area('/home/a000680/usr/src/pytroll-config/etc/areas.yaml', areaid)

    geo = GeoCloudProductsLoader(geo_files)
    geo.load()

    polar_scenes = []
    for pps_files in list_of_polar_scenes:
        polar = PPSCloudProductsLoader(pps_files)
        polar.load()
        polar.prepare_satz_angles_on_area()
        polar_scenes.append(polar)

    mscn = MultiScene([geo.scene] + [polar.scene for polar in polar_scenes])
    groups = {DataQuery(name='CTY_group'): ['ct']}
    mscn.group(groups)

    resampled = mscn.resample(areaid, reduce_data=False)
    local_scn = resampled.scenes[0]['ct']

    geo_satz = get_satellite_zenith_angle(local_scn)

    polar_satz = resampled.scenes[1]['satz']

    weights = [1./geo_satz, 1./polar_satz]

    from functools import partial
    stack_with_weights = partial(stack, weights=weights)
    blended = resampled.blend(blend_function=stack_with_weights)

    polar_sats = (polar.scene['ct'].attrs['platform_name'].lower())
    blended.save_dataset('CTY_group',
                         filename='./blended_stack_weighted_geo_{polar}_{area}.nc'.format(polar=polar_sats,
                                                                                          area=areaid))


if __name__ == "__main__":

    GEO_DIR = "/home/a000680/data/mesan/geo_in/v2021"
    # GEO_FILES = glob(os.path.join(GEO_DIR, 'S_NWC_*MSG4_MSG-N-VISIR_20230116T1100*PLAX.nc'))
    GEO_FILES = glob(os.path.join(GEO_DIR, 'S_NWC_*MSG4_MSG-N-VISIR_20230201T1700*_PLAX.nc'))

    # areaid = "mesanEx"
    areaid = "euro4"

    POLAR_DIR = "/home/a000680/data/mesan/polar_in/v2021"
    # N18_FILES = glob(os.path.join(POLAR_DIR, 'S_NWC_*noaa18_91014*nc'))
    POES_FILES = glob(os.path.join(POLAR_DIR, 'S_NWC_*noaa19_72055_20230201T1651106Z*nc'))
    NPP_FILES = glob(os.path.join(POLAR_DIR, 'S_NWC_*npp_00000_20230116T11*nc'))
    METOP_FILES = glob(os.path.join(POLAR_DIR, 'S_NWC_*metopc_21988_20230201T1657001Z*nc'))

    blend_ct_products(areaid, GEO_FILES, [POES_FILES, NPP_FILES])
