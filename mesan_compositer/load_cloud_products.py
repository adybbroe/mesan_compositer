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

"""Utilities to load and prepare cloud products on area."""

import os
from glob import glob
import xarray as xr
import numpy as np
from pyorbital.orbital import Orbital
from satpy import Scene
from satpy.utils import debug_on
from satpy import MultiScene, DataQuery
from satpy.multiscene import stack
from satpy.modifiers.angles import get_satellite_zenith_angle

debug_on()


def get_remapped_angles(scn, area_def):
    """From a polar/geo Scene object get the remapped angles."""
    from pyresample.bilinear import XArrayBilinearResampler
    resampler = XArrayBilinearResampler(scn['ct'].area, area_def, 30e3)

    satz = scn['satz']
    return resampler.resample(satz)


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

    def prepare_satz_angles_on_area(self, area_def):
        """Derive the satellite zenith angles and attach data to Satpy scene object."""
        composite_name = 'ct'

        self.scene['satz'] = get_satellite_zenith_angle(self.scene[composite_name])
        self.scene['satz'].attrs['area'] = self.scene[composite_name].attrs['area']

        return get_remapped_angles(self.scene, area_def)


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

    def prepare_satz_angles_on_area(self, area_def):
        """Derive the satellite zenith angles and attach data to Satpy scene object."""
        self.scene['satz'] = self._get_satz_angles()
        self.scene['satz'].attrs['area'] = self.scene['cma'].attrs['area']

        return get_remapped_angles(self.scene, area_def)

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
        _, elevation = orb.get_observer_look(obs_time, lon, lat, 0.)
        del _
        sat_zen = 90. - elevation

        satz = xr.DataArray(data=sat_zen, dims=["y", "x"],
                            coords=dict(
                                lon=(["y", "x"], lon),
                                lat=(["y", "x"], lat)),
                            attrs=dict(
                                description="Satellite zenith angle.",
                                units="deg",
        ),)

        return satz


if __name__ == "__main__":

    from pyresample import load_area

    GEO_DIR = "/home/a000680/data/mesan/geo_in/v2021"
    GEO_FILES = glob(os.path.join(GEO_DIR, 'S_NWC_*MSG4_MSG-N-VISIR_20230116T1100*PLAX.nc'))

    geo = GeoCloudProductsLoader(GEO_FILES)
    geo.load()

    # areaid = "mesanEx"
    areaid = 'euro4'
    area_def = load_area('/home/a000680/usr/src/pytroll-config/etc/areas.yaml', areaid)

    # geo_satz = geo.prepare_satz_angles_on_area(area_def)

    POLAR_DIR = "/home/a000680/data/mesan/polar_in/v2021"
    N18_FILES = glob(os.path.join(POLAR_DIR, 'S_NWC_*noaa18_91014*nc'))
    N20_FILES = glob(os.path.join(POLAR_DIR, 'S_NWC_*noaa20_00000_20230116T10*nc'))

    n18 = PPSCloudProductsLoader(N18_FILES)
    n18.load()

    n18_satz = n18.prepare_satz_angles_on_area(area_def)

    mscn = MultiScene([geo.scene, n18.scene])
    groups = {DataQuery(name='CTY_group'): ['ct']}
    mscn.group(groups)

    resampled = mscn.resample(areaid, reduce_data=False)

    local_scn = resampled.scenes[0]['ct']
    geo_satz = get_satellite_zenith_angle(local_scn)

    weights = [1./geo_satz, 1./n18_satz]

    from functools import partial
    stack_with_weights = partial(stack, weights=weights)
    blended = resampled.blend(blend_function=stack_with_weights)
    blended.save_dataset('CTY_group', filename='./blended_stack_weighted_geo_n18_{area}.nc'.format(area=areaid))
