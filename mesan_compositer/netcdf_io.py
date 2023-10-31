#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 - 2019, 2023 Adam.Dybbroe

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

"""Defining the netCDF4 mesan-composite object with read and write methods."""

import logging

import xarray as xr

LOG = logging.getLogger(__name__)


def get_nc_attributes_from_object(info_dict):
    """Get netCDF attributes from file object."""
    attrs = {}
    for key in info_dict.keys():
        if key in ["var_data"]:
            continue
        attrs[key] = info_dict[key]

    return attrs


def load_ct_composite(netcdf_filepath, group_name):
    """Load the blended cloud type composite."""
    CHUNK_SIZE = 4096
    nc_ = xr.open_dataset(netcdf_filepath, decode_cf=True,
                          mask_and_scale=True,
                          chunks={"columns": CHUNK_SIZE,
                                  "rows": CHUNK_SIZE})

    return nc_[group_name][:]


class cloudComposite:
    """Container for the cloud product composite."""

    def __init__(self, filename, cp_name, areaname=None):
        """Initialize the cloud product composite."""
        self.netcdf_filepath = filename
        self.group_name = cp_name + "_group"
        self.area_name = areaname
        self.area = None
        self.lon = None
        self.lat = None

    def load(self):
        """Load the cloud composite from file."""
        CHUNK_SIZE = 4096
        nc_ = xr.open_dataset(self.netcdf_filepath, decode_cf=True,
                              mask_and_scale=True,
                              chunks={"columns": CHUNK_SIZE,
                                      "rows": CHUNK_SIZE})

        self.data = nc_[self.group_name][:]
        self.lon = nc_["longitude"][:]
        self.lat = nc_["latitude"][:]
        if self.area_name:
            self.area = nc_[self.area_name]
