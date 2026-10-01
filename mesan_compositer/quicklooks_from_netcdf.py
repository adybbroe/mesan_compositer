#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2026 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c22526.ad.smhi.se>

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

"""Helper functions to create composite cloud product quicklook images from netCDF files."""

import logging
from pathlib import Path

import dask.array as da
import numpy as np
import xarray as xr
from satpy.composites.lookup import PaletteCompositor
from trollimage.xrimage import XRImage

from mesan_compositer import ctth_height, nwcsaf_cloudtype_2021

CHUNK_SIZE = 4096

LOG = logging.getLogger(__name__)

def ctth_quicklook_from_netcdf(group_name, netcdf_filename, destpath=None):
    """Make a Cloud Top Height quicklook image from the netCDF file."""
    nc_ = xr.open_dataset(netcdf_filename, decode_cf=True,
                          mask_and_scale=True,
                          chunks={"columns": CHUNK_SIZE,
                                  "rows": CHUNK_SIZE})

    ctth_alti = nc_[group_name][:]
    ctth_alti = ctth_alti.where(ctth_alti < 63535)

    ctth_data = ctth_alti.data
    ctth_data = ctth_data.clip(min=0) / 500 + 1
    ctth_data = ctth_data.astype("int32")

    palette = ctth_height()

    attrs = {"_FillValue": np.nan, "valid_range": (1, 100)}
    palette_attrs = {"palette_meanings": list(range(100))}

    pdata = xr.DataArray(palette, attrs=palette_attrs)

    masked_data = np.ma.masked_outside(ctth_data, 1, 100)
    xdata = xr.DataArray(da.from_array(masked_data), dims=["y", "x"], attrs=attrs)

    pcol = PaletteCompositor("mesan_cloud_top_height_composite")((xdata, pdata))
    ximg = XRImage(pcol)

    outfilename = netcdf_filename.stem + "_height.png"
    if destpath:
        outfile = Path(destpath) / outfilename
    else:
        outfile = netcdf_filename.parent / outfilename
    ximg.save(outfile)

    return outfile


def ctype_quicklook_from_netcdf(group_name, netcdf_filename, destpath=None):
    """Make a CLoud Type quicklook image from the netCDF file."""
    nc_ = xr.open_dataset(netcdf_filename, decode_cf=True,
                          mask_and_scale=True,
                          chunks={"columns": CHUNK_SIZE,
                                  "rows": CHUNK_SIZE})

    cloudtype = nc_[group_name][:]

    palette = nwcsaf_cloudtype_2021()

    # Cloud type field:
    attrs = {"_FillValue": np.nan, "valid_range": (0, 15)}
    palette_attrs = {"palette_meanings": list(range(16))}

    pdata = xr.DataArray(palette, attrs=palette_attrs)

    masked_data = np.ma.masked_outside(cloudtype.data, 0, 15)
    xdata = xr.DataArray(da.from_array(masked_data), dims=["y", "x"], attrs=attrs)

    pcol = PaletteCompositor("mesan_cloudtype_composite")((xdata, pdata))
    ximg = XRImage(pcol)

    outfilename = netcdf_filename.stem + "_cloudtype.png"
    if destpath:
        outfile = Path(destpath) / outfilename
    else:
        outfile = netcdf_filename.parent / outfilename
    ximg.save(outfile)

    return outfile
