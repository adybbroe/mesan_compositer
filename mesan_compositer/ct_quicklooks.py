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

"""Make cloud composite quicklooks."""

import argparse
import os

import dask.array as da
import numpy as np
import xarray as xr
from satpy.composites import PaletteCompositor
from trollimage.xrimage import XRImage

from mesan_compositer import ctth_height, nwcsaf_cloudtype_2021

CHUNK_SIZE = 4096


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
    outfile = netcdf_filename.strip(".nc") + "_height.png"
    if destpath:
        outfile = os.path.join(destpath, os.path.basename(outfile))
    ximg.save(outfile)

    return outfile


def ctype_quicklook_from_netcdf(group_name, netcdf_filename):
    """Make a CLoud Type quicklook image from the netCDF file."""
    nc_ = xr.open_dataset(netcdf_filename, decode_cf=True,
                          mask_and_scale=True,
                          chunks={"columns": CHUNK_SIZE,
                                  "rows": CHUNK_SIZE})

    # cloudtype = nc_[group_name][0][:]
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
    outfile = netcdf_filename.strip(".nc") + "_cloudtype.png"
    ximg.save(outfile)

    return outfile


def get_arguments():
    """Get command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--netcdf_filepath",
                        type=str,
                        dest="netcdf_filepath",
                        required=True,
                        help="The netcdf file path of the cloud type composite.")

    args = parser.parse_args()

    return args.netcdf_filepath


if __name__ == "__main__":

    # areaid = "euro4"
    # areaid = 'mesanEx'
    # FILEPATH = "./blended_stack_weighted_geo_noaa-19_metop-c_{area}.nc".format(area=areaid)
    # FILEPATH = "./blended_stack_weighted_geo_noaa-19_{area}.nc".format(area=areaid)
    # FILEPATH = "./blended_stack_weighted_geo_polar_euro4.nc"
    # FILEPATH = "/home/a000680/data/mesan/output/mesan_composite_euro4_20230201_1700_ct_20231005181316.nc"
    # FILEPATH = "./blended_stack_weighted_geo_n18_{area}.nc".format(area=areaid)

    netcdfpath = get_arguments()
    group_name = 'CTTH_ALTI_group'
    # group_name = 'ctth_alti'
    # ctype_quicklook_from_netcdf("CT_group", netcdfpath)
    ctth_quicklook_from_netcdf(group_name, netcdfpath, destpath="./")
