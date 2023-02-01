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

import os
import numpy as np
import xarray as xr
import dask.array as da
from trollimage.xrimage import XRImage

from satpy.composites import PaletteCompositor
from mesan_compositer import nwcsaf_cloudtype_2021

CHUNK_SIZE = 4096


if __name__ == "__main__":

    areaid = 'euro4'
    FILEPATH = "./blended_stack_weighted_geo_n18_{area}.nc".format(area=areaid)
    netcdf_filename = FILEPATH

    nc_ = xr.open_dataset(netcdf_filename, decode_cf=True,
                          mask_and_scale=True,
                          chunks={'columns': CHUNK_SIZE,
                                  'rows': CHUNK_SIZE})

    cloudtype = nc_['CTY_group'][:]

    palette = nwcsaf_cloudtype_2021()

    # Cloud type field:
    attrs = {'_FillValue': np.nan, 'valid_range': (0, 15)}
    palette_attrs = {'palette_meanings': list(range(16))}

    pdata = xr.DataArray(palette, attrs=palette_attrs)

    masked_data = np.ma.masked_outside(cloudtype.data, 0, 15)
    xdata = xr.DataArray(da.from_array(masked_data), dims=['y', 'x'], attrs=attrs)

    pcol = PaletteCompositor('mesan_cloudtype_composite')((xdata, pdata))
    ximg = XRImage(pcol)
    outfile = os.path.join('./', os.path.basename(netcdf_filename).strip('.nc') + '_cloudtype.png')
    ximg.save(outfile)
