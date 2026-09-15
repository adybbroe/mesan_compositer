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

"""Unit testing the cloud amount and cloud top height super obs creation."""


import xarray as xr

from mesan_compositer.prt_nwcsaf_cloudamount import derive_sobs as derive_sobs_clamount
from mesan_compositer.prt_nwcsaf_cloudheight import derive_sobs as derive_sobs_clheight


def test_derive_sobs_writes_expected_ascii(tmp_path, cloud_type_netcdf):
    """"Test derive super observations of cloud amount in required ascii format."""
    outfile = tmp_path / "cloudamount.txt"

    with xr.open_dataset(cloud_type_netcdf) as ds:
        derive_sobs_clamount(ds["ct"], "71", npix=8, resultfile=outfile)

    assert outfile.read_text() == (
        "   99999   12.00    4.00  -999 10 -60     1.00     0.15\n"
    )


def test_derive_sobs_writes_cloud_height_ascii(tmp_path, cloud_top_height_netcdf):
    """Test derive super observations of cloud top height in required legacy ascii format.

    This is actually testing 4 aspects of the super obs generation
    simultaneously: the 8×8 averaging, NaN rejection, centre-coordinate
    selection, and legacy south-to-north output ordering.
    """
    outfile = tmp_path / "cloudheight.dat"

    with xr.open_dataset(cloud_top_height_netcdf) as ds:
        derive_sobs_clheight(ds["ctth_alti"], npix=8, filepath=outfile)

    assert outfile.read_text() == (
        "   99999   20.00    4.00  -999 1 -60  3000.00   999.90\n"
        "   99999    4.00    4.00  -999 1 -60  1000.00   999.90\n"
    )
