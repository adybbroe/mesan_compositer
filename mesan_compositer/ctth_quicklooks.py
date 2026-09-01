#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014-2026 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <Firstname.Lastname at smhi.se>

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

"""Make quick look images of the ctth composite."""

import argparse

from mesan_compositer.ct_quicklooks import ctth_quicklook_from_netcdf


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

    netcdfpath = get_arguments()
    #group_name = "CTTH_ALTI_group"
    group_name = "ctth_alti"
    # group_name = 'ctth_alti'
    # ctype_quicklook_from_netcdf("CT_group", netcdfpath)
    ctth_quicklook_from_netcdf(group_name, netcdfpath, destpath="./")
