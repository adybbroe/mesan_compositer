#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023-2026 Adam.Dybbroe

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

"""Make cloud composite quicklooks."""

import argparse
import logging
from pathlib import Path

from mesan_compositer.logger import setup_logging
from mesan_compositer.quicklooks_from_netcdf import ctype_quicklook_from_netcdf

LOG = logging.getLogger(__name__)


def get_arguments():
    """Get command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--netcdf_filepath",
                        type=str,
                        dest="netcdf_filepath",
                        required=True,
                        help="The netcdf file path of the cloud type composite.")
    parser.add_argument("-l", "--logging",
                        help="The path to the log-configuration file (e.g. './log_config.yaml')",
                        dest="log_config_file",
                        type=str,
                        required=False)
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")

    args = parser.parse_args()

    return args


def main():
    """Generate the ct composite quicklook from netCDF file."""
    cmd_args = get_arguments()
    setup_logging(cmd_args)

    netcdfpath = Path(cmd_args.netcdf_filepath)
    group_name = "CT_group"
    try:
        resultfile = ctype_quicklook_from_netcdf(group_name, netcdfpath)
    except Exception:
        LOG.debug(f"Failed loading the Cloudtype data via the {group_name!r} dataset name.")
        group_name = "ct"
        LOG.debug(f"Try {group_name!r}...")
        try:
            resultfile = ctype_quicklook_from_netcdf(group_name, netcdfpath)
        except Exception:
            LOG.exception(f"Failed loading the Cloudtype data via the {group_name!r} dataset name")
            raise

    LOG.info(f"CT composite quicklook generated: {resultfile}")


if __name__ == "__main__":
    main()
