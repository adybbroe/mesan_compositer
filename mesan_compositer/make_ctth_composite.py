#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Adam.Dybbroe

# Author(s):

#   Adam Dybbroe <Firstname.Lastname at smhi.se>

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

"""Make a CTTH composite."""


import argparse
import logging
import sys
from datetime import datetime, timedelta
from logging import handlers

from satpy.utils import debug_on

from mesan_compositer.config import get_config
from mesan_compositer.make_ct_composite import CloudproductCompositer

debug_on()

LOG = logging.getLogger(__name__)


#: Default time format
_DEFAULT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

#: Default log format
_DEFAULT_LOG_FORMAT = "[%(levelname)s: %(asctime)s : %(name)s] %(message)s"


def get_arguments():
    """Get command line arguments.

    args.logging_conf_file, args.config_file, obs_time, area_id, wsize

    Return:
      File path of the logging.ini file
      File path of the application configuration file
      Observation/Analysis time
      Area id
      Window size

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--datetime", "-d", help="Date and time of observation - yyyymmddhh",
                        required=True)
    parser.add_argument("--time_window", "-t", help="Number of minutes before and after time window",
                        required=True)
    parser.add_argument("--area_id", "-a", help="Area id",
                        required=True)
    parser.add_argument("-c", "--config_file",
                        type=str,
                        dest="config_file",
                        required=True,
                        help="The file containing configuration parameters e.g. mesan_sat_config.yaml")
    parser.add_argument("-l", "--logging",
                        help="The path to the log-configuration file (e.g. './logging.ini')",
                        dest="logging_conf_file",
                        type=str,
                        required=False)
    parser.add_argument("-v", "--verbose",
                        help="print debug messages too",
                        action="store_true")

    args = parser.parse_args()

    tanalysis = datetime.strptime(args.datetime, "%Y%m%d%H")
    delta_t = timedelta(minutes=int(args.time_window))
    if "template" in args.config_file:
        print("Template file given as master config, aborting!")
        sys.exit()

    return args.logging_conf_file, args.config_file, tanalysis, args.area_id, delta_t


if __name__ == "__main__":

    (logfile, config_filename, time_of_analysis, area_id, delta_time_window) = get_arguments()

    handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger("").addHandler(handler)
    # logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger("satpy").setLevel(logging.DEBUG)

    LOG = logging.getLogger("make_ct_composite")

    log_handlers = logging.getLogger("").handlers
    for log_handle in log_handlers:
        if type(log_handle) is handlers.SMTPHandler:
            LOG.debug("Mail notifications to: %s", str(log_handle.toaddrs))

    OPTIONS = get_config(config_filename)

    ctcomp = CloudproductCompositer(time_of_analysis, delta_time_window, area_id, OPTIONS, "CTTH")
    ctcomp.get_catalogue()
    ctcomp.blend_cloud_products()
    output_filepath = ctcomp.write()
