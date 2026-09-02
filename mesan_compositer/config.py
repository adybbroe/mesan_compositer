#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023, 2026 Adam.Dybbroe

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

"""Reading the yaml configuration parameters."""


import yaml


def get_config(configfile):
    """Read configuration from a YAML file."""
    with open(configfile, encoding="utf-8") as fp_:
        config = yaml.safe_load(fp_)

    if config is None:
        return {}

    if not isinstance(config, dict):
        raise ValueError("Configuration root must be a mapping")

    return config
