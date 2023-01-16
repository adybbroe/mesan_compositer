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
from satpy import Scene

from satpy import MultiScene, DataQuery
from satpy.multiscene import stack_weighted, stack

from satpy.dataset import DataID
from satpy.modifiers.angles import get_satellite_zenith_angle


class GeoCloudProductsLoader:
    """Class to load and prepare a Geo Cloud product on area."""

    def __init__(self, cloud_files, hrit_files, area_id):
        """Initialize the class."""

        pass

    def load(self):
        """Load the cloud products."""

        pass
