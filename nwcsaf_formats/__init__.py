#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2019 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c14526.ad.smhi.se>

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

"""nwcsaf format conversion tools. First of all supporting conversion from PPS
v2014 format to old (v2012 and earlier) formats. Please be aware that this
format conversion is not a one-to-one mapping! There is information in the new
format that cannot be transfered to the old format!
"""


from pkg_resources import get_distribution, DistributionNotFound
try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

import logging
LOG = logging.getLogger(__name__)
