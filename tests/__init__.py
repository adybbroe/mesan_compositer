#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015, 2019 Adam.Dybbroe

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

"""The unit test package."""

from tests import test_input
from tests import test_pps_msg_converter
from tests import test_compositer
from tests import test_pps_conversions
from tests import test_color_legends

import unittest

import os
TRAVIS = os.environ.get("TRAVIS", False)


def suite():
    """Run the global test suite."""
    mysuite = unittest.TestSuite()
    if not TRAVIS:
        # Test sphinx documentation pages:
        # mysuite.addTests(doctest.DocFileSuite('../../doc/usage.rst'))
        # Test the documentation strings
        # mysuite.addTests(doctest.DocTestSuite(solar))
        pass

    # Use the unittests also
    mysuite.addTests(test_input.suite())
    mysuite.addTests(test_pps_msg_converter.suite())
    mysuite.addTests(test_compositer.suite())
    mysuite.addTests(test_pps_conversions.suite())
    mysuite.addTests(test_color_legends.suite())

    return mysuite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
