#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013, 2014, 2015 Adam Dybbroe

# Author(s):

#   Adam Dybbroe <adam.dybbroe@smhi.se>

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


try:
    with open('./README.md', 'r') as fd:
        long_description = fd.read()
except IOError:
    long_description = ''


from setuptools import setup
import imp

SHORT_DESC = ("Mesan satellite compositer")

version = imp.load_source(
    'mesan_compositer.version', 'mesan_compositer/version.py')

setup(name='mesan-compositer',
      version='0.1.0',
      description=SHORT_DESC,
      author='Adam Dybbroe, Tomas Landelius',
      author_email='adam.dybbroe@smhi.se, tomas.landelius@smhi.se',
      classifiers=['Development Status :: 4 - Beta',
                   'Intended Audience :: Science/Research',
                   'License :: OSI Approved :: GNU General Public License v3 ' +
                   'or later (GPLv3+)',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python',
                   'Topic :: Scientific/Engineering'],
      # url='https://github.com/adybbroe/py...',
      # download_url="https://github.com/adybbroe/py....
      long_description=long_description,
      license='GPLv3',

      packages=['mesan_compositer', 'nwcsaf_formats'],
      package_data={},

      # Project should use reStructuredText, so ensure that the docutils get
      # installed or upgraded on the target machine
      install_requires=['docutils>=0.3',
                        'numpy>=1.5.1',
                        'mpop>=v1.0',
                        'pyresample'],
      #'pyorbital >= v0.2.3'],

      test_requires=["mock"],
      extras_require={'netcdf4-python': ['netCDF4']},
      scripts=['mesan_compositer/make_ct_composite.py',
               'mesan_compositer/make_ctth_composite.py',
               'mesan_compositer/prt_nwcsaf_cloudamount.py',
               'mesan_compositer/ct_quicklooks.py',
               'mesan_compositer/ctth_quicklooks.py',
               'mesan_compositer/mesan_composite_runner.py'],
      data_files=[('etc', ['etc/mesan_sat_config.cfg_template']),
                  ],
      test_suite='tests.suite',
      tests_require=[],
      zip_safe=False
      )
