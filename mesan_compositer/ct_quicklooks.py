#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Adam.Dybbroe

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

"""Make quick look images of the cloudtype composite
"""

import argparse
from datetime import datetime
from mesan_compositer.netcdf_io import ncCloudTypeComposite
import sys
import os
from PIL import Image
import ConfigParser

CFG_DIR = os.environ.get('MESAN_COMPOSITE_CONFIG_DIR', './')
MODE = os.environ.get("SMHI_MODE", 'offline')

conf = ConfigParser.ConfigParser()
configfile = os.path.join(CFG_DIR, "mesan_sat_config.cfg")
if not os.path.exists(configfile):
    raise IOError('Config file %s does not exist!' % configfile)
conf.read(configfile)

options = {}
for option, value in conf.items(MODE, raw=True):
    options[option] = value


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--datetime', '-d', help='Date and time of observation - yyyymmddhh',
                        required=True)
    parser.add_argument('--area_id', '-a', help='Area id',
                        required=True)

    args = parser.parse_args()

    obstime = datetime.strptime(args.datetime, '%Y%m%d%H')
    values = {"area": args.area_id, }
    bname = obstime.strftime(options['ct_composite_filename']) % values
    path = options['composite_output_dir']
    filename = os.path.join(path, bname)

    comp = ncCloudTypeComposite()
    comp.load(filename + '.nc')

    import mpop.imageo.palettes
    palette = mpop.imageo.palettes.cms_modified()
    from mpop.imageo import geo_image
    img = geo_image.GeoImage(comp.cloudtype.data,
                             args.area_id,
                             None,
                             fill_value=(0),
                             mode = "P",
                             palette = palette)
    img.save(filename + '_cloudtype.png')

    comp_id = comp.id.data * 13
    idimg = geo_image.GeoImage(comp_id,
                               args.area_id,
                               None,
                               fill_value=(0),
                               mode = "P",
                               palette = palette)
    idimg.save(filename + '_id.png')

    comp_w = comp.weight.data * 20
    wimg = geo_image.GeoImage(comp_w,
                              args.area_id,
                              None,
                              fill_value=(0),
                              mode = "P",
                              palette = palette)
    wimg.save(filename + '_weight.png')
