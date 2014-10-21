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

"""Make a Cloud Type composite
"""


import argparse
from datetime import datetime, timedelta
import numpy as np

from mpop.satin.msg_hdf import ctype_procflags2pps

from mesan_compositer.composite_tools import (get_msglist,
                                              get_ppslist,
                                              wCT)
from mesan_compositer.netcdf_io import ncCloudTypeComposite

import sys
import os

CFG_DIR = os.environ.get('MESAN_COMPOSITE_CONFIG_DIR', './')
MODE = os.environ.get("SMHI_MODE", 'offline')

METOPS = ['metop02', 'metop01']

SENSOR = {'noaa': 'avhrr',
          'metop': 'avhrr',
          'npp': 'viirs'}


def ctype_pps(pps, areaid='mesanX'):
    """Load PPS Cloudtype and reproject"""
    from mpop.satellites import PolarFactory
    global_data = PolarFactory.create_scene(pps.platform, str(pps.number),
                                            SENSOR.get(pps.platform, 'avhrr'),
                                            pps.timeslot, pps.orbit)
    global_data.load(['CloudType'])
    return global_data.project(areaid)


def ctype_msg(msg, areaid='mesanX'):
    """Load MSG paralax corrected cloud type and reproject"""
    from mpop.satellites import GeostationaryFactory

    global_geo = GeostationaryFactory.create_scene(msg.platform,
                                                   msg.number, "seviri",
                                                   time_slot=msg.timeslot)
    global_geo.load(['CloudType_plax'])
    return global_geo.project(areaid)

# ---------------------------------------------------------------------------


class ctCompositer(object):

    """The Cloud Type Composite generator class"""

    def __init__(self, obstime, tdiff, areaid, **kwargs):
        import ConfigParser

        conf = ConfigParser.ConfigParser()
        configfile = os.path.join(CFG_DIR, "mesan_sat_config.cfg")
        if not os.path.exists(configfile):
            raise IOError('Config file %s does not exist!' % configfile)
        conf.read(configfile)

        options = {}
        for option, value in conf.items(MODE, raw=True):
            options[option] = value

        values = {"area": areaid, }

        if 'filename' in kwargs:
            self.filename = kwargs['filename']
        else:
            # Generate the filename from the observation time and the
            # specifcations in the config file:
            bname = obstime.strftime(options['ct_composite_filename']) % values
            path = options['composite_output_dir']
            self.filename = os.path.join(path, bname)

        self.description = "Cloud Type composite for Mesan"
        self.obstime = obstime
        self.timediff = tdiff
        self.time_window = (obstime - tdiff, obstime + tdiff)
        self.polar_satellites = eval(options['polar_satellites'])
        self.msg_satellites = eval(options['msg_satellites'])
        self.msg_areaname = eval(options['msg_areaname'])
        self.longitude = None
        self.latitude = None
        # An mpop-scene area object:
        self.area = None

        self._options = options

        self.pps_scenes = []
        self.msg_scenes = []

        self.composite = ncCloudTypeComposite()

    def get_catalogue(self):
        """Get the meta data (start-time, satellite, orbit number etc) for all
        available satellite scenes (both polar and geostationary) within the
        time window specified. For the time being this catalouge generation
        will be done by simple file globbing. In a later stage this will be
        done by doing a DB search"""
        from glob import glob

        # Get all polar satellite scenes:
        pps_dr_dir = self._options['pps_direct_readout_dir']
        pps_gds_dir = self._options['pps_metop_gds_dir']

        dr_list = glob(os.path.join(pps_dr_dir, '*cloudtype.h5'))
        ppsdr = get_ppslist(dr_list, self.time_window,
                            satellites=self.polar_satellites)

        now = datetime.utcnow()
        gds_list = glob(os.path.join(pps_gds_dir, '*cloudtype.h5'))
        print "Number of Metop GDS files in dir: " + str(len(gds_list))
        ppsgds = get_ppslist(gds_list, self.time_window,
                             satellites=METOPS, variant='global')
        tic = datetime.utcnow()
        print("Retrieve the metop-gds list took " +
              str((tic - now).seconds) + " sec")
        self.pps_scenes = ppsdr + ppsgds

        # Get all geostationary satellite scenes:
        msg_dir = self._options['msg_dir']
        ext = self._options['msg_cty_file_ext']
        # SAFNWC_MSG2_CT___201206252345_EuropeCanary.h5
        # What about EuropeCanary and possible other areas!? FIXME!
        msg_list = glob(os.path.join(msg_dir, '*_CT___*.PLAX.CTTH.0.h5'))
        self.msg_scenes = get_msglist(msg_list, self.time_window,
                                      self.msg_areaname,
                                      satellites=self.msg_satellites)

    def make_composite(self, areaid="mesanX"):
        """Make the Cloud Type composite"""

        # Reference time for time stamp in composite file
        #sec1970 = datetime(1970, 1, 1)
        import time

        comp_CT = []

        # Loop over all polar scenes:
        is_MSG = False
        # for scene in self.pps_scenes + self.msg_scenes:
        for scene in self.msg_scenes + self.pps_scenes:
            x_CT = None
            print scene
            if scene.platform == "meteosat" and not hasattr(scene, 'orbit'):
                is_MSG = True
                x_local = ctype_msg(scene)
                dummy, lat = x_local.area.get_lonlats()
                x_CT = x_local['CloudType_plax'].cloudtype
                # convert msg flags to pps
                x_flag = ctype_procflags2pps(
                    x_local['CloudType_plax'].processing_flags)
                x_id = 1 * np.ones(np.shape(x_CT))
            else:
                is_MSG = False

                x_local = ctype_pps(scene)
                x_CT = x_local['CloudType'].cloudtype.data
                x_flag = x_local['CloudType'].quality_flag.data
                x_id = 0 * np.ones(np.shape(x_CT))
                lat = 0 * np.ones(np.shape(x_CT))

            # time identifier is seconds since 1970-01-01 00:00:00
            x_time = time.mktime(scene.timeslot.timetuple()) * \
                np.ones(np.shape(x_CT))
            idx_MSG = is_MSG * np.ones(np.shape(x_CT), dtype=np.bool)
            if comp_CT == []:
                # initialize field with current CT
                comp_lon, comp_lat = x_local.area.get_lonlats()
                comp_CT = x_CT
                comp_flag = x_flag
                comp_time = x_time
                comp_id = x_id
                comp_w = wCT(
                    x_CT, x_flag, lat, abs(self.obstime - scene.timeslot), idx_MSG)
            else:
                # compare with quality of current CT
                x_w = wCT(
                    x_CT, x_flag, lat, abs(self.obstime - scene.timeslot), idx_MSG)

                # replace info where current CT data is best
                ii = x_w > comp_w
                comp_CT[ii] = x_CT[ii]
                comp_flag[ii] = x_flag[ii]
                comp_w[ii] = x_w[ii]
                comp_time[ii] = x_time[ii]
                comp_id[ii] = x_id[ii]

        self.longitude = comp_lon
        self.latitude = comp_lat

        self.area = x_local.area

        composite = {"cloudtype": comp_CT,
                     "flag": comp_flag,
                     "weight": comp_w,
                     "time": comp_time,
                     "id": comp_id.astype(np.uint8)}
        self.composite.store(composite, self.area)

    def write(self):
        """Write the composite to a netcdf file"""
        self.composite.write(self.filename + '.nc')

        return


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--datetime', '-d', help='Date and time of observation - yyyymmddhh',
                        required=True)
    parser.add_argument('--time_window', '-t', help='Number of minutes before and after time window',
                        required=True)
    parser.add_argument('--area_id', '-a', help='Area id',
                        required=True)
    parser.add_argument('--ctype_out', help='Mesan Cloudtype composite output',
                        required=False)
    parser.add_argument('--bagground_ctth', help='CTTH background...',
                        required=False)

    args = parser.parse_args()

    time_of_analysis = datetime.strptime(args.datetime, '%Y%m%d%H')
    delta_t = timedelta(minutes=int(args.time_window))

    ctcomp = ctCompositer(time_of_analysis, delta_t, args.area_id)
    ctcomp.get_catalogue()
    ctcomp.make_composite()
    ctcomp.write()
