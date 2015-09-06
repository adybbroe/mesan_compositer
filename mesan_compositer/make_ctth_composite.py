#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015 Adam.Dybbroe

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

"""Make a CTTH composite
"""


import argparse
from datetime import datetime, timedelta
import numpy as np

from mesan_compositer import (ProjectException, LoadException)
from mesan_compositer.pps_msg_conversions import ctth_procflags2pps
from nwcsaf_formats.pps_conversions import ctth_convert_flags


from mesan_compositer.netcdf_io import ncCTTHComposite

from mesan_compositer.composite_tools import (get_msglist,
                                              get_ppslist,
                                              get_weight_ctth)
import sys
import os

CFG_DIR = os.environ.get('MESAN_COMPOSITE_CONFIG_DIR', './')
MODE = os.environ.get("SMHI_MODE", 'offline')

METOPS = ['metop02', 'metop01']

SENSOR = {'NOAA-19': 'avhrr/3',
          'NOAA-18': 'avhrr/3',
          'NOAA-15': 'avhrr/3',
          'Metop-A': 'avhrr/3',
          'Metop-B': 'avhrr/3',
          'Metop-C': 'avhrr/3',
          'EOS-Terra': 'modis',
          'EOS-Aqua': 'modis',
          'Suomi-NPP': 'viirs',
          'JPSS-1': 'viirs'}

import logging
LOG = logging.getLogger(__name__)

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

import ConfigParser

conf = ConfigParser.ConfigParser()
configfile = os.path.join(CFG_DIR, "mesan_sat_config.cfg")
if not os.path.exists(configfile):
    raise IOError('Config file %s does not exist!' % configfile)
conf.read(configfile)

OPTIONS = {}
for option, value in conf.items(MODE, raw=True):
    OPTIONS[option] = value

_MESAN_LOG_FILE = OPTIONS.get('mesan_log_file', None)


def ctth_pps(pps, areaid='mesanX'):
    """Load PPS CTTH and reproject"""
    from mpop.satellites import PolarFactory
    global_data = PolarFactory.create_scene(pps.platform_name, "",
                                            SENSOR.get(
                                                pps.platform_name, 'avhrr'),
                                            pps.timeslot, pps.orbit)
    try:
        global_data.load(['CTTH'], filename=pps.uri,
                         geofilename=pps.geofilename)
    except AttributeError:
        raise LoadException('MPOP scene object fails to load!')
    if global_data.area or global_data['CTTH'].area:
        return global_data.project(areaid)
    else:
        raise ProjectException('MPOP Scene object has no area instance' +
                               ' and product cannot be projected')


def ctth_msg(msg, areaid='mesanX'):
    """Load MSG paralax corrected ctth and reproject"""
    from mpop.satellites import GeostationaryFactory

    global_geo = GeostationaryFactory.create_scene(msg.platform_name,
                                                   "", "seviri",
                                                   time_slot=msg.timeslot)
    global_geo.load(['CTTH'], filename=msg.uri)
    return global_geo.project(areaid)


class mesanComposite(object):

    """Master class for the Mesan cloud product composite generators"""

    def __init__(self, obstime, tdiff, areaid, **kwargs):

        self.description = "Unknown composite"
        self.obstime = obstime
        self.timediff = tdiff
        self.time_window = (obstime - tdiff, obstime + tdiff)
        LOG.debug("Time window: " + str(self.time_window[0]) +
                  " - " + str(self.time_window[1]))
        self.polar_satellites = []
        self.msg_satellites = []
        self.msg_areaname = 'unknown'
        self.longitude = None
        self.latitude = None
        # An mpop-scene area object:
        self.area = None

        self._options = {}

        self.pps_scenes = []
        self.msg_scenes = []

        self.product_names = {'msg': 'unknown', 'pps': 'unknown'}
        self.composite = None

    def get_catalogue(self, product):
        """Get the meta data (start-time, satellite, orbit number etc) for all
        available satellite scenes (both polar and geostationary) within the
        time window specified. For the time being this catalouge generation
        will be done by simple file globbing. In a later stage this will be
        done by doing a DB search.

        *product* can be either 'cloudtype' or 'ctth'

        """
        from glob import glob

        # Get all polar satellite scenes:
        pps_dr_dir = self._options.get('pps_direct_readout_dir', None)
        LOG.debug('pps_dr_dir = ' + str(pps_dr_dir))
        pps_gds_dir = self._options.get('pps_metop_gds_dir', None)
        prodn = self.product_names['pps']
        dr_list = glob(
            os.path.join(pps_dr_dir, 'S_NWC_' + str(prodn) + '*.nc'))
        ppsdr = get_ppslist(dr_list, self.time_window, product=prodn,
                            satellites=self.polar_satellites)

        now = datetime.utcnow()
        gds_list = glob(os.path.join(pps_gds_dir, '*' + str(prodn) + '*.h5'))
        LOG.info("Number of Metop GDS files in dir: " + str(len(gds_list)))
        ppsgds = get_ppslist(gds_list, self.time_window,
                             satellites=METOPS, variant='global')
        tic = datetime.utcnow()
        LOG.info("Retrieve the metop-gds list took " +
                 str((tic - now).seconds) + " sec")
        self.pps_scenes = ppsdr + ppsgds

        # Get all geostationary satellite scenes:
        msg_dir = self._options['msg_dir']
        if product == 'cloudtype':
            ext = self._options['msg_cty_file_ext']
            # SAFNWC_MSG2_CT___201206252345_EuropeCanary.h5
        elif product == 'ctth':
            ext = self._options['msg_ctth_file_ext']
            # SAFNWC_MSG2_CTTH_201206252345_EuropeCanary.h5

        # What about EuropeCanary and possible other areas!? FIXME!
        prodn = self.product_names['msg']
        msg_list = glob(
            os.path.join(msg_dir, '*_' + str(prodn) + '*' + str(ext)))
        self.msg_scenes = get_msglist(msg_list, self.time_window,
                                      self.msg_areaname,
                                      satellites=self.msg_satellites)


class ctthComposite(mesanComposite):

    """The CTTH Composite generator class"""

    def __init__(self, obstime, tdiff, areaid, **kwargs):
        super(ctthComposite, self).__init__(obstime, tdiff, areaid, **kwargs)

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
            bname = obstime.strftime(
                options['ctth_composite_filename']) % values
            path = options['composite_output_dir']
            self.filename = os.path.join(path, bname)

        self.description = "Cloud Top Temperature and Height composite for Mesan"

        self._options = options

        self.pps_scenes = []
        self.msg_scenes = []

        self.polar_satellites = options['polar_satellites'].split(',')
        self.msg_satellites = options['msg_satellites'].split(',')
        self.msg_areaname = options['msg_areaname']

        self.product_names = {'msg': 'CTTH', 'pps': 'CTTH'}
        self.composite = ncCTTHComposite()

    def get_catalogue(self, product='ctth'):
        super(ctthComposite, self).get_catalogue(product)

    def make_composite(self):
        """Make the Cloud Type composite"""

        # Reference time for time stamp in composite file
        #sec1970 = datetime(1970, 1, 1)
        import time

        comp_temperature = []
        comp_height = []
        comp_pressure = []

        # Loop over all polar scenes:
        is_MSG = False
        for scene in self.msg_scenes + self.pps_scenes:
            LOG.info("Scene:\n" + str(scene))
            if (scene.platform_name.startswith("Meteosat") and
                    not hasattr(scene, 'orbit')):
                is_MSG = True
                x_local = ctth_msg(scene)
                dummy, lat = x_local.area.get_lonlats()
                x_temperature = x_local['CTTH'].temperature
                x_pressure = x_local['CTTH'].pressure
                x_height = x_local['CTTH'].height

                # convert msg flags to pps
                # fill_value = 0, fill with 65535 (same as pps flag fill value)
                # so that bit 0 is set -> unprocessed -> w=0
                # The weight for masked data is set further down
                x_flag = np.ma.filled(ctth_procflags2pps(x_local['CTTH'].processing_flags),
                                      fill_value=65535)
                x_id = 1 * np.ones(np.shape(x_temperature))
            else:
                is_MSG = False
                try:
                    x_local = ctth_pps(scene)
                except (ProjectException, LoadException) as err:
                    LOG.warning("Couldn't load pps scene:\n" + str(scene))
                    LOG.warning("Exception was: " + str(err))
                    continue

                # Temperature (K)', u'no_data_value': 255, u'intercept': 100.0,
                # u'gain': 1.0
                x_temperature = (x_local['CTTH'].ctth_tempe.data *
                                 x_local['CTTH'].ctth_tempe.info['scale_factor'][0] +
                                 x_local['CTTH'].ctth_tempe.info['add_offset'][0])

                # Pressure (hPa)', u'no_data_value': 255, u'intercept': 0.0,
                # u'gain': 25.0
                x_pressure = (x_local['CTTH'].ctth_pres.data * x_local[
                    'CTTH'].ctth_pres.info['scale_factor'][0] +
                    x_local['CTTH'].ctth_pres.info['add_offset'][0])
                # Height (m)', u'no_data_value': 255, u'intercept': 0.0,
                # u'gain': 200.0
                x_height = (x_local['CTTH'].ctth_alti.data * x_local[
                    'CTTH'].ctth_alti.info['scale_factor'][0] +
                    x_local['CTTH'].ctth_alti.info['add_offset'][0])

                sflags = x_local['CTTH'].ctth_status_flag.data.filled(0)
                cflags = x_local['CTTH'].ctth_conditions.data.filled(0)
                qflags = x_local['CTTH'].ctth_quality.data.filled(0)
                oldflags = ctth_convert_flags(sflags, cflags, qflags)

                # fill_value = 65535 i.e bit 0 is set -> unprocessed -> w=0
                x_flag = np.ma.filled(oldflags, fill_value=65535)

                x_id = 0 * np.ones(np.shape(x_temperature))
                lat = 0 * np.ones(np.shape(x_temperature))

            # time identifier is seconds since 1970-01-01 00:00:00
            x_time = time.mktime(scene.timeslot.timetuple()) * \
                np.ones(np.shape(x_temperature))
            idx_MSG = is_MSG * np.ones(np.shape(x_temperature), dtype=np.bool)

            if comp_temperature == []:
                # initialize field with current CTTH
                comp_lon, comp_lat = x_local.area.get_lonlats()
                comp_temperature = x_temperature
                comp_pressure = x_pressure
                comp_height = x_height
                comp_flag = x_flag
                comp_time = x_time
                comp_id = x_id
                comp_w = get_weight_ctth(x_flag, lat,
                                         abs(self.obstime - scene.timeslot),
                                         idx_MSG)
                # fix to cope with unprocessed data
                ii = (x_height.mask == True) | (x_height == 0)
                comp_w[ii] = 0
            else:
                # compare with quality of current CTTH
                x_w = get_weight_ctth(x_flag, lat,
                                      abs(self.obstime - scene.timeslot),
                                      idx_MSG)
                # fix to cope with unprocessed data
                ii = (x_height.mask == True) | (x_height == 0)
                x_w[ii] = 0

                # replace info where current CTTH data is best
                ii = x_w > comp_w
                comp_temperature[ii] = x_temperature[ii]
                comp_pressure[ii] = x_pressure[ii]
                comp_height[ii] = x_height[ii]
                comp_flag[ii] = x_flag[ii]
                comp_w[ii] = x_w[ii]
                comp_time[ii] = x_time[ii]
                comp_id[ii] = x_id[ii]

        self.longitude = comp_lon
        self.latitude = comp_lat
        self.area = x_local.area

        composite = {"temperature": comp_temperature,
                     "height": comp_height,
                     "pressure": comp_pressure,
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

    from logging import handlers

    if _MESAN_LOG_FILE:
        ndays = int(OPTIONS["log_rotation_days"])
        ncount = int(OPTIONS["log_rotation_backup"])
        handler = handlers.TimedRotatingFileHandler(_MESAN_LOG_FILE,
                                                    when='midnight',
                                                    interval=ndays,
                                                    backupCount=ncount,
                                                    encoding=None,
                                                    delay=False,
                                                    utc=True)
    else:
        handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('mpop').setLevel(logging.DEBUG)

    LOG = logging.getLogger('make_ctth_composite')

    time_of_analysis = datetime.strptime(args.datetime, '%Y%m%d%H')
    delta_t = timedelta(minutes=int(args.time_window))

    ctth_comp = ctthComposite(time_of_analysis, delta_t, args.area_id)
    ctth_comp.get_catalogue()
    ctth_comp.make_composite()
    ctth_comp.write()
