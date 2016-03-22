#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015, 2016 Adam.Dybbroe

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
import tempfile
import shutil

from mpop.satin.msg_hdf import ctype_procflags2pps

from mesan_compositer import (ProjectException, LoadException)
from mesan_compositer.composite_tools import (get_msglist,
                                              get_ppslist,
                                              get_weight_cloudtype)
from mesan_compositer.netcdf_io import ncCloudTypeComposite

from nwcsaf_formats.pps_conversions import (map_cloudtypes,
                                            ctype_convert_flags)


import sys
import os

CFG_DIR = os.environ.get('MESAN_COMPOSITE_CONFIG_DIR', './')
DIST = os.environ.get("SMHI_DIST", 'elin4')
if not DIST or DIST == 'linda4':
    MODE = 'offline'
else:
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

PLATFORM_NAMES_FROM_PPS = {}

import logging
LOG = logging.getLogger(__name__)

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

import ConfigParser

CONF = ConfigParser.ConfigParser()
CONFIGFILE = os.path.join(CFG_DIR, "mesan_sat_config.cfg")
if not os.path.exists(CONFIGFILE):
    raise IOError('Config file %s does not exist!' % CONFIGFILE)
CONF.read(CONFIGFILE)

OPTIONS = {}
for opt, val in CONF.items(MODE, raw=True):
    OPTIONS[opt] = val

MIN_NUM_OF_PPS_DR_FILES = int(OPTIONS.get('min_num_of_pps_dr_files', '0'))

_MESAN_LOG_FILE = OPTIONS.get('mesan_log_file', None)


def ctype_pps(pps, areaid):
    """Load PPS Cloudtype and reproject"""
    from mpop.satellites import PolarFactory
    global_data = PolarFactory.create_scene(pps.platform_name, '',
                                            SENSOR.get(
                                                pps.platform_name, 'avhrr/3'),
                                            pps.timeslot, pps.orbit,
                                            variant='DR')

    try:
        global_data.load(['CT'], filename=pps.uri)
    except AttributeError:
        raise LoadException('MPOP scene object fails to load!')

    if global_data.area or global_data['CT'].area:
        return global_data.project(areaid)
    else:
        raise ProjectException('MPOP Scene object has no area instance' +
                               ' and product cannot be projected')


def ctype_msg(msg, areaid):
    """Load MSG paralax corrected cloud type and reproject"""
    from mpop.satellites import GeostationaryFactory

    global_geo = GeostationaryFactory.create_scene(msg.platform_name, '',
                                                   "seviri",
                                                   time_slot=msg.timeslot)
    global_geo.load(['CloudType_plax'], filename=msg.uri)
    return global_geo.project(areaid)


class ctCompositer(object):

    """The Cloud Type Composite generator class"""

    def __init__(self, obstime, tdiff, areaid, **kwargs):

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
            LOG.info("Output file name is generated from observation " +
                     "time and info in config file:")
            bname = obstime.strftime(options['ct_composite_filename']) % values
            path = options['composite_output_dir']
            self.filename = os.path.join(path, bname)

        LOG.info('Filename = ' + str(self.filename))

        self.description = "Cloud Type composite for Mesan"
        self.obstime = obstime
        self.timediff = tdiff
        self.time_window = (obstime - tdiff, obstime + tdiff)
        LOG.debug("Time window: " + str(self.time_window[0]) +
                  " - " + str(self.time_window[1]))
        self.polar_satellites = options['polar_satellites'].split()
        self.msg_satellites = options['msg_satellites'].split()
        self.msg_areaname = options['msg_areaname']
        self.areaid = areaid
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
        LOG.debug('pps_dr_dir = ' + str(pps_dr_dir))
        pps_gds_dir = self._options['pps_metop_gds_dir']

        # Example: S_NWC_CT_metopb_14320_20150622T1642261Z_20150622T1654354Z.nc
        dr_list = glob(os.path.join(pps_dr_dir, 'S_NWC_CT_*nc'))
        LOG.info("Number of direct readout pps cloudtype files in dir: " +
                 str(len(dr_list)))
        if len(dr_list) <= MIN_NUM_OF_PPS_DR_FILES:
            LOG.critical("Too few PPS DR files found! (%d<=%d)\n" +
                         "pps_dr_dir = %s",
                         len(dr_list), MIN_NUM_OF_PPS_DR_FILES,
                         str(pps_dr_dir))
        ppsdr = get_ppslist(dr_list, self.time_window,
                            satellites=self.polar_satellites)

        now = datetime.utcnow()
        gds_list = glob(os.path.join(pps_gds_dir, 'S_NWC_CT_*nc'))
        LOG.info("Number of Metop GDS files in dir: " + str(len(gds_list)))
        ppsgds = []
        if len(gds_list) > 0:
            ppsgds = get_ppslist(gds_list, self.time_window,
                                 satellites=METOPS, variant='global')
            tic = datetime.utcnow()
            LOG.info("Retrieve the metop-gds list took " +
                     str((tic - now).seconds) + " sec")

        self.pps_scenes = ppsdr + ppsgds
        LOG.info(str(len(self.pps_scenes)) + " PPS scenes located")
        for scene in self.pps_scenes:
            LOG.debug("Polar scene:\n" + str(scene))

        # Get all geostationary satellite scenes:
        msg_dir = self._options['msg_dir'] % {"number": "02"}
        #ext = self._options['msg_cty_file_ext']
        # SAFNWC_MSG2_CT___201206252345_EuropeCanary.h5
        # What about EuropeCanary and possible other areas!? FIXME!
        msg_list = glob(os.path.join(msg_dir, '*_CT___*.PLAX.CTTH.0.h5'))
        LOG.debug(
            "MSG files in directory " + str(msg_dir) + " : " + str(msg_list))
        LOG.info("Get files inside time window: " +
                 str(self.time_window[0]) + " - " +
                 str(self.time_window[1]))
        self.msg_scenes = get_msglist(msg_list, self.time_window,
                                      self.msg_areaname)  # satellites=self.msg_satellites)
        LOG.info(str(len(self.msg_scenes)) + " MSG scenes located")
        for scene in self.msg_scenes:
            LOG.debug("Geo scene:\n" + str(scene))

    def make_composite(self):
        """Make the Cloud Type composite"""

        # Reference time for time stamp in composite file
        #sec1970 = datetime(1970, 1, 1)
        import time

        comp_CT = []

        # Loop over all polar and geostationary satellite scenes:
        is_MSG = False

        if len(self.msg_scenes + self.pps_scenes) == 0:
            LOG.error("Cannot make composite when no Scenes have been found!")
            return False

        # for scene in self.pps_scenes + self.msg_scenes:
        LOG.info("Loop over all polar and geostationary scenes:")
        for scene in self.msg_scenes + self.pps_scenes:
            x_CT = None
            LOG.info("Scene:\n" + str(scene))
            if (scene.platform_name.startswith("Meteosat") and
                    not hasattr(scene, 'orbit')):
                is_MSG = True
                x_local = ctype_msg(scene, self.areaid)
                dummy, lat = x_local.area.get_lonlats()
                x_CT = x_local['CloudType_plax'].cloudtype
                # convert msg flags to pps
                x_flag = ctype_procflags2pps(
                    x_local['CloudType_plax'].processing_flags)
                x_id = 1 * np.ones(np.shape(x_CT))
            else:
                is_MSG = False
                try:
                    x_local = ctype_pps(scene, self.areaid)
                except (ProjectException, LoadException) as err:
                    LOG.warning("Couldn't load pps scene:\n" + str(scene))
                    LOG.warning("Exception was: " + str(err))
                    continue

                #x_CT = x_local['CT'].ct.data
                #x_flag = x_local['CT'].ct_quality.data
                # Convert to old format:
                x_CT = map_cloudtypes(x_local['CT'].ct.data.filled(0))
                sflags = x_local['CT'].ct_status_flag.data.filled(0)
                cflags = x_local['CT'].ct_conditions.data.filled(0)
                qflags = x_local['CT'].ct_quality.data.filled(0)
                x_flag = ctype_convert_flags(sflags, cflags, qflags)
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
                comp_w = get_weight_cloudtype(
                    x_CT, x_flag, lat, abs(self.obstime - scene.timeslot), idx_MSG)
            else:
                # compare with quality of current CT
                x_w = get_weight_cloudtype(
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

        return True

    def write(self):
        """Write the composite to a netcdf file"""
        tmpfname = tempfile.mktemp(suffix=os.path.basename(self.filename),
                                   dir=os.path.dirname(self.filename))
        #self.composite.write(self.filename + '.nc')
        self.composite.write(tmpfname)
        now = datetime.utcnow()
        fname_with_timestamp = str(
            self.filename) + now.strftime('_%Y%m%d%H%M%S.nc')
        shutil.copy(tmpfname, fname_with_timestamp)
        os.rename(tmpfname, self.filename + '.nc')

    def make_quicklooks(self):
        """Make quicklook images"""

        import mpop.imageo.palettes
        palette = mpop.imageo.palettes.cms_modified()
        from mpop.imageo import geo_image

        img = geo_image.GeoImage(self.composite.cloudtype.data,
                                 self.areaid,
                                 None,
                                 fill_value=(0),
                                 mode="P",
                                 palette=palette)
        img.save(self.filename.strip('.nc') + '_cloudtype.png')

        comp_id = self.composite.id.data * 13
        idimg = geo_image.GeoImage(comp_id,
                                   self.areaid,
                                   None,
                                   fill_value=(0),
                                   mode="P",
                                   palette=palette)
        idimg.save(self.filename.strip('.nc') + '_id.png')

        comp_w = self.composite.weight.data * 20
        wimg = geo_image.GeoImage(comp_w,
                                  self.areaid,
                                  None,
                                  fill_value=(0),
                                  mode="P",
                                  palette=palette)
        wimg.save(self.filename.strip('.nc') + '_weight.png')


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

        # handler.doRollover()
    else:
        handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('mpop').setLevel(logging.DEBUG)

    LOG = logging.getLogger('make_ct_composite')

    time_of_analysis = datetime.strptime(args.datetime, '%Y%m%d%H')
    delta_t = timedelta(minutes=int(args.time_window))

    ctcomp = ctCompositer(time_of_analysis, delta_t, args.area_id)
    ctcomp.get_catalogue()
    ctcomp.make_composite()
    ctcomp.write()
    ctcomp.make_quicklooks()
