#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014-2023 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

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

"""Make a Cloud Type composite."""

import argparse
from datetime import datetime, timedelta
from glob import glob
import numpy as np
import tempfile
import shutil
from mesan_compositer.pps_msg_conversions import ctype_procflags2pps
from mesan_compositer import (ProjectException, LoadException)
from mesan_compositer.composite_tools import (get_msglist,
                                              get_ppslist,
                                              get_weight_cloudtype)
from mesan_compositer.netcdf_io import ncCloudTypeComposite
from nwcsaf_formats.pps_conversions import (map_cloudtypes,
                                            ctype_convert_flags)
from mesan_compositer.ct_quicklooks import make_quicklooks

from mesan_compositer.composite_tools import METOPS
from mesan_compositer.config import get_config
import sys
import os
import logging
from logging import handlers

LOG = logging.getLogger(__name__)


PLATFORM_NAMES_FROM_PPS = {}

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'


def get_arguments():
    """
    Get command line arguments.

    args.logging_conf_file, args.config_file, obs_time, area_id, wsize

    Return
      File path of the logging.ini file
      File path of the application configuration file
      Observation/Analysis time
      Area id
      Window size

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--datetime', '-d', help='Date and time of observation - yyyymmddhh',
                        required=True)
    parser.add_argument('--time_window', '-t', help='Number of minutes before and after time window',
                        required=True)
    parser.add_argument('--area_id', '-a', help='Area id',
                        required=True)
    parser.add_argument('-c', '--config_file',
                        type=str,
                        dest='config_file',
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
    parser.add_argument('--ctype_out', help='Mesan Cloudtype composite output',
                        required=False)
    parser.add_argument('--background_ctth', help='CTTH background...',
                        required=False)

    args = parser.parse_args()

    tanalysis = datetime.strptime(args.datetime, '%Y%m%d%H')
    delta_t = timedelta(minutes=int(args.time_window))
    if 'template' in args.config_file:
        print("Template file given as master config, aborting!")
        sys.exit()

    return args.logging_conf_file, args.config_file, tanalysis, args.area_id, delta_t


def ctype_pps(pps, areaid):
    """Load PPS Cloudtype and reproject."""
    from satpy.scene import Scene

    scene = Scene(filenames=[pps.uri, pps.geofilename], reader='nwcsaf-pps_nc')
    scene.load(['cloudtype', 'ct', 'ct_quality', 'ct_status_flag', 'ct_conditions'])
    retv = scene.resample(areaid, radius_of_influence=8000)

    return retv


def ctype_msg(msg, areaid):
    """Load MSG paralax corrected cloud type and reproject."""
    from satpy.scene import Scene

    scene = Scene(filenames=[msg.uri, ], reader='nwcsaf-msg2013-hdf5')
    scene.load(['cloudtype', 'ct', 'ct_quality'])
    retv = scene.resample(areaid, radius_of_influence=20000)

    return retv


class ctCompositer(object):
    """The Cloud Type Composite generator class."""

    def __init__(self, obstime, tdiff, areaid, config_options, **kwargs):
        """Initialize the cloud type composite instance."""
        values = {"area": areaid, }

        if 'filename' in kwargs:
            self.filename = kwargs['filename']
        else:
            # Generate the filename from the observation time and the
            # specifcations in the config file:
            LOG.info("Output file name is generated from observation " +
                     "time and info in config file:")
            bname = obstime.strftime(config_options['ct_composite_filename']) % values
            path = config_options['composite_output_dir']
            self.filename = os.path.join(path, bname)

        LOG.info('Filename = ' + str(self.filename))

        self.description = "Cloud Type composite for Mesan"
        self.obstime = obstime
        self.timediff = tdiff
        self.time_window = (obstime - tdiff, obstime + tdiff)
        LOG.debug("Time window: " + str(self.time_window[0]) +
                  " - " + str(self.time_window[1]))
        self.polar_satellites = config_options['polar_satellites'].split()
        LOG.debug("Polar satellites supported: %s", str(self.polar_satellites))

        self.msg_satellites = config_options['msg_satellites'].split()
        self.msg_areaname = config_options['msg_areaname']
        self.areaid = areaid
        self.longitude = None
        self.latitude = None
        # A Satpy-scene area object:
        self.area = None

        self._options = config_options

        self.pps_scenes = []
        self.msg_scenes = []

        self.composite = ncCloudTypeComposite()

    def _get_all_pps_files(self, pps_dir):
        """Return list of pps files in directory."""
        if pps_dir is None:
            return []

        LOG.debug('Get all PPS files in this directory = ' + str(pps_dir))
        # Example: S_NWC_CT_metopb_14320_20150622T1642261Z_20150622T1654354Z.nc
        return glob(os.path.join(pps_dir, 'S_NWC_CT_*nc'))

    def _get_all_geo_files(self, geo_dir):
        """Return list of NWCSAF/Geo files in directory."""
        if geo_dir is None:
            return []

        LOG.debug('Get all NWCSAF/Geo files in this directory = ' + str(geo_dir))
        return glob(os.path.join(geo_dir, '*_CT___*.PLAX.CTTH.0.h5'))

    def get_pps_scenes(self, pps_file_list, satellites=None, variant=None):
        """Get the list of valid pps scenes from file list."""
        if satellites is None:
            satellites = self.polar_satellites
        return get_ppslist(pps_file_list, self.time_window,
                           satellites=satellites, variant=variant)

    def get_geo_scenes(self, geo_file_list):
        """Get the list of valid NWCSAF/Geo scenes from file list."""
        return get_msglist(geo_file_list, self.time_window, self.msg_areaname)  # satellites=self.msg_satellites)

    def get_catalogue(self):
        """Get the catalougue of input data files.

        Get the meta data (start-time, satellite, orbit number etc) for all
        available satellite scenes (both polar and geostationary) within the
        time window specified. For the time being this catalouge generation
        will be done by simple file globbing. In the future this might be
        done by doing a DB search.
        """
        min_num_of_pps_dr_files = int(self._options.get('min_num_of_pps_dr_files', '0'))

        # Get all polar satellite scenes:
        pps_dr_dir = self._options['pps_direct_readout_dir']
        dr_list = self._get_all_pps_files(pps_dr_dir)
        LOG.info("Number of direct readout pps cloudtype files in dir: %s", str(len(dr_list)))

        if len(dr_list) <= min_num_of_pps_dr_files:
            LOG.critical("Too few PPS DR files found! (%d<=%d)\n" +
                         "pps_dr_dir = %s",
                         len(dr_list), min_num_of_pps_dr_files,
                         str(pps_dr_dir))

        ppsdr = self.get_pps_scenes(dr_list)

        ppsgds = []
        gds_list = self._get_all_pps_files(self._options.get('pps_metop_gds_dir'))
        if len(gds_list) > 0:
            now = datetime.utcnow()
            LOG.info("Number of Metop GDS files in dir: " + str(len(gds_list)))
            ppsgds = self.get_pps_scenes(gds_list, satellites=METOPS, variant='global')
            tic = datetime.utcnow()
            LOG.info("Retrieve the metop-gds list took " +
                     str((tic - now).seconds) + " sec")

        self.pps_scenes = ppsdr + ppsgds
        self.pps_scenes.sort()
        LOG.info(str(len(self.pps_scenes)) + " PPS scenes located")
        for scene in self.pps_scenes:
            LOG.debug("Polar scene:\n" + str(scene))

        # Get all geostationary satellite scenes:
        msg_dir = self._options['msg_dir'] % {"number": "02"}
        # What about EuropeCanary and possible other areas!? FIXME!
        msg_list = self._get_all_geo_files(msg_dir)
        LOG.debug(
            "MSG files in directory " + str(msg_dir) + " : " + str(msg_list))
        LOG.info("Get files inside time window: " +
                 str(self.time_window[0]) + " - " +
                 str(self.time_window[1]))

        self.msg_scenes = self.get_geo_scenes(msg_list)
        self.msg_scenes.sort()
        LOG.info(str(len(self.msg_scenes)) + " MSG scenes located")
        for scene in self.msg_scenes:
            LOG.debug("Geo scene:\n" + str(scene))

    def blend_ct_products(self):
        """Blend the CT products together and create cloud analysis."""
        return

    def make_composite(self):
        """Make the Cloud Type composite."""
        # Reference time for time stamp in composite file
        # sec1970 = datetime(1970, 1, 1)
        import time

        comp_CT = None

        if len(self.msg_scenes + self.pps_scenes) == 0:
            LOG.error(
                "Cannot make ct composite when no Scenes have been found!")
            return False

        # Loop over all polar and geostationary satellite scenes:
        is_MSG = False
        LOG.info("Loop over all polar and geostationary scenes:")
        # msgscenes = [self.msg_scenes[0], self.msg_scenes[2], self.msg_scenes[1]]
        # msgscenes = [self.msg_scenes[2], self.msg_scenes[1], self.msg_scenes[0]]
        # msgscenes = [self.msg_scenes[1], self.msg_scenes[0], self.msg_scenes[2]]
        # msgscenes = [self.msg_scenes[1], self.msg_scenes[2], self.msg_scenes[0]]
        # for scene in msgscenes + self.pps_scenes:

        # Go through the list of msg-scenes and find the one closest to the
        # obs-time, and put in front. Also revert the list. All this is to make
        # the code work as it did when the system tests were generated:
        tdelta = timedelta(seconds=9999)
        myindex = 0
        for idx, scene in enumerate(self.msg_scenes):
            if abs(scene.timeslot - self.obstime) < tdelta:
                tdelta = abs(scene.timeslot - self.obstime)
                myindex = idx
        msgscenes = self.msg_scenes[::-1]
        scene = self.msg_scenes[myindex]
        msgscenes.remove(scene)
        msgscenes.insert(0, scene)

        for scene in msgscenes + self.pps_scenes:
            x_CT = None
            LOG.info("Scene:\n" + str(scene))
            if (scene.platform_name.startswith("Meteosat") and
                    not hasattr(scene, 'orbit')):
                is_MSG = True
                x_local = ctype_msg(scene, self.areaid)
                dummy, lat = x_local['ct'].area.get_lonlats()
                x_CT = x_local['ct'].data.compute()

                # convert msg flags to pps
                x_flag = ctype_procflags2pps(x_local['ct_quality'].data.compute())
                x_id = 1 * np.ones(x_CT.shape)
            else:
                is_MSG = False
                try:
                    x_local = ctype_pps(scene, self.areaid)
                except (ProjectException, LoadException) as err:
                    LOG.warning("Couldn't load pps scene:\n" + str(scene))
                    LOG.warning("Exception was: " + str(err))
                    continue

                # x_CT = x_local['CT'].ct.data
                # x_flag = x_local['CT'].ct_quality.data
                # Convert to old format:
                x_CT = map_cloudtypes(x_local['ct'].data)
                sflags = x_local['ct_status_flag']
                cflags = x_local['ct_conditions']
                qflags = x_local['ct_quality']
                x_flag = ctype_convert_flags(sflags, cflags, qflags)
                x_id = 0 * np.ones(x_CT.shape)
                lat = 0 * np.ones(x_CT.shape)

            # time identifier is seconds since 1970-01-01 00:00:00
            x_time = time.mktime(scene.timeslot.timetuple()) * np.ones(x_CT.shape)
            idx_MSG = is_MSG * np.ones(x_CT.shape, dtype=bool)
            if comp_CT is None:
                # initialize field with current CT
                comp_lon, comp_lat = x_local['ct'].area.get_lonlats()
                comp_CT = x_CT
                comp_flag = x_flag
                comp_time = x_time
                comp_id = x_id
                comp_w = get_weight_cloudtype(
                    x_CT, x_flag, lat, abs(self.obstime - scene.timeslot), idx_MSG, fill_value=255)
            else:
                # compare with quality of current CT
                x_w = get_weight_cloudtype(
                    x_CT, x_flag, lat, abs(self.obstime - scene.timeslot), idx_MSG, fill_value=255)

                # replace info where current CT data is best
                ii = x_w > comp_w

                comp_CT = np.where(ii, x_CT, comp_CT)
                comp_flag = np.where(ii, x_flag, comp_flag)
                comp_w = np.where(ii, x_w, comp_w)
                comp_time = np.where(ii, x_time, comp_time)
                comp_id = np.where(ii, x_id, comp_id)

        self.longitude = comp_lon
        self.latitude = comp_lat

        self.area = x_local['ct'].area

        composite = {"cloudtype": comp_CT,
                     "flag": comp_flag,
                     "weight": comp_w,
                     "time": comp_time,
                     "id": comp_id.astype(np.uint8)}
        self.composite.store(composite, self.area)

        return True

    def write(self):
        """Write the composite to a netcdf file."""
        tmpfname = tempfile.mktemp(suffix=os.path.basename(self.filename),
                                   dir=os.path.dirname(self.filename))
        self.composite.write(tmpfname)
        now = datetime.utcnow()
        fname_with_timestamp = str(
            self.filename) + now.strftime('_%Y%m%d%H%M%S.nc')
        shutil.copy(tmpfname, fname_with_timestamp)
        os.rename(tmpfname, self.filename + '.nc')

        return

    def make_quicklooks(self):
        """Make quicklook images."""
        make_quicklooks(self.filename, self.composite.cloudtype,
                        self.composite.id, self.composite.weight)


if __name__ == "__main__":

    (logfile, config_filename, time_of_analysis, area_id, delta_time_window) = get_arguments()

    handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    # logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('satpy').setLevel(logging.DEBUG)

    LOG = logging.getLogger('make_ct_composite')

    log_handlers = logging.getLogger('').handlers
    for log_handle in log_handlers:
        if type(log_handle) is handlers.SMTPHandler:
            LOG.debug("Mail notifications to: %s", str(log_handle.toaddrs))

    OPTIONS = get_config(config_filename)

    ctcomp = ctCompositer(time_of_analysis, delta_time_window, area_id, OPTIONS)
    ctcomp.get_catalogue()
    ctcomp.blend_ct_products()

    # ctcomp.make_composite()

    # Just for testing purposes:
    # values = {"area": area_id, }
    # iparam = 71
    # window_size = 24
    # IPAR = str(iparam)
    # NPIX = int(window_size)

    # bname = time_of_analysis.strftime(OPTIONS['cloudamount_filename']) % values
    # path = OPTIONS['composite_output_dir']
    # filename = os.path.join(path, bname + '.dat')

    # from mesan_compositer.prt_nwcsaf_cloudamount import derive_sobs
    # derive_sobs(ctcomp.composite, IPAR, NPIX, filename)

    ctcomp.write()
    ctcomp.make_quicklooks()
