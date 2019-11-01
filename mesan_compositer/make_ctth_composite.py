#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 - 2019 Adam.Dybbroe

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

"""Make a CTTH composite
"""


import argparse
from datetime import datetime, timedelta
import numpy as np
import xarray as xr

from trollimage.xrimage import XRImage
from mesan_compositer import ctth_height
from satpy.composites import PaletteCompositor

from mesan_compositer import (ProjectException, LoadException)
from mesan_compositer.pps_msg_conversions import ctth_procflags2pps
from nwcsaf_formats.pps_conversions import ctth_convert_flags
from mesan_compositer.composite_tools import METOPS
from mesan_compositer.netcdf_io import ncCTTHComposite
from mesan_compositer.netcdf_io import get_nc_attributes_from_object

from mesan_compositer import get_config

from mesan_compositer.composite_tools import (get_msglist,
                                              get_ppslist,
                                              get_weight_ctth)
import sys
import os
import tempfile
import shutil
from logging import handlers
import logging

LOG = logging.getLogger(__name__)

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'


def get_arguments():
    """
    Get command line arguments

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

    args = parser.parse_args()

    tanalysis = datetime.strptime(args.datetime, '%Y%m%d%H')
    delta_t = timedelta(minutes=int(args.time_window))
    area_id = args.area_id
    if 'template' in args.config_file:
        print("Template file given as master config, aborting!")
        sys.exit()

    return args.logging_conf_file, args.config_file, tanalysis, area_id, delta_t


def ctth_pps(pps, areaid):
    """Load PPS CTTH and reproject"""

    from satpy.scene import Scene

    scene = Scene(filenames=[pps.uri, pps.geofilename], reader='nwcsaf-pps_nc')
    scene.load(['ctth_alti', 'ctth_pres', 'ctth_tempe', 'ctth_quality',
                'ctth_conditions', 'ctth_status_flag'])

    retv = scene.resample(areaid, radius_of_influence=5000)
    return retv


def ctth_msg(msg, areaid):
    """Load MSG paralax corrected ctth and reproject"""

    from satpy.scene import Scene

    scene = Scene(filenames=[msg.uri, ], reader='nwcsaf-msg2013-hdf5')
    scene.load(['ctth_alti', 'ctth_pres', 'ctth_tempe', 'ctth_quality', 'ctth_effective_cloudiness'])

    retv = scene.resample(areaid, radius_of_influence=20000)
    return retv


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
        # A Satpy-scene area object:
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
        pps_gds_dir = self._options.get('pps_metop_gds_dir')
        prodn = self.product_names['pps']
        dr_list = glob(
            os.path.join(pps_dr_dir, 'S_NWC_' + str(prodn) + '*.nc'))
        ppsdr = get_ppslist(dr_list, self.time_window,
                            satellites=self.polar_satellites)

        ppsgds = []
        if pps_gds_dir:
            now = datetime.utcnow()
            gds_list = glob(os.path.join(pps_gds_dir, '*' + str(prodn) + '*.nc'))
            if len(gds_list) > 0:
                LOG.info("Number of Metop GDS files in dir: " + str(len(gds_list)))
                ppsgds = get_ppslist(gds_list, self.time_window,
                                     satellites=METOPS, variant='global')
                tic = datetime.utcnow()
                LOG.info("Retrieve the metop-gds list took " +
                         str((tic - now).seconds) + " sec")

        self.pps_scenes = ppsdr + ppsgds
        self.pps_scenes.sort()
        LOG.info(str(len(self.pps_scenes)) + " Polar scenes located")
        for scene in self.pps_scenes:
            LOG.debug("Polar scene:\n" + str(scene))

        # Get all geostationary satellite scenes:
        msg_dir = self._options['msg_dir'] % {"number": "03"}
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
                                      self.msg_areaname)  # satellites=self.msg_satellites)
        self.msg_scenes.sort()
        LOG.info(str(len(self.msg_scenes)) + " MSG scenes located")
        for scene in self.msg_scenes:
            LOG.debug("Geo scene:\n" + str(scene))


class ctthComposite(mesanComposite):

    """The CTTH Composite generator class"""

    def __init__(self, obstime, tdiff, areaid, config_options, **kwargs):
        super(ctthComposite, self).__init__(obstime, tdiff, areaid,  **kwargs)

        values = {"area": areaid, }

        if 'filename' in kwargs:
            self.filename = kwargs['filename']
        else:
            # Generate the filename from the observation time and the
            # specifcations in the config file:
            bname = obstime.strftime(
                config_options['ctth_composite_filename']) % values
            path = config_options['composite_output_dir']
            self.filename = os.path.join(path, bname)

        self.description = "Cloud Top Temperature and Height composite for Mesan"

        self._options = config_options

        self.pps_scenes = []
        self.msg_scenes = []

        self.polar_satellites = config_options['polar_satellites'].split()
        self.msg_satellites = config_options['msg_satellites'].split()
        self.msg_areaname = config_options['msg_areaname']
        self.areaid = areaid

        self.product_names = {'msg': 'CTTH', 'pps': 'CTTH'}
        self.composite = ncCTTHComposite()

    def get_catalogue(self, product='ctth'):
        super(ctthComposite, self).get_catalogue(product)

    def make_composite(self):
        """Make the CTTH composite"""

        # Reference time for time stamp in composite file
        # sec1970 = datetime(1970, 1, 1)
        import time

        comp_temperature = None
        comp_height = None
        comp_pressure = None

        if len(self.msg_scenes + self.pps_scenes) == 0:
            LOG.critical(
                "Cannot make ctth composite when no Scenes have been found!")
            return False

        # Loop over all polar scenes:
        is_MSG = False
        LOG.info(
            "CTTH composite - Loop over all polar and geostationary scenes:")
        for scene in self.msg_scenes + self.pps_scenes:
            LOG.info("Scene: " + str(scene))
            if (scene.platform_name.startswith("Meteosat") and
                    not hasattr(scene, 'orbit')):
                is_MSG = True
                x_local = ctth_msg(scene, self.areaid)

                dummy, lat = x_local['ctth_alti'].area.get_lonlats()
                x_temperature = x_local['ctth_tempe'].data.compute()
                x_pressure = x_local['ctth_pres'].data.compute()
                x_height = x_local['ctth_alti'].data.compute()

                # convert msg flags to pps
                # fill_value = 0, fill with 65535 (same as pps flag fill value)
                # so that bit 0 is set -> unprocessed -> w=0
                # The weight for masked data is set further down
                x_flag = np.ma.filled(ctth_procflags2pps(x_local['ctth_quality'].data.compute()),
                                      fill_value=65535)
                x_id = 1 * np.ones(x_temperature.shape)
            else:
                is_MSG = False
                try:
                    x_local = ctth_pps(scene, self.areaid)
                except (ProjectException, LoadException) as err:
                    LOG.critical("Couldn't load pps scene: %s\nException was: %s",
                                 (str(scene), str(err)))
                    continue

                # Temperature (K)', u'no_data_value': 255, u'intercept': 100.0,
                # u'gain': 1.0
                LOG.debug("scale and offset: %s %s", str(x_local['ctth_tempe'].attrs['scale_factor']),
                          str(x_local['ctth_tempe'].attrs['add_offset']))

                x_temperature = x_local['ctth_tempe'].data.compute()
                x_pressure = x_local['ctth_pres'].data.compute()
                x_height = x_local['ctth_alti'].data.compute()

                sflags = x_local['ctth_status_flag'].data.compute()
                cflags = x_local['ctth_conditions'].data.compute()
                # qflags = x_local['CTTH'].ctth_quality.data.filled(0)
                qflags = x_local['ctth_quality'].data.compute()
                oldflags = ctth_convert_flags(sflags, cflags, qflags)

                # fill_value = 65535 i.e bit 0 is set -> unprocessed -> w=0
                # x_flag = np.ma.filled(oldflags, fill_value=65535)
                x_flag = oldflags

                x_id = 0 * np.ones(x_temperature.shape)
                lat = 0 * np.ones(x_temperature.shape)

            # time identifier is seconds since 1970-01-01 00:00:00
            x_time = time.mktime(scene.timeslot.timetuple()) * \
                np.ones(x_temperature.shape)
            idx_MSG = is_MSG * np.ones(x_temperature.shape, dtype=np.bool)

            if comp_temperature is None:
                # initialize field with current CTTH
                comp_lon, comp_lat = x_local['ctth_alti'].area.get_lonlats()
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
                #ii = (x_height.mask == True) | (x_height == 0)
                ii = np.isnan(x_height)
                comp_w[ii] = 0
            else:
                # compare with quality of current CTTH
                x_w = get_weight_ctth(x_flag, lat,
                                      abs(self.obstime - scene.timeslot),
                                      idx_MSG)

                # fix to cope with unprocessed data
                # ii = (x_height.mask == True) | (x_height == 0)
                ii = np.isnan(x_height)
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
        self.area = x_local['ctth_alti'].area

        composite = {"temperature": comp_temperature,
                     "height": comp_height,
                     "pressure": comp_pressure,
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
        self.composite.write(tmpfname)
        now = datetime.utcnow()
        fname_with_timestamp = str(
            self.filename) + now.strftime('_%Y%m%d%H%M%S.nc')
        shutil.copy(tmpfname, fname_with_timestamp)
        os.rename(tmpfname, self.filename + '.nc')

        return

    def make_quicklooks(self):
        """Make quicklook images"""
        palette = ctth_height()
        filename = self.filename.strip('.nc') + '_height.png'

        attrs = get_nc_attributes_from_object(self.composite.height.info)
        attrs['_FillValue'] = np.nan
        pimage = PaletteCompositor('MesanComposite')
        xdata = xr.DataArray(self.composite.height.data, dims=['y', 'x'], attrs=attrs)
        ximg = XRImage(pimage((xdata, palette)))

        ximg.save(filename)


if __name__ == "__main__":

    (logfile, config_filename, time_of_analysis, areaid, delta_time_window) = get_arguments()

    if logfile:
        logging.config.fileConfig(logfile)

    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)

    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('satpy').setLevel(logging.INFO)

    LOG = logging.getLogger('make_ctth_composite')

    log_handlers = logging.getLogger('').handlers
    for log_handle in log_handlers:
        if type(log_handle) is handlers.SMTPHandler:
            LOG.debug("Mail notifications to: %s", str(log_handle.toaddrs))

    OPTIONS = get_config(config_filename)

    ctth_comp = ctthComposite(time_of_analysis, delta_time_window, areaid, OPTIONS)
    ctth_comp.get_catalogue()
    ctth_comp.make_composite()
    ctth_comp.write()
    ctth_comp.make_quicklooks()
