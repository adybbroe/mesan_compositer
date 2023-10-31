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
import logging
import os
import pathlib
import shutil
import sys
import tempfile
from datetime import datetime, timedelta
from glob import glob
from logging import handlers
from tempfile import gettempdir

from satpy.utils import debug_on
from trollsift import Parser, globify

from mesan_compositer.composite_tools import METEOSAT, METOPS, MSGSATS, GeoMetaData, get_ppslist
from mesan_compositer.config import get_config
from mesan_compositer.ct_quicklooks import ctype_quicklook_from_netcdf
from mesan_compositer.load_cloud_products import blend_cloud_products

debug_on()

LOG = logging.getLogger(__name__)


#: Default time format
_DEFAULT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

#: Default log format
_DEFAULT_LOG_FORMAT = "[%(levelname)s: %(asctime)s : %(name)s] %(message)s"

CPROD_GROUP_NAME = {"CT": "CT_group",
                    "CTTH": "CTTH_group"}


def get_arguments():
    """Get command line arguments.

    args.logging_conf_file, args.config_file, obs_time, area_id, wsize

    Return:
      File path of the logging.ini file
      File path of the application configuration file
      Observation/Analysis time
      Area id
      Window size

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--datetime", "-d", help="Date and time of observation - yyyymmddhh",
                        required=True)
    parser.add_argument("--time_window", "-t", help="Number of minutes before and after time window",
                        required=True)
    parser.add_argument("--area_id", "-a", help="Area id",
                        required=True)
    parser.add_argument("-c", "--config_file",
                        type=str,
                        dest="config_file",
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
    parser.add_argument("--ctype_out", help="Mesan Cloudtype composite output",
                        required=False)
    parser.add_argument("--background_ctth", help="CTTH background...",
                        required=False)

    args = parser.parse_args()

    tanalysis = datetime.strptime(args.datetime, "%Y%m%d%H")
    delta_t = timedelta(minutes=int(args.time_window))
    if "template" in args.config_file:
        print("Template file given as master config, aborting!")
        sys.exit()

    return args.logging_conf_file, args.config_file, tanalysis, args.area_id, delta_t


class CloudproductCompositer:
    """The NWCSAF Cloud product composite generator class."""

    def __init__(self, obstime, tdiff, areaid, config_options, product):
        """Initialize the cloud type composite instance."""
        if product not in ["CT", "CTTH"]:
            raise ValueError("Cloud products must be one of CT or CTTH!")
        self.product = product

        values = {"area": areaid, }

        # Generate the filename from the observation time and the
        # specifcations in the config file:
        LOG.info("Output file name is generated from observation " +
                 "time and info in config file:")
        cprod_comp_filename = "{name}_composite_filename".format(name=self.product.lower())
        bname = obstime.strftime(config_options[cprod_comp_filename]) % values
        path = config_options["composite_output_dir"]
        self.filename = os.path.join(path, bname)

        LOG.info("Filename = " + str(self.filename))

        description = {"CT": "Cloud Type",
                       "CTTH": "Cloud Top Temperature and Height"}.get(self.product)
        self.description = "{desc} composite for Mesan".format(desc=description)
        self.obstime = obstime
        self.timediff = tdiff
        self.time_window = (obstime - tdiff, obstime + tdiff)
        LOG.debug("Time window (used only for polar sat scenes): " +
                  str(self.time_window[0]) + " - " + str(self.time_window[1]))

        # geo_tdiff = timedelta(minutes=1)
        # self._geo_time_window = (obstime - geo_tdiff, obstime + geo_tdiff)
        # LOG.debug("Time window (used only for the Geo scenes): " +
        #          str(self._geo_time_window[0]) + " - " + str(self._geo_time_window[1]))

        self.polar_satellites = config_options["polar_satellites"]
        LOG.debug("Polar satellites supported: %s", str(self.polar_satellites))

        self.msg_satellites = config_options["msg_satellites"]
        self.msg_areaname = config_options["msg_areaname"]
        self.areaid = areaid
        self.longitude = None
        self.latitude = None

        self._options = config_options

        self.pps_scenes = []
        self.msg_scenes = []

        self.group_name = CPROD_GROUP_NAME.get(self.product)  # Ex. 'CT_group'
        self.blended_scene = None

    def _get_all_pps_files(self, pps_dir):
        """Return list of pps files in directory."""
        if pps_dir is None:
            return []

        LOG.debug("Get all PPS files in this directory = " + str(pps_dir))
        # Example: S_NWC_CT_metopb_14320_20150622T1642261Z_20150622T1654354Z.nc
        # return glob(os.path.join(pps_dir, 'S_NWC_CT_*nc'))
        return glob(os.path.join(pps_dir, globify(self._options["pps_filename"],
                                                  {"product": self.product})))

    def _get_all_geo_files(self, geo_dir):
        """Return list of NWCSAF/Geo files in directory."""
        if geo_dir is None:
            return []

        LOG.debug("Get all NWCSAF/Geo files in this directory = " + str(geo_dir))
        # S_NWC_CT_MSG4_MSG-N-VISIR_20230118T103000Z_PLAX.nc
        return glob(os.path.join(geo_dir, globify(self._get_msg_filepattern_from_config())))

    def _get_msg_filepattern_from_config(self):
        msg_prod_filename = "msg_{prod}_filename".format(prod={"CT": "cty",
                                                               "CTTH": "ctth"}.get(self.product))
        return self._options[msg_prod_filename]

    def get_pps_scenes(self, pps_file_list, satellites=None, variant=None):
        """Get the list of valid pps scenes from file list."""
        if satellites is None:
            satellites = self.polar_satellites

        return get_ppslist(pps_file_list, self.time_window,
                           satellites=satellites, variant=variant)

    def get_geo_scenes(self, geo_file_list):
        """Get the list of valid NWCSAF/Geo scenes from file list."""
        metsats = [MSGSATS.get(s, "MSGx") for s in self.msg_satellites]
        p__ = Parser(self._get_msg_filepattern_from_config())

        mlist = []
        for geo_file in geo_file_list:
            try:
                res = p__.parse(os.path.basename(geo_file))
            except ValueError:
                LOG.debug("File name format not supported/requested: {fname}".format(fname=os.path.basename(geo_file)))
                continue

            platform_name = res["satellite"]
            areaid = res["area"]
            timeslot = res["nominal_time"]
            if platform_name not in metsats:
                LOG.warning("Satellite " + str(platform_name) + " not in list: " + str(metsats))
                continue

            if areaid != self.msg_areaname:
                LOG.debug("Area id " + str(areaid) + " not requested (" + str(self.msg_areaname) + ")")
                continue

            if (timeslot > self.time_window[0] and
                    timeslot < self.time_window[1]):
                mda = GeoMetaData(filename=geo_file,
                                  areaid=areaid, timeslot=timeslot,
                                  platform_name=METEOSAT.get(platform_name))
                mlist.append(mda)

        return mlist
        # return get_msglist(geo_file_list, p__, self.time_window, self.msg_areaname, self.msg_satellites)

    def get_catalogue(self):
        """Get the catalougue of input data files.

        Get the meta data (start-time, satellite, orbit number etc) for all
        available satellite scenes (both polar and geostationary) within the
        time window specified. For the time being this catalouge generation
        will be done by simple file globbing. In the future this might be
        done by doing a DB search.
        """
        self._get_pps_catalogue()
        LOG.info(str(len(self.pps_scenes)) + " PPS scenes located")
        for scene in self.pps_scenes:
            LOG.debug("Polar scene:\n" + str(scene))

        self._get_geo_catalogue()
        LOG.info(str(len(self.msg_scenes)) + " MSG scenes located")
        for scene in self.msg_scenes:
            LOG.debug("Geo scene:\n" + str(scene))

    def _get_pps_catalogue(self):
        """Get the catalougue of NWCSAF/PPS input data files."""
        min_num_of_pps_dr_files = int(self._options.get("min_num_of_pps_dr_files", "0"))
        # Get all polar satellite scenes:
        pps_dr_dir = self._options["pps_direct_readout_dir"]

        dr_list = self._get_all_pps_files(pps_dr_dir)
        LOG.info("Number of direct readout pps %s files in dir: %s",
                 {"CT": "cloudtype", "CTTH": "ctth"}.get(self.product),
                 str(len(dr_list)))

        if len(dr_list) <= min_num_of_pps_dr_files:
            LOG.critical("Too few PPS DR files found! (%d<=%d)\n" +
                         "pps_dr_dir = %s",
                         len(dr_list), min_num_of_pps_dr_files,
                         str(pps_dr_dir))

        if len(dr_list) > 0:
            ppsdr = self.get_pps_scenes(dr_list)
        else:
            ppsdr = []

        ppsgds = []
        gds_list = self._get_all_pps_files(self._options.get("pps_metop_gds_dir"))
        if len(gds_list) > 0:
            now = datetime.utcnow()
            LOG.info("Number of Metop GDS files in dir: " + str(len(gds_list)))
            ppsgds = self.get_pps_scenes(gds_list, satellites=METOPS, variant="global")
            tic = datetime.utcnow()
            LOG.info("Retrieve the metop-gds list took " +
                     str((tic - now).seconds) + " sec")

        self.pps_scenes = ppsdr + ppsgds
        self.pps_scenes.sort()

    def _get_geo_catalogue(self):
        """Get the catalougue of NWCSAF/PPS input data files."""
        # Get all geostationary satellite scenes:
        # prod_id = {"CT": "02", "CTTH": "03"}.get(self.product)
        # msg_dir = self._options["msg_dir"] % {"number": prod_id}
        p__ = Parser(self._options["msg_dir"])
        msg_dir = p__.compose({"product": self.product})
        # What about EuropeCanary and possible other areas!? FIXME!

        msg_list = self._get_all_geo_files(msg_dir)
        LOG.debug(
            "MSG files in directory " + str(msg_dir) + " : " + str(msg_list))
        LOG.info("Get files inside time window: " + str(self.time_window[0]) + " - " + str(self.time_window[1]))

        self.msg_scenes = self.get_geo_scenes(msg_list)
        # Return only the most relevant (closest in time to the obstime):
        found_idx = -1
        tdiff = timedelta(minutes=120)
        for idx in range(len(self.msg_scenes)):
            scene = self.msg_scenes[idx]
            LOG.debug("Time slot for scene: %s" % str(scene.timeslot))
            if abs(scene.timeslot - self.obstime) < tdiff:
                tdiff = abs(scene.timeslot - self.obstime)
                print(tdiff)
                found_idx = idx

        if found_idx >= 0:
            self.msg_scenes = [self.msg_scenes[found_idx]]
            LOG.info("The scene closest in time to the analysis time: %s" % str(self.msg_scenes[0]))

    def blend_cloud_products(self):
        """Blend the Cloud products together and create a cloud analysis."""
        areaid = self.areaid

        cloud_scenes = []
        for scene in self.msg_scenes:
            cloud_product_filepath = pathlib.Path(scene.uri)
            cloud_scenes.append([cloud_product_filepath, ])

        for scene in self.pps_scenes:
            ppsfile = pathlib.Path(scene.uri)
            geofilename = pathlib.Path(scene.geofilename)
            cloud_scenes.append([ppsfile, geofilename])

        satpy_composite_name = {"CT": "ct", "CTTH": "ctth_alti"}.get(self.product)
        self.blended_scene, group_name = blend_cloud_products(satpy_composite_name,
                                                              areaid, *cloud_scenes,
                                                              cache_dir=gettempdir())
        self.group_name = group_name

    def write(self):
        """Write the composite to a netcdf file."""
        tmpfname = tempfile.mktemp(suffix=os.path.basename(self.filename)+".nc",
                                   dir=os.path.dirname(self.filename))

        self.blended_scene.save_dataset(self.group_name, filename=tmpfname)
        now = datetime.utcnow()
        fname_with_timestamp = str(self.filename) + now.strftime("_%Y%m%d%H%M%S.nc")
        shutil.copy(tmpfname, fname_with_timestamp)
        os.rename(tmpfname, self.filename + ".nc")
        return self.filename + ".nc"

    def quicklook(self, netcdf_filename):
        """Make a quicklook image from the netCDF file."""
        # FIXME! This is hard coded for a cloud type image!
        return ctype_quicklook_from_netcdf(self.group_name, netcdf_filename)


if __name__ == "__main__":

    (logfile, config_filename, time_of_analysis, area_id, delta_time_window) = get_arguments()

    handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger("").addHandler(handler)
    # logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger("satpy").setLevel(logging.DEBUG)

    LOG = logging.getLogger("make_ct_composite")

    log_handlers = logging.getLogger("").handlers
    for log_handle in log_handlers:
        if type(log_handle) is handlers.SMTPHandler:
            LOG.debug("Mail notifications to: %s", str(log_handle.toaddrs))

    OPTIONS = get_config(config_filename)

    ctcomp = CloudproductCompositer(time_of_analysis, delta_time_window, area_id, OPTIONS, "CT")
    ctcomp.get_catalogue()
    ctcomp.blend_cloud_products()
    output_filepath = ctcomp.write()
    qlook_filepath = ctcomp.quicklook(output_filepath)
