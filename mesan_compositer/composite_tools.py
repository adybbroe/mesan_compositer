#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 - 2019, 2023 Adam.Dybbroe

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

"""Collection of minor helper tools for the generation of Mesan composites"""

import os
from glob import glob
from trollsift import Parser, globify
from datetime import datetime, timedelta
from six.moves import configparser
import six
from mesan_compositer.pps_msg_conversions import get_bit_from_flags
import logging

LOG = logging.getLogger(__name__)


MSGSATS = {'Meteosat-9': 'MSG2',
           'Meteosat-8': 'MSG1',
           'Meteosat-10': 'MSG3',
           'Meteosat-11': 'MSG4'}
METEOSAT = {'MSG1': 'Meteosat-8',
            'MSG2': 'Meteosat-9',
            'MSG3': 'Meteosat-10',
            'MSG4': 'Meteosat-11'}

TERRA_AQUA_NAMES = {'eos1': 'EOS-Terra',
                    'eos2': 'EOS-Aqua'}

PLATFORM_NAME_INV = {'Suomi-NPP': 'npp',
                     'NOAA-20': 'noaa20',
                     'EOS-Terra': 'eos1',
                     'EOS-Aqua': 'eos2',
                     'Metop-A': 'metopa',
                     'Metop-B': 'metopb',
                     'Metop-C': 'metopc',
                     'NOAA-18': 'noaa18',
                     'NOAA-15': 'noaa15',
                     'NOAA-19': 'noaa19'}
PLATFORM_NAME = {'npp': 'Suomi-NPP',
                 'noaa20': 'NOAA-20',
                 'eos1': 'EOS-Terra',
                 'eos2': 'EOS-Aqua',
                 'metopa': 'Metop-A',
                 'metopb': 'Metop-B',
                 'metopc': 'Metop-C',
                 'noaa18': 'NOAA-18',
                 'noaa15': 'NOAA-15',
                 'noaa19': 'NOAA-19'}

METOPS = ['metop03', 'metop02', 'metop01']

SENSOR = {'NOAA-19': 'avhrr/3',
          'NOAA-18': 'avhrr/3',
          'NOAA-15': 'avhrr/3',
          'Metop-A': 'avhrr/3',
          'Metop-B': 'avhrr/3',
          'Metop-C': 'avhrr/3',
          'EOS-Terra': 'modis',
          'EOS-Aqua': 'modis',
          'Suomi-NPP': 'viirs',
          'JPSS-1': 'viirs',
          'NOAA-20': 'viirs'}


PPS_FILENAME = "S_NWC_{product:s}_{platform_name:s}_{orbit:05d}_{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z.nc"


class PpsMetaData(object):

    """Container for the metadata defining the pps scenes"""

    def __init__(self, filename=None, geofilename=None,
                 platform_name=None,
                 orbit="00000", timeslot=None,
                 variant=None):
        self.platform_name = platform_name
        self.orbit = orbit
        self.timeslot = timeslot
        self.variant = variant
        self.uri = filename
        self.geofilename = geofilename

    def __str__(self):
        return "\n".join(['filename=' + str(self.uri),
                          'geofilename=' + str(self.geofilename),
                          'platform_name=' + str(self.platform_name),
                          'orbit=' + self.orbit,
                          'timeslot=' + str(self.timeslot),
                          'variant=' + str(self.variant)])

    def __lt__(self, other):
        return self.timeslot < other.timeslot

    def __gt__(self, other):
        return self.timeslot > other.timeslot

    def __le__(self, other):
        return self.timeslot <= other.timeslot

    def __ge__(self, other):
        return self.timeslot >= other.timeslot


class GeoMetaData:

    """Container for the metadata defining the NWCSAF/Geo cloud scenes."""

    def __init__(self, filename=None, platform_name=None,
                 areaid=None, timeslot=None):
        self.timeslot = timeslot
        self.areaid = areaid
        self.platform_name = platform_name
        self.uri = filename
        self._hrit_pattern = '{rate:1s}-000-{hrit_format:_<6s}-{platform_shortname:4s}_{service:_<7s}-{channel:_<9s}-{segment:06d}___-{start_time:%Y%m%d%H%M}-__'
        self._hrit_path = None
        self.hrit_files = None

    def find_hrit_files(self, hrit_path):
        """Find the matching hrit files for the cloud scene."""
        self._hrit_path = hrit_path
        self.hrit_files = []

        p__ = Parser(self._hrit_pattern)
        hrit_files = self._hrit_path.glob(globify(self._hrit_pattern))

        for hrit_fname in hrit_files:
            res = p__.parse(hrit_fname.name)
            if self.timeslot - timedelta(seconds=1) < res['start_time'] < self.timeslot + timedelta(seconds=1):
                self.hrit_files.append(hrit_fname)

        self.hrit_files.sort()

    def __str__(self):
        return "\n".join(['filename=' + str(self.uri),
                          'platform_name=' + str(self.platform_name),
                          'areaid=' + self.areaid,
                          'timeslot=' + str(self.timeslot)])

    def __lt__(self, other):
        return self.timeslot < other.timeslot

    def __gt__(self, other):
        return self.timeslot > other.timeslot

    def __le__(self, other):
        return self.timeslot <= other.timeslot

    def __ge__(self, other):
        return self.timeslot >= other.timeslot


def get_analysis_time(start_t, end_t):
    """From two times defining an interval, determine the closest hour (zero
    minutes past) and return as a datetime object"""

    if end_t and start_t > end_t:
        raise IOError("Start time greater than end time!")
    elif not end_t:
        end_t = start_t + timedelta(seconds=300)
        LOG.warning(
            "No end time, so assuming equal to start time + 5 minutes!")
    mean_time = (end_t - start_t) / 2 + start_t
    mean_time = mean_time + timedelta(seconds=1800)

    return datetime(
        mean_time.year, mean_time.month, mean_time.day, mean_time.hour, 0, 0)


def get_ppslist(filelist, timewindow, satellites=None, variant=None):
    """Get the full list of metadata keys for all pps passes in the *filelist*,
    but only for the satellites specified in the list *satellites* if given"""

    from trollsift import Parser
    prod_p = Parser(PPS_FILENAME)

    LOG.debug("List of satellites: %s", str(satellites))
    plist = []
    files_old = True
    latest_file = None
    latest_file_time = datetime(1970, 1, 1)
    LOG.info("Going through file list with %d files", len(filelist))
    for filename in filelist:
        bname = os.path.basename(filename)
        try:
            res = prod_p.parse(bname)
        except ValueError:
            LOG.exception('Failed processing filename %s', filename)
            LOG.warning('Probably wrong date time string in PPS file, skip it...')
            continue

        sat = res['platform_name']

        if satellites and PLATFORM_NAME.get(sat, sat) not in satellites:
            LOG.debug("Satellite not in the list of platforms! platform=%s", PLATFORM_NAME.get(sat, sat))
            continue

        product = res['product']
        geofilename = filename.replace(product, 'CMA')
        orbit = '%05d' % res['orbit']
        if 'end_time' in res.keys():
            if six.PY2:
                # Requires Python 2.7:
                delta_seconds = (res['end_time']-res['start_time']).total_seconds()
                timeslot = res['start_time'] + timedelta(seconds=delta_seconds/2.)
            else:
                timeslot = res['start_time'] + (res['end_time']-res['start_time'])/2.
        else:
            timeslot = res['start_time']

        if timeslot > latest_file_time:
            latest_file_time = timeslot
            latest_file = filename
        platform_name = PLATFORM_NAME.get(sat)
        if not platform_name:
            raise IOError("Error: satellite %s not supported!" % sat)

        # Now filter out all passes outside time window:
        if (timeslot > timewindow[0] and
                timeslot < timewindow[1]):

            mda = PpsMetaData(filename=filename,
                              geofilename=geofilename,
                              orbit=orbit, timeslot=timeslot,
                              platform_name=platform_name,
                              variant=variant)
            plist.append(mda)
            files_old = False
        elif (timewindow[0] - timeslot) < timedelta(seconds=3600 * 4):
            files_old = False

    if files_old and latest_file is not None:
        LOG.critical("No fresh pps products found - " +
                     "most recent = %s (obs-time = %s)\n" +
                     "Latest file is more than 4 hours from time-window\n",
                     os.path.basename(latest_file), str(latest_file_time))
    elif len(plist) == 0:
        LOG.debug("No valid pps products found for filelist with %d files.\n\tFirst and last file in list: %s %s",
                  len(filelist), filelist[0], filelist[-1])

    return plist


def get_msglist(filelist, timewindow, area_id, satellites=None):
    """Get the full list of metadata keys for all Meteosat slots in the
    *filelist*, but only for the satellites specified in the list *satellites*
    if given"""

    if not satellites:
        satellites = ['Meteosat-8', 'Meteosat-9',
                      'Meteosat-10', 'Meteosat-11']
    metsats = [MSGSATS.get(s, 'MSGx') for s in satellites]

    mlist = []
    for filename in filelist:
        bname = os.path.basename(filename)
        LOG.debug("Filename: %s", str(bname))

        bnsplit = bname.split('_')
        sat = bnsplit[1]
        if sat not in metsats:
            LOG.warning('Satellite ' + str(sat) +
                        ' not in list: ' + str(metsats))
            continue

        platform_name = METEOSAT[sat]
        bnsplit = bname[17:].split('_')
        areaid = bnsplit[1].split('.')[0]
        if areaid != area_id:
            LOG.debug("Area id " + str(areaid) +
                      " not requested (" + str(area_id) + ")")
            LOG.debug("bnsplit = %s", str(bnsplit))
            continue

        # Hardcoded the filenaming convention! FIXME!
        try:
            #timeslot = datetime.strptime(bname[17:17 + 12], '%Y%m%d%H%M')
            timeslot = datetime.strptime(bnsplit[0], '%Y%m%d%H%M')
        except ValueError:
            LOG.error("Failure: Can't get the time of the msg scene! " +
                      str(bname))
            continue

        # Now filter out all passes outside time window:
        if (timeslot > timewindow[0] and
                timeslot < timewindow[1]):
            mda = GeoMetaData(filename=filename,
                              areaid=areaid, timeslot=timeslot,
                              platform_name=platform_name)
            mlist.append(mda)

    return mlist


def get_nwcsaf_files(basedir, file_ext):
    """Get list of file names of msg or pps products"""
    from glob import glob
    return glob(os.path.join(basedir, '*' + file_ext))


def get_weight_ctth(ctth_flag, lat, tdiff, is_msg):
    """Weights for given CTTH flag, time diff and latitude (only MSG).
    """
    #
    import numpy as np
    #
    #  limits; linear lat dependence for MSG
    latmin_msg = 52.0  # weight factor is 1 if lat < LATMIN_MSG
    latmax_msg = 75.0  # weight factor is 0 if lat > LATMAX_MSG
    #
    # time diff in minutes when diff affects quality
    # weight factor is 0.5 if diff > TDIFF
    tdiff_thr = 30.0
    #
    # weight factors per CT quality flag (MSG,PPS)
    # this is based on pps flags, msg flags are mapped to pps
    wCTTHflg = np.array([
        [0.0,  0.0],  # 00 Not processed
        [1.0,  1.0],  # 01 Cloudy
        [1.0,  1.0],  # 02 Opaque cloud
        [1.0,  1.0],  # 03 RTTOV IR simulations available
        [0.5,  0.5],  # 04 Missing NWP data
        [0.5,  0.5],  # 05 Thermal inversion available
        [1.0,  1.0],  # 06 Missing AVHRR data
        [1.0,  1.0],  # 07 RTTOV IR simulation applied
        [0.5,  0.5],  # 08 Windowing technique applied
        [1.0,  1.0],  # 09 ???
        [1.0,  1.0],  # 10 ???
        [1.0,  1.0],  # 11 ???
        [1.0,  1.0],  # 12 ??? prev used for PPS/MSG
        [1.0,  1.0],  # 13 ??? prev used for large time diff
        [1.0,  1.0],  # 14 Quality estimation available
        [0.5,  0.5]   # 15 Low confidence
    ])
    #
    #
    # default quality is 1.0
    weight = np.ones(np.shape(ctth_flag))
    #
    # large time diff to analysis time - decrease weight
    if abs(tdiff).seconds / 60 > tdiff_thr:
        weight *= 0.5
    #
    # reduce quality according to CT flag, MSG = 0 / 1
    #
    #
    for bit in range(len(wCTTHflg)):
        b = get_bit_from_flags(ctth_flag, bit)
        # need integer for index to this array
        weight[np.nonzero(b)] *= wCTTHflg[bit, 1 * is_msg[np.nonzero(b)]]
    # linear lat dependence for MSG btw LATMIN_MSG and latmax_msg
    # weight is 1.0 for lat < LATMIN and 0.0 for lat > LATMAX
    if not np.all(is_msg == False):
        ii = (lat >= latmin_msg) * (lat <= latmax_msg) * is_msg
        weight[ii] *= (latmax_msg - lat[ii]) / (latmax_msg - latmin_msg)
        ii = (lat > latmax_msg) * is_msg
        weight[ii] = 0.0

    return weight


def get_weight_cloudtype(ctype, ctype_flag, lat, tdiff, is_msg, fill_value=20):
    """Weights for given ctype, ctype flag, time diff and latitude (only MSG).
    """
    #
    import numpy as np

    #  limits; linear lat dependence for MSG
    latmin_msg = 52.0  # weight factor is 1 if lat < latmin_msg
    latmax_msg = 75.0  # weight factor is 0 if lat > latmax_msg
    #
    # time diff in minutes when diff affects quality
    # weight factor is 0.5 if diff > tdiff_thr
    tdiff_thr = 30.0
    #
    # weight factors per ctype quality flag (MSG,PPS)
    # this is based on pps flags, msg flags are mapped to pps
    wCTflg = np.array([
        [1.0,  1.0],  # 00 Land
        [0.9,  0.9],  # 01 Coast
        [1.0,  1.0],  # 02 Night
        [0.95, 0.95],  # 03 Twilight
        [0.5,  0.5],  # 04 Sunglint
        [0.95, 0.95],  # 05 High terrain
        [0.5,  0.5],  # 06 Low level inversion present
        [1.0,  1.0],  # 07 NWP data present
        [1.0,  1.0],  # 08 AVHRR channel(s) missing
        [0.5,  0.5],  # 09 Low quality
        [0.0,  0.0],  # 10 Reclassified after spatial smoothing
        [1.0,  1.0]   # 11 Stratiform-cumuliform distinction performed
    ])
    #
    # weight factors per ctype class
    weights_ctype_class = np.zeros((256,))
    weights_ctype_class_values = np.array([
        0.0,  # 00 Not processed
        0.95,  # 01 Cloud free land
        1.0,  # 02 Cloud free sea
        0.95,  # 03 Snow/ice contaminated land
        0.95,  # 04 Snow/ice contaminated sea
        1.0,  # 05 Very low cumiliform cloud
        1.0,  # 06 Very low stratiform cloud
        1.0,  # 07 Low cumiliform cloud
        1.0,  # 08 Low stratiform cloud
        1.0,  # 09 Medium level cumiliform cloud
        1.0,  # 10 Medium level stratiform cloud
        1.0,  # 11 High and opaque cumiliform cloud
        1.0,  # 12 High and opaque stratiform cloud
        1.0,  # 13 Very high and opaque cumiliform cloud
        1.0,  # 14 Very high and opaque stratiform cloud
        1.0,  # 15 Very thin cirrus cloud
        1.0,  # 16 Thin cirrus cloud
        1.0,  # 17 Thick cirrus cloud
        1.0,  # 18 Cirrus above low or medium level cloud
        0.95,  # 19 Fractional or sub-pixel cloud
        0.0   # 20 Undefined
    ])
    for idx in range(21):
        weights_ctype_class[idx] = weights_ctype_class_values[idx]
    #

    #
    # default quality is 1.0
    weight = np.ones(np.shape(ctype_flag))
    #
    # large time diff to analysis time - decrease weight
    if abs(tdiff).seconds / 60 > tdiff_thr:
        weight *= 0.5
    # else:
    #     # Small time difference to analysis time, decrease wight slightly only:
    #     weight *= (1 - abs(tdiff).seconds/(tdiff_thr * 60.)**2)

    #
    # reduce quality according to ctype flag, is_msg = 0 / 1
    for bit in range(len(wCTflg)):
        bit_is_set = get_bit_from_flags(ctype_flag, bit)
        # need integer for index to this array
        weight[np.nonzero(bit_is_set)] *= wCTflg[bit,
                                                 1 * is_msg[np.nonzero(bit_is_set)]]

    # linear lat dependence for MSG btw latmin_msg and latmax_msg
    # weight is 1.0 for lat < LATMIN and 0.0 for lat > LATMAX
    if not np.all(is_msg == False):
        ii = (lat >= latmin_msg) * (lat <= latmax_msg) * is_msg
        weight[ii] *= (latmax_msg - lat[ii]) / (latmax_msg - latmin_msg)
        ii = (lat > latmax_msg) * is_msg
        weight[ii] = 0.0
    #
    # special treatment of high clouds - medium to Cirrus
    # why? increase q to override ceilometers???
    #
    # if weight is very small: set to 0.0
    ii = (ctype >= 9) * (ctype <= 18) * (weight < 1e-6)
    weight[ii] = 0.0
    # if not low quality: set to 1.0
    b = get_bit_from_flags(ctype_flag, 9)
    ii = (ctype >= 9) * (ctype <= 18) * (b == 0)
    weight[ii] = 1.0
    #
    # all ctype above 20 are set to fill_value (undefined with weight 0)
    # howto index with x_ctype.astype('int') from masked array ?
    ctype[ctype > 20] = fill_value
    #
    # dependence on cloud type
    weight *= weights_ctype_class[ctype.astype('int')]
    #
    # np.savez('input_output.npz',
    #          CTYPE=ctype.data[1200:1210, 1000:1010],
    #          CTYPE_FLAGS=ctype_flag.data[1200:1210, 1000:1010],
    #          LAT=lat[1200:1210, 1000:1010],
    #          IS_MSG=is_msg[1200:1210, 1000:1010],
    #          WEIGHTS=weight[1200:1210, 1000:1010])

    return weight
