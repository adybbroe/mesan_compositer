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

"""Collection of minor helper tools for the generation of Mesan composites"""

import os
from datetime import datetime

import logging
LOG = logging.getLogger(__name__)


MSGSATS = {'meteosat09': 'MSG2',
           'meteosat08': 'MSG1',
           'meteosat10': 'MSG3',
           'meteosat11': 'MSG4'}
METEOSAT = {'MSG1': 'meteosat08',
            'MSG2': 'meteosat09',
            'MSG3': 'meteosat10',
            'MSG4': 'meteosat11'}


class PpsMetaData(object):

    """Container for the metadata defining the pps scenes"""

    def __init__(self, platform=None, number=None,
                 orbit="00000", timeslot=None,
                 variant=None):
        self.platform = platform
        self.number = number
        self.orbit = orbit
        self.timeslot = timeslot
        self.variant = variant

    def __str__(self):
        return "\n".join(['platform=' + str(self.platform),
                          'number=' + str(self.number),
                          'orbit=' + self.orbit,
                          'timeslot=' + str(self.timeslot),
                          'variant=' + str(self.variant)])


class MsgMetaData(object):

    """Container for the metadata defining the msg scenes"""

    def __init__(self, platform=None, number=None,
                 areaid=None, timeslot=None):
        self.timeslot = timeslot
        self.areaid = areaid
        self.platform = platform
        self.number = number

    def __str__(self):
        return "\n".join(['platform=' + str(self.platform),
                          'number=' + str(self.number),
                          'areaid=' + self.areaid,
                          'timeslot=' + str(self.timeslot)])


def get_ppslist(filelist, timewindow, satellites=None, variant=None):
    """Get the full list of metadata keys for all pps passes in the *filelist*,
    but only for the satellites specified in the list *satellites* if given"""

    plist = []
    for filename in filelist:
        bname = os.path.basename(filename)
        bnsplit = bname.split('_')
        sat = bnsplit[0]
        if satellites and sat not in satellites:
            continue

        orbit = bnsplit[3]
        timeslot = datetime.strptime(bnsplit[1] + bnsplit[2], '%Y%m%d%H%M')
        if sat.find('npp') == 0:
            platform = 'npp'
            number = ''
        elif sat.find('noaa') == 0:
            platform = 'noaa'
            number = sat.split('noaa')[1]
        elif sat.find('metop') == 0:
            platform = 'metop'
            number = sat.split('metop')[1]
        else:
            raise IOError("Error: satellite %s not supported!" % sat)

        # Now filter out all passes outside time window:
        if (timeslot > timewindow[0] and
                timeslot < timewindow[1]):

            plist.append(PpsMetaData(orbit=orbit, timeslot=timeslot,
                                     platform=platform, number=number,
                                     variant=variant))

    return plist


def get_msglist(filelist, timewindow, area_id, satellites=None):
    """Get the full list of metadata keys for all Meteosat slots in the
    *filelist*, but only for the satellites specified in the list *satellites*
    if given"""

    if not satellites:
        satellites = ['meteosat08', 'meteosat09',
                      'meteosat10', 'meteosat11']
    metsats = [MSGSATS.get(s, 'MSGx') for s in satellites]

    mlist = []
    for filename in filelist:
        bname = os.path.basename(filename)
        bnsplit = bname.split('_')
        sat = bnsplit[1]
        if sat not in metsats:
            continue

        platform = 'meteosat'
        number = (METEOSAT.get(sat, 'meteosat09')).split(platform)[1]
        areaid = bnsplit[-1].split('.')[0]

        if areaid != area_id:
            continue

        # Hardcoded the filenaming convention! FIXME!
        try:
            timeslot = datetime.strptime(bname[17:17 + 12], '%Y%m%d%H%M')
        except ValueError:
            LOG.error("Failure: Can't get the time of the msg scene! " +
                      str(bname))
            continue

        # Now filter out all passes outside time window:
        if (timeslot > timewindow[0] and
                timeslot < timewindow[1]):
            mlist.append(MsgMetaData(areaid=areaid, timeslot=timeslot,
                                     platform=platform, number=number))

    return mlist


def get_nwcsaf_files(basedir, file_ext):
    """Get list of file names of msg or pps products"""
    from glob import glob
    return glob(os.path.join(basedir, '*' + file_ext))


def get_weight_cloudtype(ctype, ctype_flag, lat, tdiff, MSG):
    """Weights for given ctype, ctype flag, time diff and latitude (only MSG).
    """
    #
    import numpy as np
    #
    #  limits; linear lat dependence for MSG
    LATMIN_MSG = 52.0  # weight factor is 1 if lat < LATMIN_MSG
    LATMAX_MSG = 75.0  # weight factor is 0 if lat > LATMAX_MSG
    #
    # time diff in minutes when diff affects quality
    # weight factor is 0.5 if diff > TDIFF
    TDIFF = 30.0
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
    weights_ctype_class = np.array([
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
    #

    def get_bit_from_flags(arr, nbit):
        """I don't know what this function does.
        """
        res = np.bitwise_and(np.right_shift(arr, nbit), 1)
        return res.astype('b')
    #
    # default quality is 1.0
    w = np.ones(np.shape(ctype_flag))
    #
    # large time diff to analysis time - decrease weight
    if abs(tdiff).seconds / 60 > TDIFF:
        w *= 0.5
    #
    # reduce quality according to ctype flag, MSG = 0 / 1
    for bit in range(len(wCTflg)):
        b = get_bit_from_flags(ctype_flag, bit)
        # need integer for index to this array
        w[np.nonzero(b)] *= wCTflg[bit, 1 * MSG[np.nonzero(b)]]

    # linear lat dependence for MSG btw LATMIN_MSG and LATMAX_MSG
    # weight is 1.0 for lat < LATMIN and 0.0 for lat > LATMAX
    if not np.all(MSG == False):
        ii = (lat >= LATMIN_MSG) * (lat <= LATMAX_MSG) * MSG
        w[ii] *= (LATMAX_MSG - lat[ii]) / (LATMAX_MSG - LATMIN_MSG)
        ii = (lat > LATMAX_MSG) * MSG
        w[ii] = 0.0
    #
    # special treatment of high clouds - medium to Cirrus
    # why? increase q to override ceilometers???
    #
    # if weight is very small: set to 0.0
    ii = (ctype >= 9) * (ctype <= 18) * (w < 1e-6)
    w[ii] = 0.0
    # if not low quality: set to 1.0
    b = get_bit_from_flags(ctype_flag, 9)
    ii = (ctype >= 9) * (ctype <= 18) * (b == 0)
    w[ii] = 1.0
    #
    # all ctype above 20 are set to 20 (undefined with weight 0)
    # howto index with x_ctype.astype('int') from masked array ?
    ctype[ctype > 20] = 20
    #
    # dependence on cloud type
    w *= weights_ctype_class[ctype.astype('int')]
    #
    return w
