#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 - 2019, 2023 Adam.Dybbroe

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

"""Unit testing the composite generation."""

import pathlib
import unittest
from datetime import datetime, timedelta

import numpy as np

from mesan_compositer.composite_tools import (GeoMetaData, PpsMetaData,
                                              get_analysis_time,
                                              get_weight_cloudtype)

CTYPE_MSG = np.array([[6, 6, 6, 6, 6, 6, 6, 6, 6, 6],
                      [6, 6, 6, 6, 6, 6, 6, 19, 6, 6],
                      [6, 6, 6, 6, 6, 6, 19, 19, 19, 6],
                      [6, 6, 6, 6, 1, 19, 19, 19, 19, 6],
                      [6, 6, 1, 1, 1, 19, 19, 19, 19, 6],
                      [1, 1, 1, 1, 1, 19, 19, 6, 6, 6],
                      [1, 1, 1, 1, 1, 6, 6, 6, 6, 6],
                      [1, 1, 1, 6, 6, 6, 6, 6, 6, 6],
                      [1, 6, 6, 6, 6, 6, 6, 6, 6, 6],
                      [6, 6, 6, 6, 6, 6, 6, 6, 6, 6]], "uint8")

CTYPE_MSG_FLAG = np.array([[128, 128, 128, 128, 128, 128, 128, 128, 128, 128],
                           [128, 128, 128, 128, 128, 128, 128, 640, 128, 128],
                           [128, 128, 128, 128, 128, 128, 640, 640, 640, 128],
                           [128, 128, 128, 128, 640, 640, 640, 640, 640, 128],
                           [128, 128, 640, 640, 640, 640, 640, 640, 640, 128],
                           [640, 640, 640, 640, 640, 640, 640, 128, 128, 128],
                           [640, 640, 640, 640, 640, 128, 128, 128, 128, 128],
                           [640, 640, 640, 128, 128, 128, 128, 128, 128, 128],
                           [640, 128, 128, 128, 128, 128, 128, 128, 128, 128],
                           [128, 128, 128, 128, 128, 128, 128, 128, 128, 128]], "int16")


LAT_MSG = np.ones((10, 10), "float") * 51.5
LAT_MSG2 = np.ones((10, 10), "float") * 60.0

TDIFF_MSG = timedelta(seconds=0)
IS_MSG_MSG = np.array([[True, True, True, True, True,
                        True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True,
                           True, True, True, True, True],
                       [True, True, True, True, True, True, True, True, True, True]])

WEIGHT_MSG = np.array([[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                       [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.475, 1.0, 1.0],
                       [1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
                           0.475, 0.475, 0.475, 1.0],
                       [1.0, 1.0, 1.0, 1.0, 0.475, 0.475,
                           0.475, 0.475, 0.475, 1.0],
                       [1.0, 1.0, 0.475, 0.475, 0.475,
                           0.475, 0.475, 0.475, 0.475, 1.0],
                       [0.475, 0.475, 0.475, 0.475,
                           0.475, 0.475, 0.475, 1.0, 1.0, 1.0],
                       [0.475, 0.475, 0.475,
                           0.475, 0.475, 1.0, 1.0, 1.0, 1.0, 1.0],
                       [0.475, 0.475,
                           0.475, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                       [0.475, 1.0, 1.0, 1.0,
                           1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                       [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]], "float")
WEIGHT_MSG2 = np.array([[0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391,
                         0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391],
                        [0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391,
                         0.65217391,  0.65217391,  0.30978261,  0.65217391,  0.65217391],
                        [0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391,
                         0.65217391,  0.30978261,  0.30978261,  0.30978261,  0.65217391],
                        [0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.30978261,
                         0.30978261,  0.30978261,  0.30978261,  0.30978261,  0.65217391],
                        [0.65217391,  0.65217391,  0.30978261,  0.30978261,  0.30978261,
                         0.30978261,  0.30978261,  0.30978261,  0.30978261,  0.65217391],
                        [0.30978261,  0.30978261,  0.30978261,  0.30978261,  0.30978261,
                         0.30978261,  0.30978261,  0.65217391,  0.65217391,  0.65217391],
                        [0.30978261,  0.30978261,  0.30978261,  0.30978261,  0.30978261,
                         0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391],
                        [0.30978261,  0.30978261,  0.30978261,  0.65217391,  0.65217391,
                         0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391],
                        [0.30978261,  0.65217391,  0.65217391,  0.65217391,  0.65217391,
                         0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391],
                        [0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391,
                         0.65217391,  0.65217391,  0.65217391,  0.65217391,  0.65217391]], "float")


class TestCloudTypeWeights(unittest.TestCase):
    """Unit testing the functions to convert msg flags to pps (old) flags."""

    def setUp(self):
        """Set up."""
        return

    def test_cloudtype_weights(self):
        """Test the derivation of weights for a given cloudtype, flags, obs times etc."""
        retv = get_weight_cloudtype(
            CTYPE_MSG, CTYPE_MSG_FLAG, LAT_MSG, TDIFF_MSG, IS_MSG_MSG)
        assert np.allclose(retv, WEIGHT_MSG)

        retv = get_weight_cloudtype(
            CTYPE_MSG, CTYPE_MSG_FLAG, LAT_MSG2, TDIFF_MSG, IS_MSG_MSG)
        assert np.allclose(retv, WEIGHT_MSG2)

    def tearDown(self):
        """Clean up."""
        return


class TestTimeTools:
    """Test (time) arithmetics for observation time and listing/sorting of PPS/MSG scenes."""

    def test_pps_metadata(self, tmp_path):
        """Test operations on the PPS meta data class."""
        filename = tmp_path / "my_pps_testfile.nc"
        geofilename = tmp_path / "my_pps_geo_testfile.nc"
        platform_name = "NOAA-20"
        orbit = "00102"
        timeslot1 = datetime(2019, 11, 5, 12, 0)
        variant = None
        pm1 = PpsMetaData(filename, geofilename, platform_name, orbit, timeslot1, variant)

        timeslot2 = datetime(2019, 11, 5, 13, 30)
        orbit = "00103"
        pm2 = PpsMetaData(filename, geofilename, platform_name, orbit, timeslot2, variant)

        timeslot3 = datetime(2019, 11, 5, 10, 30)
        orbit = "00101"
        pm3 = PpsMetaData(filename, geofilename, platform_name, orbit, timeslot3, variant)

        timeslot4 = datetime(2019, 11, 5, 12, 0)
        orbit = "00999"
        platform_name = "EOS-Aqua"
        pm4 = PpsMetaData(filename, geofilename, platform_name, orbit, timeslot4, variant)

        pmlist = [pm1, pm2, pm3, pm4]
        pmlist.sort()
        tslots = [p.timeslot for p in pmlist]
        norbits = [int(p.orbit) for p in pmlist]

        assert [101, 102, 999, 103] == norbits
        assert tslots[0] <= tslots[1]
        assert tslots[1] <= tslots[2]
        assert tslots[2] <= tslots[3]

    def test_geo_metadata(self, tmp_path):
        """Test operations on the NWCSAF/Geo meta data class."""
        filename = tmp_path / "S_NWC_CTTH_MSG4_MSG-N-VISIR_20191105T180000Z_PLAX.nc"
        timeslot1 = datetime(2019, 11, 5, 18, 0)

        mda = GeoMetaData(filename, "Meteosat-11", "some_area", timeslot1)

        assert isinstance(mda.uri, pathlib.PosixPath)
        assert mda.uri.name == "S_NWC_CTTH_MSG4_MSG-N-VISIR_20191105T180000Z_PLAX.nc"
        assert mda.timeslot == datetime(2019, 11, 5, 18, 0)
        assert mda.platform_name == "Meteosat-11"
        assert mda.areaid == "some_area"

    def test_geo_metadata_several_timeslots(self, tmp_path):
        """Test operations on the NWCSAF/Geo meta data class - several timeslots."""
        filename = tmp_path / "S_NWC_CTTH_MSG4_MSG-N-VISIR_20191105T180000Z_PLAX.nc"
        platform_name = "Meteosat-11"
        timeslot1 = datetime(2019, 11, 5, 18, 0)
        areaid = "area1"
        mm1 = GeoMetaData(filename, platform_name, areaid, timeslot1)

        timeslot2 = datetime(2019, 11, 4, 18, 0)
        areaid = "area2"
        mm2 = GeoMetaData(filename, platform_name, areaid, timeslot2)

        timeslot3 = datetime(2019, 11, 5, 18, 15)
        areaid = "area3"
        mm3 = GeoMetaData(filename, platform_name, areaid, timeslot3)

        timeslot4 = datetime(2019, 11, 3, 12, 0)
        areaid = "area4"
        mm4 = GeoMetaData(filename, platform_name, areaid, timeslot4)

        timeslot5 = datetime(2019, 11, 5, 18, 15)
        areaid = "area5"
        platform_name = "Meteosat-9"
        mm5 = GeoMetaData(filename, platform_name, areaid, timeslot5)

        mmlist = [mm1, mm2, mm3, mm4, mm5]
        mmlist.sort()
        tslots = [m.timeslot for m in mmlist]
        areas = [m.areaid for m in mmlist]

        assert ["area4", "area2", "area1", "area3", "area5"] == areas
        assert tslots[0] <= tslots[1]
        assert tslots[1] <= tslots[2]
        assert tslots[2] <= tslots[3]
        assert tslots[3] <= tslots[4]

    def test_get_analysis_time(self):
        """Test the determination of the analysis time from two times defining a time interval."""
        dtime_eps = timedelta(seconds=1)

        t1_ = datetime(2015, 6, 23, 12, 22)
        t2_ = datetime(2015, 6, 23, 12, 35)
        res = get_analysis_time(t1_, t2_)
        assert abs(res - datetime(2015, 6, 23, 12, 0)) < dtime_eps

        res = get_analysis_time(t1_, t2_, minutes_resolution=15)
        assert abs(res - datetime(2015, 6, 23, 12, 30)) < dtime_eps

        t1_ = datetime(2015, 6, 23, 12, 42)
        t2_ = datetime(2015, 6, 23, 12, 55)
        res = get_analysis_time(t1_, t2_)
        assert abs(res - datetime(2015, 6, 23, 13, 0)) < dtime_eps

        res = get_analysis_time(t1_, t2_, minutes_resolution=15)
        assert abs(res - datetime(2015, 6, 23, 12, 45)) < dtime_eps

        t1_ = datetime(2015, 6, 23, 12, 48)
        t2_ = datetime(2015, 6, 23, 13, 1)
        res = get_analysis_time(t1_, t2_)
        assert res - datetime(2015, 6, 23, 13, 0) < dtime_eps

        res = get_analysis_time(t1_, t2_, minutes_resolution=15)
        assert res - datetime(2015, 6, 23, 13, 0) < dtime_eps

        t1_ = datetime(2015, 6, 23, 13, 10)
        t2_ = datetime(2015, 6, 23, 13, 25)
        res = get_analysis_time(t1_, t2_)
        assert res - datetime(2015, 6, 23, 13, 0) < dtime_eps

        res = get_analysis_time(t1_, t2_, minutes_resolution=15)
        assert res - datetime(2015, 6, 23, 13, 15) < dtime_eps
