#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014-2023 Adam.Dybbroe

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

"""Unit tests for the finding and selection of NWCSAF Geo and PPS scenes."""

import logging
from datetime import datetime, timedelta

import pytest

from mesan_compositer.config import get_config
from mesan_compositer.make_ct_composite import ctCompositer


class TestFindCtScenes:
    """Test finding the correct NWCSAF Geo/PPS scenes."""

    @pytest.fixture(autouse=True)
    def _setup_method(self, fake_yamlconfig_file):
        """Set the common test env."""
        self.config = get_config(fake_yamlconfig_file)
        self.area_id = "mesanEx"

    def test_get_geo_ctype_scenes(self, caplog, fake_empty_nwcsaf_geo_files):
        """Test get the nwcsaf/geo cloud type files for a time window and check the metadata."""
        time_of_analysis = datetime(2023, 1, 16, 11, 0)
        delta_time_window = timedelta(minutes=20)

        path = str(fake_empty_nwcsaf_geo_files[0].parent)
        self.config["msg_dir"] = path

        ctcomp = ctCompositer(time_of_analysis, delta_time_window, "unknown_area", self.config)
        with caplog.at_level(logging.ERROR):
            ctcomp.get_catalogue()

        assert "Too few PPS DR files found!" in caplog.text
        assert "CRITICAL" in caplog.text

        assert len(ctcomp.pps_scenes) == 0
        assert len(ctcomp.msg_scenes) == 1

        result = str(ctcomp.msg_scenes[0]).split("\n")
        expected = ["filename={filepath}/S_NWC_CT_MSG4_MSG-N-VISIR_20230116T110000Z_PLAX.nc".format(filepath=path),
                    "platform_name=Meteosat-11",
                    "areaid=MSG-N",
                    "timeslot=2023-01-16 11:00:00"]
        assert result == expected

    def test_get_geo_ctype_scenes_unknown_area(self, caplog, fake_empty_nwcsaf_geo_files):
        """Test get the nwcsaf/geo cloud type files for a time window but for an unknown area."""
        time_of_analysis = datetime(2023, 1, 16, 11, 0)
        delta_time_window = timedelta(minutes=20)

        path = str(fake_empty_nwcsaf_geo_files[0].parent)
        self.config["msg_dir"] = path

        self.config["msg_areaname"] = "UNKNOWN-AREA-ID"
        ctcomp = ctCompositer(time_of_analysis, delta_time_window, self.area_id, self.config)
        geo_file_list = [str(f) for f in fake_empty_nwcsaf_geo_files]

        with caplog.at_level(logging.DEBUG):
            ctcomp.get_geo_scenes(geo_file_list)

        assert "File name format not supported/requested:" in caplog.text
        assert "Area id MSG-N not requested (UNKNOWN-AREA-ID)" in caplog.text
        assert len(ctcomp.msg_scenes) == 0

    def test_get_pps_ctype_scenes(self, fake_empty_nwcsaf_pps_files):
        """Test get the pps cloud type files for a time window and check the metadata."""
        time_of_analysis = datetime(2015, 6, 23, 0, 0)
        delta_time_window = timedelta(minutes=35)

        path = str(fake_empty_nwcsaf_pps_files[0].parent)
        self.config["pps_direct_readout_dir"] = path
        ctcomp = ctCompositer(time_of_analysis, delta_time_window, self.area_id, self.config)
        ctcomp.get_catalogue()

        assert len(ctcomp.msg_scenes) == 0
        assert len(ctcomp.pps_scenes) == 2
        result = str(ctcomp.pps_scenes[0]).split("\n")

        expected = ["filename={directory}/S_NWC_CT_noaa20_00001_20150622T2324597Z_20150622T2338100Z.nc".format(
            directory=path),
            "geofilename={directory}/S_NWC_CMA_noaa20_00001_20150622T2324597Z_20150622T2338100Z.nc".format(
            directory=path),
            "platform_name=NOAA-20",
            "orbit=00001",
            "timeslot=2015-06-22 23:31:34.850000",
            "variant=None"]
        assert result == expected

        result = str(ctcomp.pps_scenes[1]).split("\n")
        expected = ["filename={directory}/S_NWC_CT_noaa19_32830_20150622T2354597Z_20150623T0008100Z.nc".format(
            directory=path),
            "geofilename={directory}/S_NWC_CMA_noaa19_32830_20150622T2354597Z_20150623T0008100Z.nc".format(
            directory=path),
            "platform_name=NOAA-19",
            "orbit=32830",
            "timeslot=2015-06-23 00:01:34.850000",
            "variant=None"]
        assert result == expected

    @pytest.mark.parametrize(("time_of_analysis", "minutes", "expected"),
                             [(datetime(2023, 1, 16, 11, 0), 35, ["2023-01-16 11:00:00"]),
                              (datetime(2023, 1, 16, 11, 0), 10, ["2023-01-16 11:00:00"]),
                              (datetime(2023, 1, 15, 11, 0), 100, []),
                              (datetime(2023, 1, 16, 11, 0), 25, ["2023-01-16 11:00:00"])
                              ]
                             )
    def test_get_correct_list_of_nwcsaf_geo_ct_scenes_from_filelist(self, fake_empty_nwcsaf_geo_files,
                                                                    time_of_analysis, minutes, expected):
        """Test get the correct nwcsaf/Geo CT scenes given a list of fake files."""
        path = str(fake_empty_nwcsaf_geo_files[0].parent)
        self.config["msg_dir"] = path

        delta_time_window = timedelta(minutes=minutes)
        ctcomp = ctCompositer(time_of_analysis, delta_time_window, self.area_id, self.config)
        ctcomp.get_catalogue()

        assert len(ctcomp.msg_scenes) == len(expected)

        for scene in ctcomp.msg_scenes:
            assert str(scene.timeslot) in expected

    @pytest.mark.parametrize(("time_of_analysis", "minutes", "expected"),
                             [(datetime(2015, 6, 23, 0, 0), 35, ["NOAA-20", "NOAA-19"]),
                              (datetime(2015, 6, 23, 0, 0), 10, ["NOAA-19"]),
                              (datetime(2015, 6, 22, 12, 0), 100, []),
                              (datetime(2015, 6, 23, 0, 0), 45, ["NOAA-20", "NOAA-19", "Suomi-NPP"]),
                              ]
                             )
    def test_get_correct_list_of_nwcsaf_pps_ct_scenes_from_filelist(self, fake_empty_nwcsaf_pps_files,
                                                                    time_of_analysis, minutes, expected):
        """Test get the correct nwcsaf/pps CT scenes given a list of fake files."""
        self.config["pps_direct_readout_dir"] = str(fake_empty_nwcsaf_pps_files[0].parent)

        delta_time_window = timedelta(minutes=minutes)
        ctcomp = ctCompositer(time_of_analysis, delta_time_window, self.area_id, self.config)
        ctcomp.get_catalogue()

        assert len(ctcomp.pps_scenes) == len(expected)

        for scene in ctcomp.pps_scenes:
            assert scene.platform_name in expected
