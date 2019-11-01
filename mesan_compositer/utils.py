#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21529.ad.smhi.se>

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

"""Miscellaneous tools/utilities taken from mpop
"""

import os
import socket
import netifaces
from six.moves.urllib.parse import urlparse
import numpy as np
import logging
LOG = logging.getLogger(__name__)


def get_bit_from_flags(arr, nbit):
    """I don't know what this function does.
    """
    res = np.bitwise_and(np.right_shift(arr, nbit), 1)
    return res.astype('b')


def ctth_procflags2pps(data):
    """Convert ctth processing flags from MSG to PPS format.
    """

    ones = np.ones(data.shape, "h")

    # 2 bits to define processing status
    # (maps to pps bits 0 and 1:)
    is_bit0_set = get_bit_from_flags(data, 0)
    is_bit1_set = get_bit_from_flags(data, 1)
    proc = (is_bit0_set * np.left_shift(ones, 0) +
            is_bit1_set * np.left_shift(ones, 1))
    del is_bit0_set
    del is_bit1_set

    # Non-processed?
    # If non-processed in msg (0) then set pps bit 0 and nothing else.
    # If non-processed in msg due to FOV is cloud free (1) then do not set any
    # pps bits.
    # If processed (because cloudy) with/without result in msg (2&3) then set
    # pps bit 1.

    arr = np.where(np.equal(proc, 0), np.left_shift(ones, 0), 0)
    arr = np.where(np.equal(proc, 2), np.left_shift(ones, 1), 0)
    arr = np.where(np.equal(proc, 3), np.left_shift(ones, 1), 0)
    retv = np.array(arr)
    del proc

    # 1 bit to define if RTTOV-simulations are available?
    # (maps to pps bit 3:)
    is_bit2_set = get_bit_from_flags(data, 2)
    proc = is_bit2_set

    # RTTOV-simulations available?

    arr = np.where(np.equal(proc, 1), np.left_shift(ones, 3), 0)
    retv = np.add(retv, arr)
    del is_bit2_set

    # 3 bits to describe NWP input data
    # (maps to pps bits 4&5:)
    is_bit3_set = get_bit_from_flags(data, 3)
    is_bit4_set = get_bit_from_flags(data, 4)
    is_bit5_set = get_bit_from_flags(data, 5)
    # Put together the three bits into a nwp-flag:
    nwp_bits = (is_bit3_set * np.left_shift(ones, 0) +
                is_bit4_set * np.left_shift(ones, 1) +
                is_bit5_set * np.left_shift(ones, 2))
    arr = np.where(np.logical_and(np.greater_equal(nwp_bits, 3),
                                  np.less_equal(nwp_bits, 5)),
                   np.left_shift(ones, 4),
                   0)
    arr = np.add(arr, np.where(np.logical_or(np.equal(nwp_bits, 2),
                                             np.equal(nwp_bits, 4)),
                               np.left_shift(ones, 5),
                               0))

    retv = np.add(retv, arr)
    del is_bit3_set
    del is_bit4_set
    del is_bit5_set

    # 2 bits to describe SEVIRI input data
    # (maps to pps bits 6:)
    is_bit6_set = get_bit_from_flags(data, 6)
    is_bit7_set = get_bit_from_flags(data, 7)
    # Put together the two bits into a seviri-flag:
    seviri_bits = (is_bit6_set * np.left_shift(ones, 0) +
                   is_bit7_set * np.left_shift(ones, 1))
    arr = np.where(np.greater_equal(seviri_bits, 2),
                   np.left_shift(ones, 6), 0)

    retv = np.add(retv, arr)
    del is_bit6_set
    del is_bit7_set

    # 4 bits to describe which method has been used
    # (maps to pps bits 7&8 and bit 2:)
    is_bit8_set = get_bit_from_flags(data, 8)
    is_bit9_set = get_bit_from_flags(data, 9)
    is_bit10_set = get_bit_from_flags(data, 10)
    is_bit11_set = get_bit_from_flags(data, 11)
    # Put together the four bits into a method-flag:
    method_bits = (is_bit8_set * np.left_shift(ones, 0) +
                   is_bit9_set * np.left_shift(ones, 1) +
                   is_bit10_set * np.left_shift(ones, 2) +
                   is_bit11_set * np.left_shift(ones, 3))
    arr = np.where(np.logical_or(
        np.logical_and(np.greater_equal(method_bits, 1),
                       np.less_equal(method_bits, 2)),
        np.equal(method_bits, 13)),
        np.left_shift(ones, 2),
        0)
    arr = np.add(arr,
                 np.where(np.equal(method_bits, 1),
                          np.left_shift(ones, 7),
                          0))
    arr = np.add(arr,
                 np.where(np.logical_and(
                     np.greater_equal(method_bits, 3),
                     np.less_equal(method_bits, 12)),
                     np.left_shift(ones, 8),
                     0))

    # (Maps directly - as well - to the spare bits 9-12)
    arr = np.add(arr, np.where(is_bit8_set, np.left_shift(ones, 9), 0))
    arr = np.add(arr, np.where(is_bit9_set,
                               np.left_shift(ones, 10),
                               0))
    arr = np.add(arr, np.where(is_bit10_set,
                               np.left_shift(ones, 11),
                               0))
    arr = np.add(arr, np.where(is_bit11_set,
                               np.left_shift(ones, 12),
                               0))
    retv = np.add(retv, arr)
    del is_bit8_set
    del is_bit9_set
    del is_bit10_set
    del is_bit11_set

    # 2 bits to describe the quality of the processing itself
    # (maps to pps bits 14&15:)
    is_bit12_set = get_bit_from_flags(data, 12)
    is_bit13_set = get_bit_from_flags(data, 13)
    # Put together the two bits into a quality-flag:
    qual_bits = (is_bit12_set * np.left_shift(ones, 0) +
                 is_bit13_set * np.left_shift(ones, 1))
    arr = np.where(np.logical_and(np.greater_equal(qual_bits, 1),
                                  np.less_equal(qual_bits, 2)),
                   np.left_shift(ones, 14), 0)
    arr = np.add(arr,
                 np.where(np.equal(qual_bits, 2),
                          np.left_shift(ones, 15),
                          0))

    retv = np.add(retv, arr)
    del is_bit12_set
    del is_bit13_set

    return retv.astype('h')


def proj2cf(proj_dict):
    """Return the cf grid mapping from a proj dict.

    Description of the cf grid mapping:
    http://cf-pcmdi.llnl.gov/documents/cf-conventions/1.4/ch05s06.html

    Table of the available grid mappings:
    http://cf-pcmdi.llnl.gov/documents/cf-conventions/1.4/apf.html
    """

    cases = {"geos": geos2cf,
             "stere": stere2cf,
             "merc": merc2cf,
             "aea": aea2cf,
             "laea": laea2cf,
             "ob_tran": obtran2cf,
             "eqc": eqc2cf, }

    return cases[proj_dict["proj"]](proj_dict)


def geos2cf(proj_dict):
    """Return the cf grid mapping from a geos proj dict.
    """

    return {"grid_mapping_name": "geostationary",
            "latitude_of_projection_origin": 0.0,
            "longitude_of_projection_origin": eval(proj_dict["lon_0"]),
            "semi_major_axis": eval(proj_dict["a"]),
            "semi_minor_axis": eval(proj_dict["b"]),
            "perspective_point_height": eval(proj_dict["h"])
            }


def eqc2cf(proj_dict):
    """Return the cf grid mapping from a eqc proj dict. However, please be
    aware that this is not an official CF projection. See
    http://cf-pcmdi.llnl.gov/documents/cf-conventions/1.4/apf.html.
    """

    return {"grid_mapping_name": "equirectangular",
            "latitude_of_true_scale": eval(proj_dict.get("lat_ts", "0")),
            "latitude_of_projection_origin": eval(proj_dict["lat_0"]),
            "longitude_of_projection_origin": eval(proj_dict["lon_0"]),
            "false_easting": eval(proj_dict.get("x_0", "0")),
            "false_northing": eval(proj_dict.get("y_0", "0"))
            }


def stere2cf(proj_dict):
    """Return the cf grid mapping from a stereographic proj dict.
    """

    return {"grid_mapping_name": "stereographic",
            "latitude_of_projection_origin": eval(proj_dict["lat_0"]),
            "longitude_of_projection_origin": eval(proj_dict["lon_0"]),
            "scale_factor_at_projection_origin": eval(
                proj_dict.get("x_0", "1.0")),
            "false_easting": eval(proj_dict.get("x_0", "0")),
            "false_northing": eval(proj_dict.get("y_0", "0"))
            }


def merc2cf(proj_dict):
    """Return the cf grid mapping from a mercator proj dict.
    """

    raise NotImplementedError(
        "CF grid mapping from a PROJ.4 mercator projection is not implemented")


def aea2cf(proj_dict):
    """Return the cf grid mapping from a Albers Equal Area proj dict.
    """

    #standard_parallels = []
    # for item in ['lat_1', 'lat_2']:
    #    if item in proj_dict:
    #        standard_parallels.append(eval(proj_dict[item]))
    if 'lat_2' in proj_dict:
        standard_parallel = [eval(proj_dict['lat_1']),
                             eval(proj_dict['lat_2'])]
    else:
        standard_parallel = [eval(proj_dict['lat_1'])]

    lat_0 = 0.0
    if 'lat_0' in proj_dict:
        lat_0 = eval(proj_dict['lat_0'])

    x_0 = 0.0
    if 'x_0' in proj_dict:
        x_0 = eval(proj_dict['x_0'])

    y_0 = 0.0
    if 'y_0' in proj_dict:
        y_0 = eval(proj_dict['y_0'])

    retv = {"grid_mapping_name": "albers_conical_equal_area",
            "standard_parallel": standard_parallel,
            "latitude_of_projection_origin": lat_0,
            "longitude_of_central_meridian": eval(proj_dict["lon_0"]),
            "false_easting": x_0,
            "false_northing": y_0
            }

    retv = build_dict("albers_conical_equal_area",
                      proj_dict,
                      standard_parallel=["lat_1", "lat_2"],
                      latitude_of_projection_origin="lat_0",
                      longitude_of_central_meridian="lon_0",
                      false_easting="x_0",
                      false_northing="y_0")

    return retv


def laea2cf(proj_dict):
    """Return the cf grid mapping from a Lambert azimuthal equal-area proj dict.
    http://trac.osgeo.org/gdal/wiki/NetCDF_ProjectionTestingStatus
    """
    x_0 = eval(proj_dict.get('x_0', '0.0'))
    y_0 = eval(proj_dict.get('y_0', '0.0'))

    retv = {"grid_mapping_name": "lambert_azimuthal_equal_area",
            "longitude_of_projection_origin": eval(proj_dict["lon_0"]),
            "latitude_of_projection_origin": eval(proj_dict["lat_0"]),
            "false_easting": x_0,
            "false_northing": y_0
            }

    retv = build_dict("lambert_azimuthal_equal_area",
                      proj_dict,
                      longitude_of_projection_origin="lon_0",
                      latitude_of_projection_origin="lat_0",
                      false_easting="x_0",
                      false_northing="y_0")

    return retv


def obtran2cf(proj_dict):
    """Return a grid mapping from a rotated pole grid (General Oblique
    Transformation projection) proj dict.

    Please be aware this is not yet supported by CF!
    """
    LOG.warning("The General Oblique Transformation " +
                "projection is not CF compatible yet...")
    x_0 = float(proj_dict.get('x_0', 0.0))
    y_0 = float(proj_dict.get('y_0', 0.0))

    retv = {"grid_mapping_name": "general_oblique_transformation",
            "longitude_of_projection_origin": float(proj_dict["lon_0"]),
            "grid_north_pole_latitude": float(proj_dict.get("o_lat_p")),
            "grid_north_pole_longitude": float(proj_dict.get("o_lon_p")),
            "false_easting": x_0,
            "false_northing": y_0
            }

    retv = build_dict("general_oblique_transformation",
                      proj_dict,
                      longitude_of_projection_origin="lon_0",
                      grid_north_pole_latitude="o_lat_p",
                      grid_north_pole_longitude="o_lon_p",
                      false_easting="x_0",
                      false_northing="y_0")

    return retv


def build_dict(proj_name, proj_dict, **kwargs):
    new_dict = {}
    new_dict["grid_mapping_name"] = proj_name

    for key, val in kwargs.items():
        if isinstance(val, (list, tuple)):
            new_dict[key] = [proj_dict.get(x) for x in val if x in proj_dict]
        elif val in proj_dict:
            new_dict[key] = float(proj_dict.get(val))
    # add a, b, rf and/or ellps
    if "a" in proj_dict:
        new_dict["semi_major_axis"] = proj_dict.get("a")
    if "b" in proj_dict:
        new_dict["semi_minor_axis"] = proj_dict.get("b")
    if "rf" in proj_dict:
        new_dict["inverse_flattening"] = proj_dict.get("rf")
    if "ellps" in proj_dict:
        new_dict["ellipsoid"] = proj_dict.get("ellps")

    return new_dict


def aeqd2cf(proj_dict):
    return build_dict("azimuthal_equidistant",
                      proj_dict,
                      standard_parallel=["lat_1", "lat_2"],
                      latitude_of_projection_origin="lat_0",
                      longitude_of_central_meridian="lon_0",
                      false_easting="x_0",
                      false_northing="y_0")


def check_uri(uri):
    """Check that the provided *uri* is on the local host and return the
    file path.
    """
    if isinstance(uri, (list, set, tuple)):
        paths = [check_uri(ressource) for ressource in uri]
        return paths
    url = urlparse(uri)
    try:
        if url.hostname:
            url_ip = socket.gethostbyname(url.hostname)

            if url_ip not in get_local_ips():
                try:
                    os.stat(url.path)
                except OSError:
                    raise IOError(
                        "Data file %s unaccessible from this host" % uri)

    except socket.gaierror:
        LOG.warning("Couldn't check file location, running anyway")

    return url.path


def get_local_ips():
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips
