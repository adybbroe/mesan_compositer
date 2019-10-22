#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015, 2018, 2019 Adam.Dybbroe

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

"""Defining the netCDF4 mesan-composite object with read and write methods
"""

import logging
import numpy as np
from utils import proj2cf
import xarray as xr
from datetime import datetime

LOG = logging.getLogger(__name__)

TIME_UNITS = "seconds since 1970-01-01 00:00:00"
CF_FLOAT_TYPE = np.float64
CF_DATA_TYPE = np.int16
# To be complete, get from appendix F of cf conventions
MAPPING_ATTRIBUTES = {'grid_mapping_name': "proj",
                      'standard_parallel': ["lat_1", "lat_2"],
                      'latitude_of_projection_origin': "lat_0",
                      'longitude_of_projection_origin': "lon_0",
                      'longitude_of_central_meridian': "lon_0",
                      'grid_north_pole_longitude': 'o_lon_p',
                      'grid_north_pole_latitude': 'o_lat_p',
                      'perspective_point_height': "h",
                      'false_easting': "x_0",
                      'false_northing': "y_0",
                      'semi_major_axis': "a",
                      'semi_minor_axis': "b",
                      'inverse_flattening': "rf",
                      'ellipsoid': "ellps",  # not in CF conventions...
                      }

# To be completed, get from appendix F of cf conventions
PROJNAME = {"vertical_perspective": "nsper",
            "geostationary": "geos",
            "albers_conical_equal_area": "aea",
            "azimuthal_equidistant": "aeqd",
            "equirectangular": "eqc",
            "transverse_mercator": "tmerc",
            "stereographic": "stere",
            "general_oblique_transformation": "ob_tran"
            }


class InfoObject(object):

    """Simple data and info container.
    """

    def __init__(self):
        self.info = {}
        self.data = None


class ncCloudTypeComposite(object):

    """netcdf cloud type composite object"""

    def __init__(self):

        self.info = {}
        #self.info["Conventions"] = "CF-1.6"
        self.info["Conventions"] = "Undefined"

        self.time = InfoObject()
        self.cloudtype = InfoObject()
        self.weight = InfoObject()
        self.id = InfoObject()
        self.area = InfoObject()
        self.area_def = None

    def store(self, comp_dict, area_obj, product_id='MSG/PPS Cloud Type composite'):
        """Store the composite into the object"""

        self.info["product"] = product_id
        self.area_def = area_obj

        resolution = 1000  # FIXME!
        str_res = '1000 m'
        dim_names = ['y' + str_res, 'x' + str_res]

        self.time.data = comp_dict["time"]
        valid_min, valid_max = (self.time.data.min(), self.time.data.max())
        self.time.info = {"var_name": "time",
                          "var_data": self.time.data,
                          "var_dim_names": dim_names,
                          "long_name": "observation time of best cloud type",
                          "standard_name": "time",
                          "valid_range": np.array([valid_min, valid_max]),
                          "units": TIME_UNITS}

        self.cloudtype.data = comp_dict["cloudtype"]
        valid_min, valid_max = (0, 20)
        self.cloudtype.info = {"var_name": "cloudtype",
                               "var_data": self.cloudtype.data,
                               'var_dim_names': dim_names,
                               "standard_name": "Cloudtype",
                               "valid_range": np.array([valid_min, valid_max]),
                               "resolution": resolution}
        self.cloudtype.info["description"] = 'NWCSAF cloudtype classification'

        # Weight:
        self.weight.data = comp_dict["weight"]
        valid_min, valid_max = (self.weight.data.min(), self.weight.data.max())
        self.weight.info = {"var_name": "weight",
                            "var_data": self.weight.data,
                            'var_dim_names': dim_names,
                            "standard_name": "Cloud Type weight",
                            "valid_range": np.array([valid_min, valid_max]),
                            "resolution": 1000}
        self.weight.info["description"] = "Weight of the best Cloud Type"

        # Id:
        self.id.data = comp_dict["id"]
        valid_min, valid_max = (self.id.data.min(), self.id.data.max())
        self.id.info = {"var_name": "id",
                        "var_data": self.id.data,
                        'var_dim_names': dim_names,
                        "standard_name": "Cloud Type id",
                        "valid_range": np.array([valid_min, valid_max]),
                        "resolution": 1000}
        self.id.info[
            "description"] = "Id (pps=0 or msg=1) of the best Cloud Type"

        # Grid mapping:
        self.area.data = 0
        self.area.info = {"var_name": 'area',
                          "var_data": self.area.data,
                          "var_dim_names": ()}

        self.area.info.update(proj2cf(area_obj.proj_dict))

        setattr(self, self.area.info["var_name"], self.area)

        # x__ = InfoObject()
        # y__ = InfoObject()

        # x__.data, y__.data = area_obj.get_proj_coords_dask()

        # x__.info = {"var_name": "x" + str_res,
        #             "var_data": x__.data,
        #             "var_dim_names": ("x" + str_res,),
        #             "units": "m",
        #             "standard_name": "projection_x_coordinate",
        #             "long_name": "x coordinate of projection"}
        # setattr(self, x__.info["var_name"], x__)

        # y__.info = {"var_name": "y" + str_res,
        #             "var_data": y__.data,
        #             "var_dim_names": ("y" + str_res,),
        #             "units": "m",
        #             "standard_name": "projection_y_coordinate",
        #             "long_name": "y coordinate of projection"}
        # setattr(self, y__.info["var_name"], y__)

        self.cloudtype.info["grid_mapping"] = self.area.info["var_name"]
        self.weight.info["grid_mapping"] = self.area.info["var_name"]
        self.id.info["grid_mapping"] = self.area.info["var_name"]
        self.time.info["grid_mapping"] = self.area.info["var_name"]

    def write(self, filename):
        """Write the data to netCDF file"""

        other_to_netcdf_kwargs = {'compute': True}
        root = xr.Dataset({}, attrs={'history': 'Created by mesan_compositor on {}'.format(datetime.utcnow()),
                                     'Conventions': 'Undefined'})
        engine = 'netcdf4'

        xcoord, ycoord = self.area_def.get_proj_vectors()
        attrs = get_nc_attributes_from_object(self.cloudtype.info)

        ctype = xr.DataArray(data=self.cloudtype.data, dims=['y1000 m', 'x1000 m'], attrs=attrs)

        attrs = get_nc_attributes_from_object(self.id.info)
        data_id = xr.DataArray(data=self.id.data, dims=['y1000 m', 'x1000 m'], attrs=attrs)

        attrs = get_nc_attributes_from_object(self.time.info)
        data_time = xr.DataArray(data=self.time.data, dims=['y1000 m', 'x1000 m'], attrs=attrs)

        attrs = get_nc_attributes_from_object(self.weight.info)
        weight = xr.DataArray(data=self.weight.data, dims=['y1000 m', 'x1000 m'], attrs=attrs)

        attrs = get_nc_attributes_from_object(self.area.info)
        area_data = xr.DataArray(data=self.area.data, dims=None, attrs=attrs)

        _ = root.to_netcdf(filename, engine=engine, mode='w')

        data_arrays = [ctype, data_id, data_time, weight, area_data]
        data_names = ['cloudtype', 'id', 'time', 'weight', 'area']

        encodings = {'dtype': ctype.dtype, 'scale_factor': 1, 'zlib': True,
                     'complevel': 4, '_FillValue': 255, 'add_offset': 0}
        encodings2 = {'complevel': 4, 'zlib': True}

        dataset_dict = {}
        encoding = {}
        for dataset_name, data_array in zip(data_names, data_arrays):
            dataset_dict[dataset_name] = data_array
            if dataset_name in ['cloudtype', 'id']:
                encoding[dataset_name] = encodings
            elif dataset_name not in ['area']:
                encoding[dataset_name] = encodings2

        dataset = xr.Dataset(dataset_dict, coords={'y1000 m': ycoord, 'x1000 m': xcoord})
        _ = dataset.to_netcdf(filename, engine=engine, group=None, mode='a',
                              encoding=encoding, **other_to_netcdf_kwargs)

        return

    def load(self, filename):
        """Read the cloudtype composite from file"""

        import numpy as np
        from netCDF4 import Dataset

        rootgrp = Dataset(filename, 'r')

        self.info["Conventions"] = rootgrp.Conventions
        # self.info["product"] = rootgrp.product
        self.info["product"] = 'Unknown'

        for var_name in rootgrp.variables.keys():
            LOG.debug(str(var_name))
            var = rootgrp.variables[str(var_name)]
            dims = var.dimensions

            cnt = 0
            for cnt, dim in enumerate(dims):
                if dim.startswith("x") or dim.startswith("y"):
                    break

            if hasattr(self, str(var_name)):
                LOG.debug("set data...")
                item = getattr(self, str(var_name))
                setattr(item, 'data', var[:])

                info = {}
                for attr in var.ncattrs():
                    info[str(attr)] = getattr(var, str(attr))
                    setattr(item, 'info', info)

            area = None
            try:
                area_var_name = getattr(var, "grid_mapping")
                area_var = rootgrp.variables[area_var_name]
                proj4_dict = {}
                for attr, projattr in MAPPING_ATTRIBUTES.items():
                    try:
                        the_attr = getattr(area_var, attr)
                        if projattr == "proj":
                            proj4_dict[projattr] = PROJNAME[the_attr]
                        elif(isinstance(projattr, (list, tuple))):
                            try:
                                for i, subattr in enumerate(the_attr):
                                    proj4_dict[projattr[i]] = subattr
                            except TypeError:
                                proj4_dict[projattr[0]] = the_attr
                        else:
                            proj4_dict[projattr] = the_attr
                    except AttributeError:
                        pass
                y_name, x_name = dims[:cnt] + dims[cnt:]
                x__ = rootgrp.variables[x_name][:]
                y__ = rootgrp.variables[y_name][:]

                if proj4_dict["proj"] == "ob_tran":
                    proj4_dict["o_proj"] = 'eqc'  # FIXME!
                elif proj4_dict["proj"] == "geos":
                    x__ *= proj4_dict["h"]
                    y__ *= proj4_dict["h"]

                x_pixel_size = abs((np.diff(x__)).mean())
                y_pixel_size = abs((np.diff(y__)).mean())

                llx = x__[0] - x_pixel_size / 2.0
                lly = y__[-1] - y_pixel_size / 2.0
                urx = x__[-1] + x_pixel_size / 2.0
                ury = y__[0] + y_pixel_size / 2.0

                area_extent = (llx, lly, urx, ury)
                try:
                    # create the pyresample areadef
                    from pyresample.geometry import AreaDefinition
                    area = AreaDefinition("myareaid", "myareaname",
                                          "myprojid", proj4_dict,
                                          len(x__), len(y__),
                                          area_extent)
                    self.area_def = area
                except ImportError:
                    LOG.error("Pyresample not found, "
                              "cannot load area descrition")
                LOG.info("Grid mapping found and used")
            except AttributeError:
                LOG.info("No grid mapping found")


class ncCTTHComposite(ncCloudTypeComposite):

    """netcdf ctth composite object"""

    def __init__(self):
        self.info = {}
        #self.info["Conventions"] = "CF-1.6"
        self.info["Conventions"] = "Undefined"

        self.time = InfoObject()
        self.temperature = InfoObject()
        self.height = InfoObject()
        self.pressure = InfoObject()
        self.weight = InfoObject()
        self.flags = InfoObject()
        self.id = InfoObject()
        self.area = InfoObject()
        self.area_def = None

    def store(self, comp_dict, area_obj, product_id='MSG/PPS CTTH composite'):
        """Store the composite into the object"""

        self.info["product"] = product_id
        self.area_def = area_obj

        resolution = 1000  # FIXME!
        str_res = '1000 m'
        dim_names = ['y' + str_res, 'x' + str_res]

        self.time.data = comp_dict["time"]
        valid_min, valid_max = (self.time.data.min(), self.time.data.max())
        self.time.info = {"var_name": "time",
                          "var_data": self.time.data,
                          "var_dim_names": dim_names,
                          "long_name": "observation time of best ctth value",
                          "standard_name": "time",
                          "valid_range": np.array([valid_min, valid_max]),
                          "units": TIME_UNITS}

        # Temperature
        self.temperature.data = comp_dict["temperature"]
        valid_min, valid_max = (
            self.temperature.data.min(), self.temperature.data.max())
        self.temperature.info = {"var_name": "temperature",
                                 "var_data": self.temperature.data,
                                 'var_dim_names': dim_names,
                                 "standard_name": "Temperature",
                                 "_FillValue": 0.0,
                                 "scale_factor": 1.0,
                                 "add_offset": 0.0,
                                 "valid_range": np.array([valid_min, valid_max]),
                                 "resolution": resolution}
        self.temperature.info["description"] = 'NWCSAF CTTH - temperature'

        # Height
        self.height.data = comp_dict["height"]
        valid_min, valid_max = (
            self.height.data.min(), self.height.data.max())
        self.height.info = {"var_name": "height",
                            "var_data": self.height.data,
                            'var_dim_names': dim_names,
                            "standard_name": "Height",
                            "_FillValue": 0.0,
                            "scale_factor": 1.0,
                            "add_offset": 0.0,
                            "valid_range": np.array([valid_min, valid_max]),
                            "resolution": resolution}
        self.height.info["description"] = 'NWCSAF CTTH - height'

        # Pressure
        self.pressure.data = comp_dict["pressure"]
        valid_min, valid_max = (
            self.pressure.data.min(), self.pressure.data.max())
        self.pressure.info = {"var_name": "pressure",
                              "var_data": self.pressure.data,
                              'var_dim_names': dim_names,
                              "standard_name": "Pressure",
                              "_FillValue": 0.0,
                              "scale_factor": 1.0,
                              "add_offset": 0.0,
                              "valid_range": np.array([valid_min, valid_max]),
                              "resolution": resolution}
        self.pressure.info["description"] = 'NWCSAF CTTH - pressure'

        # Weight:
        self.weight.data = comp_dict["weight"]
        valid_min, valid_max = (self.weight.data.min(), self.weight.data.max())
        self.weight.info = {"var_name": "weight",
                            "var_data": self.weight.data,
                            'var_dim_names': dim_names,
                            "standard_name": "CTTH weight",
                            "valid_range": np.array([valid_min, valid_max]),
                            "resolution": 1000}
        self.weight.info["description"] = "Weight of the best CTTH"

        # Processing flags:
        self.flags.data = comp_dict["flag"]
        valid_min, valid_max = (self.flags.data.min(), self.flags.data.max())
        self.flags.info = {"var_name": "flags",
                           "var_data": self.flags.data,
                           'var_dim_names': dim_names,
                           "standard_name": "CTTH processing flags",
                           "valid_range": np.array([valid_min, valid_max]),
                           "resolution": 1000}
        self.flags.info["description"] = "CTTH processing flags"

        # Id:
        self.id.data = comp_dict["id"]
        valid_min, valid_max = (self.id.data.min(), self.id.data.max())
        self.id.info = {"var_name": "id",
                        "var_data": self.id.data,
                        'var_dim_names': dim_names,
                        "standard_name": "CTTH id",
                        "valid_range": np.array([valid_min, valid_max]),
                        "resolution": 1000}
        self.id.info[
            "description"] = "Id (pps=0 or msg=1) of the best CTTH estimate"

        # Grid mapping:
        self.area.data = 0
        self.area.info = {"var_name": 'area',
                          "var_data": self.area.data,
                          "var_dim_names": ()}
        self.area.info.update(proj2cf(area_obj.proj_dict))

        setattr(self, self.area.info["var_name"], self.area)
        x__ = InfoObject()
        area_obj.get_proj_coords(cache=True)

        try:
            x__.data = area_obj.projection_x_coords[0, :]
        except IndexError:
            x__.data = area_obj.projection_x_coords
        x__.info = {"var_name": "x" + str_res,
                    "var_data": x__.data,
                    "var_dim_names": ("x" + str_res,),
                    "units": "m",
                    "standard_name": "projection_x_coordinate",
                    "long_name": "x coordinate of projection"}
        setattr(self, x__.info["var_name"], x__)

        y__ = InfoObject()
        try:
            y__.data = area_obj.projection_y_coords[:, 0]
        except IndexError:
            y__.data = area_obj.projection_y_coords

        y__.info = {"var_name": "y" + str_res,
                    "var_data": y__.data,
                    "var_dim_names": ("y" + str_res,),
                    "units": "m",
                    "standard_name": "projection_y_coordinate",
                    "long_name": "y coordinate of projection"}
        setattr(self, y__.info["var_name"], y__)

        self.temperature.info["grid_mapping"] = self.area.info["var_name"]
        self.height.info["grid_mapping"] = self.area.info["var_name"]
        self.pressure.info["grid_mapping"] = self.area.info["var_name"]
        self.weight.info["grid_mapping"] = self.area.info["var_name"]
        self.id.info["grid_mapping"] = self.area.info["var_name"]
        self.time.info["grid_mapping"] = self.area.info["var_name"]


def get_nc_attributes_from_object(info_dict):
    attrs = {}
    for key in info_dict.keys():
        if key in ['var_data']:
            continue
        attrs[key] = info_dict[key]

    return attrs
