#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019, 2023 Adam.Dybbroe

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

"""Miscellaneous tools/utilities taken from mpop."""

import logging
import os
import socket

import netifaces
from six.moves.urllib.parse import urlparse

LOG = logging.getLogger(__name__)


class NoGeoScenesError(Exception):
    """Custom Exception to capture cases where no Geo scenes can be found."""
    pass


def check_uri(uri):
    """Check that the provided *uri* is on the local host and return the file path."""
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
                    raise IOError("Data file %s unaccessible from this host" % uri) from None

    except socket.gaierror:
        LOG.warning("Couldn't check file location, running anyway")

    return url.path


def get_local_ips():
    """Get local IP adresses."""
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add["addr"])
    return ips
