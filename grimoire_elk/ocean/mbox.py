#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# MBox Ocean feeder
#
# Copyright (C) 2015 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import datetime
from dateutil import parser
import logging

from .elastic import ElasticOcean

class MBoxOcean(ElasticOcean):
    """MBox Ocean feeder"""

    def _fix_item(self, item):
        if "Message-ID" in item["data"] and item["data"]["Message-ID"]:
            item["ocean-unique-id"] = item["data"]["Message-ID"]+"_"+item['origin']
        else:
            logging.warning("No Message-ID in %s %s" % (item["data"]["Subject"], item['origin']))
            item["ocean-unique-id"] = "NONE_"+item['origin']

    @classmethod
    def get_perceval_params_from_url(cls, url):
        # In the url the uri and the data dir are included
        params = url.split()

        return params
