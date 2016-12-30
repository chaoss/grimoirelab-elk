#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# StackExchange Ocean feeder
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
from .elastic import ElasticOcean

class StackExchangeOcean(ElasticOcean):
    """StackExchange Ocean feeder"""

    def _fix_item(self, item):
        item["ocean-unique-id"] = str(item["data"]["question_id"])+"_"+item['origin']

    @classmethod
    def get_perceval_params_from_url(cls, url):
        params = []

        tokens = url.replace('https://','').replace('http://','').split('/')
        tag = tokens[-1]
        site = tokens[0]
        params.append('--site')
        params.append(site)
        params.append('--tagged')
        params.append(tag)
        params.append('--tag') #this is the origin we record in our DB
        params.append(url)

        return params
