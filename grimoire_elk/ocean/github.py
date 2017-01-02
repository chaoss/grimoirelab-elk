#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# GitHub Ocean feeder
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

from dateutil import parser

from .elastic import ElasticOcean

class GitHubOcean(ElasticOcean):
    """GitHub Ocean feeder"""

    def _fix_item(self, item):
        item["ocean-unique-id"] = str(item["data"]["id"])+"_"+item['origin']

    @classmethod
    def get_perceval_params_from_url(cls, url):
        """ Get the perceval params given a URL for the data source """
        params = []

        owner = url.split('/')[-2]
        repo = url.split('/')[-1]
        params.append('--owner')
        params.append(owner)
        params.append('--repository')
        params.append(repo)
        return params
