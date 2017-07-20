#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
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

import json
import logging

from dateutil import parser

from .enrich import Enrich, metadata


logger = logging.getLogger(__name__)


class DockerHubEnrich(Enrich):

    def get_field_author(self):
        return "nick"

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "full_description_analyzed": {
                  "type": "string",
                  "index":"analyzed"
                  },
                "description_analyzed": {
                  "type": "string",
                  "index":"analyzed"
                  }
           }
        } """

        return {"items":mapping}


    def get_identities(self, item):
        """ Return the identities from an item """
        # In DockerHub there are no identities. Just the organization and
        # the repository name for the docker image
        identities = []
        return identities

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        image = item['data']

        # data fields to copy
        copy_fields = ["is_automated", "is_private",
                       "pull_count", "repository_type",
                       "star_count", "status", "user"]
        for f in copy_fields:
            if f in image:
                eitem[f] = image[f]
            else:
                eitem[f] = None

        # Fields which names are translated
        map_fields = {}
        for fn in map_fields:
            eitem[map_fields[fn]] = image[fn]

        eitem["id"] = image["name"] + '-' + image["namespace"]
        eitem['last_updated'] = image['last_updated']

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "dockerhub"))

        return eitem
