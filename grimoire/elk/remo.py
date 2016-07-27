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

from grimoire.elk.enrich import Enrich

class ReMoEnrich(Enrich):

    def __init__(self, remo, db_sortinghat=None, db_projects_map = None):
        super().__init__(db_sortinghat, db_projects_map)
        self.elastic = None
        self.perceval_backend = remo
        self.index_remo = "remo"

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_date(self):
        return "metadata__updated_on"

    def get_field_unique_id(self):
        return "event_url"

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "description_analyzed": {
                  "type": "string",
                  "index":"analyzed"
                  },
                "geolocation": {
                   "type": "geo_point"
                }
           }
        } """

        return {"items":mapping}

    def get_identities(self, item):
        ''' Return the identities from an item '''

        identities = []

        item = item['data']
        owner_data = None
        if "owner_data" in item:
            owner_data = item['owner_data']

        identities.append(self.get_sh_identity(item["owner_name"], owner_data))

        return identities

    def get_sh_identity(self, owner_name, owner_data=None):
        #  "owner_name": "Melchor Compendio"
        identity = {}

        identity['username'] = owner_name
        identity['email'] = None
        identity['name'] = owner_name
        if owner_data:
            # There is details about this identity
            identity['name'] = owner_data['fullname']
            identity['username'] = owner_data['profile']['display_name']
            identity['email'] = None

        return identity

    def get_rich_item(self, item):
        eitem = {}

        # metadata fields to copy
        copy_fields = ["metadata__updated_on","metadata__timestamp","ocean-unique-id","origin"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        build = item['data']

        # data fields to copy
        copy_fields = ["actual_attendance","campaign","city","country",
                       "description","estimated_attendance","event_url",
                       "external_link","lat","lon","mozilla_event",
                       "multiday","owner_profile_url","region","timezone",
                       "venue"]
        for f in copy_fields:
            if f in build:
                eitem[f] = build[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {
            "description": "description_analyzed",
            "end":"end_date",
            "local_end":"locan_end_date",
            "start":"start_date",
            "local_start":"local_start_date",
            "name":"title",
            "owner_name":"owner"
        }
        for fn in map_fields:
            eitem[map_fields[fn]] = build[fn]

        # geolocation
        eitem['geolocation'] = {
            "lat": eitem['lat'],
            "lon": eitem['lon'],
        }

        if self.sortinghat:
            eitem.update(self.get_item_sh(item,"owner_name"))

        return eitem

    def enrich_items(self, items):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        url = self.elastic.index_url+'/items/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))

        for item in items:
            if current >= max_items:
                self.requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (rich_item[self.get_field_unique_id()])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        self.requests.put(url, data = bulk_json)
