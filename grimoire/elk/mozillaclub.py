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

from grimoire.elk.enrich import Enrich

class MozillaClubEnrich(Enrich):

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_author(self):
        return "Your Name"

    def get_field_date(self):
        return "metadata__updated_on"

    def get_field_unique_id(self):
        return "uuid"

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "Event Description": {
                  "type": "string",
                  "index":"analyzed"
                  },
                "Event Creation": {
                  "type": "string",
                  "index":"analyzed"
                  },
                "Feedback from Attendees": {
                  "type": "string",
                  "index":"analyzed"
                  },
                "Your Feedback": {
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
        identities.append(self.get_sh_identity(item["Your Name"]))

        return identities

    def get_sh_identity(self, owner_name):
        identity = {}

        identity['username'] = owner_name
        identity['email'] = None
        identity['name'] = owner_name

        return identity

    def get_rich_item(self, item):
        eitem = {}

        # metadata fields to copy
        copy_fields = ["metadata__updated_on", "metadata__timestamp", "ocean-unique-id", "origin"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        event = item['data']

        # just copy all fields
        for f in event:
            eitem[f] = event[f]

        # Transform numeric fields
        eitem['Attendance'] = int(eitem['Attendance'])

        if self.sortinghat:
            eitem.update(self.get_item_sh(item,"Your Name"))

        return eitem

    def enrich_items(self, items):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        url = self.elastic.index_url+'/items/_bulk'

        logging.debug("Adding items to %s (in %i packs)", url, max_items)

        for item in items:
            if current >= max_items:
                self.requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (item[self.get_field_unique_id()])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        self.requests.put(url, data = bulk_json)
