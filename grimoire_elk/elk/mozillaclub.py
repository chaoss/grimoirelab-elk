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

from grimoire_elk.elk.enrich import Enrich
from ..elastic_mapping import Mapping as BaseMapping


logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        if es_major != '2':
            mapping = """
            {
                "properties": {
                    "Event Description": {
                      "type": "text"
                      },
                    "Event Creation": {
                      "type": "text"
                      },
                    "Feedback from Attendees": {
                      "type": "text"
                      },
                    "Your Feedback": {
                      "type": "text"
                      },
                    "geolocation": {
                       "type": "geo_point"
                    }
               }
            } """
        else:
            mapping = """
            {
                "properties": {
                    "Event Description": {
                      "type": "string",
                      "index": "analyzed"
                      },
                    "Event Creation": {
                      "type": "string",
                      "index": "analyzed"
                      },
                    "Feedback from Attendees": {
                      "type": "string",
                      "index": "analyzed"
                      },
                    "Your Feedback": {
                      "type": "string",
                      "index": "analyzed"
                      },
                    "geolocation": {
                       "type": "geo_point"
                    }
               }
            } """

        return {"items": mapping}


class MozillaClubEnrich(Enrich):

    mapping = Mapping

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_author(self):
        return "Your Name"

    def get_field_date(self):
        return "metadata__updated_on"

    def get_field_unique_id(self):
        return "uuid"

    def get_identities(self, item):
        ''' Return the identities from an item '''

        identities = []

        identities.append(self.get_sh_identity(item, self.get_field_author()))

        return identities

    def get_sh_identity(self, item, identity_field):
        identity = {}

        identity['username'] = item['data'][identity_field]
        identity['email'] = None
        identity['name'] = item['data'][identity_field]

        return identity

    def get_rich_item(self, item):
        eitem = {}

        # metadata fields to copy
        copy_fields = ["metadata__updated_on", "metadata__timestamp",
                       "origin", "uuid"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        event = item['data']

        # just copy all fields converting in field names spaces to _
        for f in event:
            eitem[f.replace(" ", "_")] = event[f]

        # Use the unique id from perceval
        eitem['id'] = eitem['uuid']
        # There is no url
        eitem['url'] = None

        # Transform numeric fields
        eitem['Attendance'] = int(eitem['Attendance'])

        if self.sortinghat:
            eitem.update(self.get_item_sh(item, "Your Name"))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(event["Timestamp"], "event"))

        return eitem

    def enrich_items(self, ocean_backend):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        url = self.elastic.index_url + '/items/_bulk'

        logger.debug("Adding items to %s (in %i packs)", url, max_items)

        items = ocean_backend.fetch()
        for item in items:
            if current >= max_items:
                self.requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (item[self.get_field_unique_id()])
            bulk_json += data_json + "\n"  # Bulk document
            current += 1
        self.requests.put(url, data=bulk_json)
