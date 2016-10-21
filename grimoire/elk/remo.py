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

from grimoire.elk.enrich import Enrich

class ReMoEnrich(Enrich):

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
