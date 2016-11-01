#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
#
# Copyright (C) 2016 Bitergia
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

import logging

from datetime import datetime

from dateutil import parser

from grimoire.elk.enrich import Enrich

from .utils import get_time_diff_days


class RedmineEnrich(Enrich):

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "description_analyzed": {
                  "type": "string",
                  "index":"analyzed"
                  }
           }
        } """

        return {"items":mapping}

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        data = item['data']
        if 'assigned_to' in data:
            identities.append(self.get_sh_identity(data['assigned_to']))
        identities.append(self.get_sh_identity(data['author']))

        return identities

    def get_sh_identity(self, user):
        identity = {}
        identity['email'] = None
        if 'email' in user:
            identity['email'] = user['email']
        identity['username'] = user['id']
        identity['name'] = user['name']
        return identity

    def get_item_sh(self, item):
        """ Add sorting hat enrichment fields for the author of the item """

        eitem = {}  # Item enriched

        identity  = self.get_sh_identity(item['data']['author'])
        eitem = self.get_item_sh_fields(identity, parser.parse(item[self.get_field_date()]))

        return eitem

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
        ticket = item['data']

        # data fields to copy
        copy_fields = ["subject","description","created_on","updated_on"]
        for f in copy_fields:
            if f in ticket:
                eitem[f] = ticket[f]
            else:
                eitem[f] = None
        eitem['description_analyzed'] = eitem['description']
        # Fields which names are translated
        map_fields = {}
        for fn in map_fields:
            eitem[map_fields[fn]] = ticket[fn]

        # People
        eitem['author_id'] = ticket['author']['id']
        eitem['author_name'] = ticket['author']['name']
        if 'assigned_to' in ticket:
            eitem['assigned_to_id'] = ticket['assigned_to']['id']
            eitem['assigned_to_name'] = ticket['assigned_to']['name']


        # Time to
        eitem['resolution_days'] = \
            get_time_diff_days(eitem['created_on'], eitem['updated_on'])
        eitem['timeopen_days'] = \
            get_time_diff_days(eitem['created_on'], datetime.utcnow())

        # Enrich dates
        eitem["created_on"] = parser.parse(eitem["created_on"]).isoformat()
        eitem["updated_on"] = parser.parse(eitem["updated_on"]).isoformat()

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "job"))

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        return eitem
