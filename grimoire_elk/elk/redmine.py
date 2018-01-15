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

from .enrich import Enrich

from .utils import get_time_diff_days


logger = logging.getLogger(__name__)


class RedmineEnrich(Enrich):

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "description_analyzed": {
                  "type": "keyword"
                  }
           }
        } """

        return {"items": mapping}

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        data = item['data']
        if 'assigned_to' in data:
            identities.append(self.get_sh_identity(data, 'assigned_to'))
        identities.append(self.get_sh_identity(data, 'author'))
        # TODO: identities in journals not added yet

        return identities

    def get_field_author(self):
        return "author"

    def get_sh_identity(self, data, rol):
        identity = {}
        identity['email'] = None
        if rol + "_data" in data:
            if 'mail' in data[rol + "_data"]:
                identity['email'] = data[rol + "_data"]['mail']
        identity['username'] = data[rol]['id']
        identity['name'] = data[rol]['name']
        return identity

    def get_item_sh(self, item):
        """ Add sorting hat enrichment fields for the author of the item """

        eitem = {}  # Item enriched

        identity = self.get_sh_identity(item['data'], 'author')
        eitem = self.get_item_sh_fields(identity, parser.parse(item[self.get_field_date()]))

        return eitem

    def get_rich_item(self, item):
        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        ticket = item['data']

        # data fields to copy
        copy_fields = ["subject", "description", "updated_on",
                       "due_date", "estimated_hours", "done_ratio",
                       "id", "spent_hours", "start_date", "subject",
                       "last_update"]
        for f in copy_fields:
            if f in ticket:
                eitem[f] = ticket[f]
            else:
                eitem[f] = None
        eitem['description_analyzed'] = eitem['description']
        # Fields which names are translated
        map_fields = {"due_date": "estimated_closing_date",
                      "created_on": "creation_date",
                      "closed_on": "closing_date"}
        for fn in map_fields:
            if fn in ticket:
                eitem[map_fields[fn]] = ticket[fn]
        # Common format
        common = ['category', 'fixed_version', 'priority', 'project', 'status',
                  'tracker', 'author', 'assigned_to']
        for f in common:
            if f in ticket:
                eitem[f + '_id'] = ticket[f]['id']
                eitem[f + '_name'] = ticket[f]['name']

        len_fields = ['attachments', 'changesets', 'journals', 'relations']
        for f in len_fields:
            if f in ticket:
                eitem[f] = len(ticket[f])

        if 'parent' in ticket:
            eitem['parent_id'] = ticket['parent']['id']

        eitem['url'] = eitem['origin'] + "/issues/" + str(eitem['id'])

        # Time to
        if "closing_date" in eitem:
            eitem['timeopen_days'] = \
                get_time_diff_days(eitem['creation_date'], eitem['closing_date'])
            eitem['timeworking_days'] = \
                get_time_diff_days(eitem['start_date'], eitem['closing_date'])
        else:
            eitem['timeopen_days'] = \
                get_time_diff_days(eitem['creation_date'], datetime.utcnow())
            eitem['timeworking_days'] = \
                get_time_diff_days(eitem['start_date'], datetime.utcnow())

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "job"))

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        return eitem
