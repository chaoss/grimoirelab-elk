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

class RedmineEnrich(Enrich):

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "fullDisplayName_analyzed": {
                  "type": "string",
                  "index":"analyzed"
                  }
           }
        } """

        return {"items":mapping}
    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        revisions = item['data']['revisions']

        for revision in revisions:
            user = self.get_sh_identity(revision)
            identities.append(user)
        return identities

    def get_sh_identity(self, revision):
        identity = {}
        identity['username'] = None
        identity['email'] = None
        identity['name'] = None
        if 'user' in revision:
            identity['username'] = revision['user']
            identity['name'] = revision['user']
        return identity

    def get_item_sh(self, item):
        """ Add sorting hat enrichment fields for the author of the item """

        eitem = {}  # Item enriched
        if not len(item['data']['revisions']) > 0:
            return eitem

        first_revision = item['data']['revisions'][0]

        identity  = self.get_sh_identity(first_revision)
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
        copy_fields = ["fullDisplayName","url","result","duration","builtOn"]
        for f in copy_fields:
            if f in ticket:
                eitem[f] = ticket[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"fullDisplayName": "fullDisplayName_analyzed",
                      "number": "ticket"
                      }
        for fn in map_fields:
            eitem[map_fields[fn]] = ticket[fn]

        # Job url: remove the last /ticket_id from job_url/ticket_id/
        eitem['job_url'] = eitem['url'].rsplit("/", 2)[0]
        eitem['job_name'] = eitem['url'].rsplit('/', 3)[1]
        eitem['job_ticket'] = eitem['job_name']+'/'+str(eitem['ticket'])

        # Enrich dates
        eitem["ticket_date"] = parser.parse(item["metadata__updated_on"]).isoformat()

        # Add duration in days
        if "duration" in eitem:
            seconds_day = float(60*60*24)
            duration_days = eitem["duration"]/(1000*seconds_day)
            eitem["duration_days"] = float('%.2f' % duration_days)

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "job"))

        if self.sortinghat:
            erevision.update(self.get_review_sh(ticket, item))


        return eitem
