#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# BugzillaREST to Elastic class helper
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

from time import time
from dateutil import parser
import json
import logging

from .enrich import Enrich, metadata

from .utils import get_time_diff_days

class BugzillaRESTEnrich(Enrich):

    def get_field_author(self):
        return 'creator_detail'

    def get_fields_uuid(self):
        return ["assigned_to_uuid", "creator_uuid"]

    def get_field_unique_id(self):
        return "ocean-unique-id"

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        identity_fields = ['assigned_to_detail', 'qa_contact_detail', 'creator_detail']

        for f in identity_fields:
            if f in item['data']:
                user = self.get_sh_identity(item['data'][f])
            identities.append(user)
        return identities

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is used
        if 'data' in item and type(item) == dict:
            user = item['data'][identity_field]

        identity['username'] = user['name']
        identity['email'] = user['email']
        identity['name'] = user['real_name']
        return identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        product = eitem['product']
        repo += "/buglist.cgi?product="+product
        return repo

    @metadata
    def get_rich_item(self, item):

        if 'id' not in item['data']:
            logging.warning("Dropped bug without bug_id %s" % (item))
            return None

        eitem = {}

        # metadata fields to copy
        copy_fields = ["metadata__updated_on","metadata__timestamp","ocean-unique-id","origin"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None

        # The real data
        issue = item['data']

        if "assigned_to_detail" in issue and "real_name" in issue["assigned_to_detail"]:
            eitem["assigned_to"] = issue["assigned_to_detail"]["real_name"]

        if "creator_detail" in issue and "real_name" in issue["creator_detail"]:
            eitem["creator"] = issue["creator_detail"]["real_name"]

        eitem["id"] = issue['id']
        eitem["status"]  = issue['status']
        if "summary" in issue:
            eitem["summary"]  = issue['summary']
        # Component and product
        eitem["component"] = issue['component']
        eitem["product"]  = issue['product']

        # Fix dates
        date_ts = parser.parse(issue['creation_time'])
        eitem['creation_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')
        date_ts = parser.parse(issue['last_change_time'])
        eitem['changeddate_date'] = date_ts.isoformat()
        eitem['delta_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')

        # Add extra JSON fields used in Kibana (enriched fields)
        eitem['number_of_comments'] = 0
        eitem['time_to_last_update_days'] = None
        eitem['url'] = None

        if 'long_desc' in issue:
            eitem['number_of_comments'] = len(issue['long_desc'])
        eitem['url'] = item['origin'] + "/show_bug.cgi?id=" + str(issue['id'])
        eitem['time_to_last_update_days'] = \
            get_time_diff_days(eitem['creation_ts'], eitem['delta_ts'])

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        return eitem
