#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Gerrit to Elastic class helper
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

from datetime import datetime
from dateutil import parser
import json
import logging
import time


from .enrich import Enrich, metadata

class GerritEnrich(Enrich):

    def get_field_author(self):
        return "owner"

    def get_fields_uuid(self):
        return ["review_uuid", "patchSet_uuid", "approval_uuid"]

    def get_sh_identity(self, item, identity_field=None):
        identity = {}
        for field in ['name', 'email', 'username']:
            identity[field] = None

        user = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
            user = item['data'][identity_field]

        if 'name' in user: identity['name'] = user['name']
        if 'email' in user: identity['email'] = user['email']
        if 'username' in user: identity['username'] = user['username']
        return identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        repo += "_" + eitem['repository']
        return repo

    def get_identities(self, item):
        ''' Return the identities from an item '''

        identities = []

        item = item['data']

        # Changeset owner
        user = item['owner']
        identities.append(self.get_sh_identity(user))

        # Patchset uploader and author
        if 'patchSets' in item:
            for patchset in item['patchSets']:
                user = patchset['uploader']
                identities.append(self.get_sh_identity(user))
                if 'author' in patchset:
                    user = patchset['author']
                    identities.append(self.get_sh_identity(user))
                if 'approvals' in patchset:
                    # Approvals by
                    for approval in patchset['approvals']:
                        user = approval['by']
                        identities.append(self.get_sh_identity(user))
        # Comments reviewers
        if 'comments' in item:
            for comment in item['comments']:
                user = comment['reviewer']
                identities.append(self.get_sh_identity(user))

        return identities

    def get_item_id(self, eitem):
        """ Return the item_id linked to this enriched eitem """

        # The eitem _id includes also the patch.
        return eitem["_source"]["review_id"]

    def _fix_review_dates(self, item):
        ''' Convert dates so ES detect them '''


        for date_field in ['timestamp','createdOn','lastUpdated']:
            if date_field in item.keys():
                date_ts = item[date_field]
                item[date_field] = time.strftime('%Y-%m-%dT%H:%M:%S',
                                                  time.localtime(date_ts))
        if 'patchSets' in item.keys():
            for patch in item['patchSets']:
                pdate_ts = patch['createdOn']
                patch['createdOn'] = time.strftime('%Y-%m-%dT%H:%M:%S',
                                                   time.localtime(pdate_ts))
                if 'approvals' in patch:
                    for approval in patch['approvals']:
                        adate_ts = approval['grantedOn']
                        approval['grantedOn'] = \
                            time.strftime('%Y-%m-%dT%H:%M:%S',
                                          time.localtime(adate_ts))
        if 'comments' in item.keys():
            for comment in item['comments']:
                cdate_ts = comment['timestamp']
                comment['timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%S',
                                                     time.localtime(cdate_ts))


    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
               "summary_analyzed": {
                  "type": "string",
                  "index":"analyzed"
               },
               "timeopen": {
                  "type": "double"
               }
            }
        }
        """

        return {"items":mapping}


    @metadata
    def get_rich_item(self, item):
        eitem = {}  # Item enriched

        # metadata fields to copy
        copy_fields = ["metadata__updated_on","metadata__timestamp","ocean-unique-id","origin"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        eitem['closed'] = item['metadata__updated_on']
        # The real data
        review = item['data']
        self._fix_review_dates(review)

        # data fields to copy
        copy_fields = ["status", "branch", "url"]
        for f in copy_fields:
            eitem[f] = review[f]
        # Fields which names are translated
        map_fields = {"subject": "summary",
                      "id": "githash",
                      "createdOn": "opened",
                      "project": "repository",
                      "number": "number"
                      }
        for fn in map_fields:
            eitem[map_fields[fn]] = review[fn]
        eitem["summary_analyzed"] = eitem["summary"]
        eitem["name"] = None
        eitem["domain"] = None
        if 'name' in review['owner']:
            eitem["name"] = review['owner']['name']
            if 'email' in review['owner']:
                if '@' in review['owner']['email']:
                    eitem["domain"] = review['owner']['email'].split("@")[1]
        # New fields generated for enrichment
        eitem["patchsets"] = len(review["patchSets"])

        # Time to add the time diffs
        createdOn_date = parser.parse(review['createdOn'])
        if len(review["patchSets"]) > 0:
            createdOn_date = parser.parse(review["patchSets"][0]['createdOn'])
        lastUpdated_date = parser.parse(review['lastUpdated'])
        seconds_day = float(60*60*24)
        if eitem['status'] in ['MERGED','ABANDONED']:
            timeopen = \
                (lastUpdated_date-createdOn_date).total_seconds() / seconds_day
        else:
            timeopen = \
                (datetime.utcnow()-createdOn_date).total_seconds() / seconds_day
        eitem["timeopen"] =  '%.2f' % timeopen

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(review['createdOn'], "review"))

        return eitem
