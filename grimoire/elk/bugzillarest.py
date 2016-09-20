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
from urllib.parse import urlparse

from .enrich import Enrich

from .utils import get_time_diff_days

class BugzillaRESTEnrich(Enrich):

    def __init__(self, bugzilla, db_sortinghat=None, db_projects_map = None):
        super().__init__(db_sortinghat, db_projects_map)
        self.perceval_backend = bugzilla
        self.elastic = None

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_date(self):
        return "last_change_time"

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

    def get_sh_identity(self, user):
        identity = {}
        identity['username'] = user['name']
        identity['email'] = user['email']
        identity['name'] = user['real_name']
        return identity

    def get_item_sh(self, item):
        """ Add sorting hat enrichment fields for the author of the item """

        eitem = {}  # Item enriched
        data = item['data']['creator_detail']

        identity  = self.get_sh_identity(data)
        eitem = self.get_item_sh_fields(identity, parser.parse(item['data'][self.get_field_date()]))

        return eitem

    def get_item_project(self, item):
        """ Get project mapping enrichment field """
        ds_name = "bugzillarest"  # data source name in projects map
        url = item['origin']
        # https://bugs.eclipse.org/bugs/buglist.cgi?product=Mylyn%20Tasks
        product = item['data']['product']
        repo = url+"/buglist.cgi?product="+product

        try:
            project = (self.prjs_map[ds_name][repo])
        except KeyError:
            # logging.warning("Project not found for repository %s" % (repo))
            project = None
        return {"project": project}


    def enrich_issue(self, item):

        if 'id' not in item['data']:
            logging.warning("Dropped bug without bug_id %s" % (issue))
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
            eitem.update(self.get_item_project(item))

        return eitem


    def enrich_items(self, items):
#         if self.perceval_backend.detail == "list":
#             self.issues_list_to_es(items)
#         else:
#             self.issues_to_es(items)
        self.issues_to_es(items)

    def issues_list_to_es(self, items):

        elastic_type = "issues_list"

        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        url = self.elastic.index_url+'/' + elastic_type + '/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))

        # In this client, we will publish all data in Elastic Search
        for issue in items:
            if current >= max_items:
                task_init = time()
                self.requests.put(url, data=bulk_json)
                bulk_json = ""
                total += current
                current = 0
                logging.debug("bulk packet sent (%.2f sec, %i total)"
                              % (time()-task_init, total))
            data_json = json.dumps(issue)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (rich_item[self.get_field_unique_id()])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        task_init = time()
        total += current
        self.requests.put(url, data=bulk_json)
        logging.debug("bulk packet sent (%.2f sec, %i total)"
                      % (time()-task_init, total))


    def issues_to_es(self, items):

        elastic_type = "items"

        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        url = self.elastic.index_url+'/' + elastic_type + '/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))

        for issue in items:
            if current >= max_items:
                self.requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0
            eitem = self.enrich_issue(issue)
            if not eitem:
                continue
            data_json = json.dumps(eitem)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (eitem[self.get_field_unique_id()])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        self.requests.put(url, data=bulk_json)

        logging.debug("Adding issues to ES Done")
