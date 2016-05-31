#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# JIRA to Elastic class helper
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
import json
import logging

from urllib.parse import urlparse

from .enrich import Enrich

from .utils import get_time_diff_days

class JiraEnrich(Enrich):

    def __init__(self, jira, sortinghat=True, db_projects_map = None):
        super().__init__(sortinghat, db_projects_map)
        self.perceval_backend = jira
        self.elastic = None

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_date(self):
        return "metadata__updated_on"

    def get_fields_uuid(self):
        return ["assigned_to_uuid", "reporter_uuid"]

    @classmethod
    def get_sh_identity(cls, user):
        """ Return a Sorting Hat identity using jira user data """

        identity = {}
        for field in ['name', 'email', 'username']:
            # Basic fields in Sorting Hat
            identity[field] = None
        if 'displayName' in user:
            identity['name'] = user['displayName']
        if 'name' in user:
            identity['username'] = user['name']
        if 'email' in user:
            identity['email'] = user['emailAddress']
        return identity

    def get_item_sh(self, item):
        """ Add sorting hat enrichment fields """
        eitem = {}  # Item enriched

        # Sorting Hat integration: reporter, assignee, creator uuids
        for field in ["assignee","reporter","creator"]:
            if field in item:
                identity = JiraEnrich.get_sh_identity(item['field'][field])
                eitem[field+'_uuid'] = self.get_uuid(identity, self.get_connector_name())
                eitem[field+'_name'] = identity['displayName']
        return eitem


    def get_identities(self, item):
        ''' Return the identities from an item '''

        identities = []

        item = item['data']

        for field in ["assignee","reporter","creator"]:
            if item["fields"][field]:
                identities.append(self.get_sh_identity(item["fields"][field]))

        return identities

    def get_field_unique_id(self):
        return "ocean-unique-id"

    def enrich_issue(self, item):

        def get_jira_url():
            u = urlparse(self.perceval_backend.url)
            return u.scheme+"//"+u.netloc

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

        # Fields that are the same in item and eitem
        copy_fields = ["assigned_to","reporter"]
        for f in copy_fields:
            if f in issue:
                eitem[f] = issue[f]
            else:
                eitem[f] = None

        if issue["fields"]["assignee"]:
            eitem["assigned_to"] = issue["fields"]["assignee"]["displayName"]
        eitem["reporter"] = issue["fields"]["reporter"]["displayName"]

        # Add extra JSON fields used in Kibana (enriched fields)
        eitem['number_of_comments'] = 0
        eitem['time_to_last_update_days'] = None
        eitem['url'] = None

        if 'long_desc' in issue:
            eitem['number_of_comments'] = len(issue['long_desc'])
        eitem['url'] = self.perceval_backend.url + "/browse/"+ issue['key']
        eitem['time_to_last_update_days'] = \
            get_time_diff_days(issue['fields']['created'], issue['fields']['updated'])

        if self.sortinghat:
            eitem.update(self.get_item_sh(issue))

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
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (issue[self.get_field_unique_id()])
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
            data_json = json.dumps(eitem)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (eitem[self.get_field_unique_id()])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        self.requests.put(url, data=bulk_json)

        logging.debug("Adding issues to ES Done")
