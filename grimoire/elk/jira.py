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

from datetime import datetime
from time import time
import json
import logging

from urllib.parse import urlparse

from dateutil import parser

from .enrich import Enrich

from .utils import get_time_diff_days

class JiraEnrich(Enrich):

    def __init__(self, jira, db_sortinghat=None, db_projects_map = None):
        super().__init__(db_sortinghat, db_projects_map)
        self.perceval_backend = jira
        self.elastic = None

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_date(self):
        return "metadata__updated_on"

    def get_fields_uuid(self):
        return ["assigned_to_uuid", "reporter_uuid"]

    def get_sh_identity(self, user):
        """ Return a Sorting Hat identity using jira user data """

        identity = {}

        for field in ['name', 'email', 'username']:
            # Basic fields in Sorting Hat
            identity[field] = None

        if not user:
            return identity

        if 'displayName' in user:
            identity['name'] = user['displayName']
        if 'name' in user:
            identity['username'] = user['name']
        if 'emailAddress' in user:
            identity['email'] = user['emailAddress']
        return identity

    def get_item_sh(self, item):
        """ Add sorting hat enrichment fields """
        eitem = {}  # Item enriched

        # Sorting Hat integration: reporter, assignee, creator uuids
        for field in ["assignee","reporter","creator"]:
            if field in item['data']['fields']:
                identity = self.get_sh_identity(item['data']['fields'][field])
                eitem[field+'_uuid'] = self.get_uuid(identity, self.get_connector_name())
                eitem[field+'_name'] = identity['name']
                eitem[field+"_org_name"] = self.get_enrollment(eitem[field+'_uuid'], parser.parse(item[self.get_field_date()]))
                eitem[field+"_domain"] = self.get_domain(identity)
                eitem[field+"_bot"] = self.is_bot(eitem[field+'_uuid'])


        # Unify fields for SH filtering
        if 'reporter' in item['data']['fields']:
            eitem["author_uuid"] = eitem["reporter_uuid"]
            eitem["author_name"] = eitem["reporter_name"]
            eitem["author_org_name"] = eitem["reporter_org_name"]
            eitem["author_domain"] = eitem["reporter_domain"]

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

        # dizquierdo requirements T146
        eitem['changes'] = issue['changelog']['total']
        if issue["fields"]["assignee"]:
            eitem['assignee'] =  issue["fields"]["assignee"]["displayName"]
            eitem['assignee_email'] = None
            if "emailAddress" in issue["fields"]["creator"]:
                eitem['assignee_email'] = issue["fields"]["assignee"]["emailAddress"]
            eitem['assignee_tz'] = issue["fields"]["assignee"]["timeZone"]
        eitem['author_name'] =  issue["fields"]["creator"]["displayName"]
        eitem['author_email'] = None
        if "emailAddress" in issue["fields"]["creator"]:
            eitem['author_email'] = issue["fields"]["creator"]["emailAddress"]
        eitem['author_login'] = issue["fields"]["creator"]["name"]
        eitem['author_tz'] = issue["fields"]["creator"]["timeZone"]
        eitem['creation_date'] = issue["fields"]['created']
        eitem['main_description'] = issue["fields"]['description']
        eitem['isssue_type'] = issue["fields"]['issuetype']['name']
        eitem['issue_description'] = issue["fields"]['issuetype']['description']

        eitem['labels'] = issue['fields']['labels']
        eitem['priority'] = issue['fields']['priority']['name']
        # data.fields.progress.percent not exists in Puppet JIRA
        eitem['progress_total'] = issue['fields']['progress']['total']
        eitem['project_id'] = issue['fields']['project']['id']
        eitem['project_key'] = issue['fields']['project']['key']
        eitem['project_name'] = issue['fields']['project']['name']

        eitem['reporter_name'] = issue['fields']['reporter']['displayName']
        eitem['reporter_email'] = None
        if "emailAddress" in issue["fields"]["reporter"]:
            eitem['reporter_email'] = issue['fields']['reporter']['emailAddress']
        eitem['reporter_login'] = issue['fields']['reporter']['name']
        eitem['reporter_tz'] = issue['fields']['reporter']['timeZone']
        if issue['fields']['resolution']:
            eitem['resolution_id'] = issue['fields']['resolution']['id']
            eitem['resolution_name'] = issue['fields']['resolution']['name']
            eitem['resolution_description'] = issue['fields']['resolution']['description']
            eitem['resolution_self'] = issue['fields']['resolution']['self']
        eitem['resolution_date'] = issue['fields']['resolutiondate']
        eitem['status_description'] = issue['fields']['status']['description']
        eitem['status'] = issue['fields']['status']['name']
        eitem['summary'] = issue['fields']['summary']
        eitem['original_time_estimation'] = issue['fields']['timeoriginalestimate']
        if eitem['original_time_estimation']:
            eitem['original_time_estimation_hours'] =  int(eitem['original_time_estimation'])/3600
        eitem['time_spent'] = issue['fields']['timespent']
        if eitem['time_spent']:
            eitem['time_spent_hours'] = int(eitem['time_spent'])/3600
        eitem['time_estimation'] = issue['fields']['timeestimate']
        if eitem['time_estimation']:
            eitem['time_estimation_hours'] = int(eitem['time_estimation'])/3600
        eitem['watchers'] = issue['fields']['watches']['watchCount']
        eitem['key'] = issue['key']

        # Add extra JSON fields used in Kibana (enriched fields)
        eitem['number_of_comments'] = 0
        eitem['time_to_last_update_days'] = None
        eitem['url'] = None

        if 'long_desc' in issue:
            eitem['number_of_comments'] = len(issue['long_desc'])
        eitem['url'] = item['origin'] + "/browse/"+ issue['key']
        eitem['time_to_close_days'] = \
            get_time_diff_days(issue['fields']['created'], issue['fields']['updated'])
        eitem['time_to_last_update_days'] = \
            get_time_diff_days(issue['fields']['created'], datetime.utcnow())

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        eitem.update(self.get_grimoire_fields(issue['fields']['created'], "issue"))

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
