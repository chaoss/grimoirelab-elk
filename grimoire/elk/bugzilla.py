#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Bugzilla to Elastic class helper
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

from datetime import datetime
from time import time
from urllib.parse import urlparse

from dateutil import parser

from .enrich import Enrich

from .utils import get_time_diff_days

class BugzillaEnrich(Enrich):

    def __init__(self, bugzilla, db_sortinghat=None, db_projects_map = None):
        super().__init__(db_sortinghat, db_projects_map)
        self.perceval_backend = bugzilla
        self.elastic = None

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_date(self):
        return "delta_ts"

    def get_fields_uuid(self):
        return ["assigned_to_uuid", "reporter_uuid"]

    def get_field_unique_id(self):
        return "ocean-unique-id"

    @classmethod
    def get_sh_identity(cls, user):
        """ Return a Sorting Hat identity using bugzilla user data """

        def fill_list_identity(identity, user_list_data):
            """ Fill identity with user data in first item in list """
            identity['username'] = user_list_data[0]['__text__']
            if '@' in identity['username']:
                identity['email'] = identity['username']
            if 'name' in user_list_data[0]:
                identity['name'] = user_list_data[0]['name']
            return identity

        identity = {}
        for field in ['name', 'email', 'username']:
            # Basic fields in Sorting Hat
            identity[field] = None
        if 'reporter' in user:
            identity = fill_list_identity(identity, user['reporter'])
        if 'assigned_to' in user:
            identity = fill_list_identity(identity, user['assigned_to'])
        if 'who' in user:
            identity = fill_list_identity(identity, user['who'])
        if 'Who' in user:
            identity['username'] = user['Who']
            if '@' in identity['username']:
                identity['email'] = identity['username']
        if 'qa_contact' in user:
            identity = fill_list_identity(identity, user['qa_contact'])
        if 'changed_by' in user:
            identity['name'] = user['changed_by']

        return identity

    def get_item_sh(self, item):
        """ Add sorting hat enrichment fields """
        eitem = {}  # Item enriched

        # Sorting Hat integration: reporter and assigned_to uuids
        if 'assigned_to' in item['data']:
            identity = BugzillaEnrich.get_sh_identity({'assigned_to':item["data"]['assigned_to']})
            eitem['assigned_to_uuid'] = self.get_uuid(identity, self.get_connector_name())
            eitem['assigned_to_name'] = identity['name']
            item_date = item['data'][self.get_field_date()][0]['__text__']
            item_date_dt = parser.parse(item_date)
            item_date_utc = (item_date_dt-item_date_dt.utcoffset()).replace(tzinfo=None)
            eitem["assigned_to_org_name"] = self.get_enrollment(eitem['assigned_to_uuid'], item_date_utc)
            eitem["assigned_to_domain"] = self.get_domain(identity)
            eitem["assigned_to_bot"] = self.is_bot(eitem['assigned_to_uuid'])
        if 'reporter' in item['data']:
            identity = BugzillaEnrich.get_sh_identity({'reporter':item["data"]['reporter']})
            eitem['reporter_uuid'] = self.get_uuid(identity, self.get_connector_name())
            eitem['reporter_name'] = identity['name']
            item_date = item['data'][self.get_field_date()][0]['__text__']
            item_date_dt = parser.parse(item_date)
            item_date_utc = (item_date_dt-item_date_dt.utcoffset()).replace(tzinfo=None)
            eitem["reporter_org_name"] = self.get_enrollment(eitem['reporter_uuid'], item_date_utc)
            eitem["reporter_domain"] = self.get_domain(identity)
            eitem["reporter_bot"] = self.is_bot(eitem['reporter_uuid'])

        # Unify fields name
        eitem["author_uuid"] = eitem["reporter_uuid"]
        eitem["author_name"] = eitem["reporter_name"]
        eitem["author_org_name"] = eitem["reporter_org_name"]
        eitem["author_domain"] = eitem["reporter_domain"]

        return eitem

    def get_item_project(self, item):
        """ Get project mapping enrichment field """
        ds_name = "its"  # data source name in projects map
        url = item['origin']
        # https://bugs.eclipse.org/bugs/buglist.cgi?product=Mylyn%20Tasks
        product = item['data']['product'][0]['__text__']
        repo = url+"/buglist.cgi?product="+product
        try:
            project = (self.prjs_map[ds_name][repo])
        except KeyError:
            # logging.warning("Project not found for repository %s" % (repo))
            project = None
        return {"project": project}

    def get_identities(self, item):
        ''' Return the identities from an item '''

        identities = []

        if 'activity' in item["data"]:
            for event in item["data"]['activity']:
                identities.append(self.get_sh_identity(event))
        if 'long_desc' in item["data"]:
            for comment in item["data"]['long_desc']:
                identities.append(self.get_sh_identity(comment))
        elif 'assigned_to' in item["data"]:
            identities.append(self.get_sh_identity({'assigned_to':
                                                    item["data"]['assigned_to']}))
        elif 'reporter' in item["data"]:
            identities.append(self.get_sh_identity({'reporter':
                                                    item["data"]['reporter']}))
        elif 'qa_contact' in item["data"]:
            identities.append(self.get_sh_identity({'qa_contact':
                                                    item['qa_contact']}))
        return identities

    def enrich_issue(self, item):

        if 'bug_id' not in item['data']:
            logging.warning("Dropped bug without bug_id %s", issue)
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

        # dizquierdo specification

        eitem['changes'] = len(item['data']['activity'])

        eitem['labels'] = item['data']['keywords']
        eitem['priority'] = item['data']['priority'][0]['__text__']
        eitem['severity'] = item['data']['bug_severity'][0]['__text__']
        eitem['op_sys'] = item['data']['op_sys'][0]['__text__']
        eitem['product'] = item['data']['product'][0]['__text__']
        eitem['project_name'] = item['data']['product'][0]['__text__']
        eitem['component'] = item['data']['component'][0]['__text__']
        eitem['platform'] = item['data']['rep_platform'][0]['__text__']
        if '__text__' in item['data']['resolution'][0]:
            eitem['resolution'] = item['data']['resolution'][0]['__text__']
        if 'watchers' in item['data']:
            eitem['watchers'] = item['data']['watchers'][0]['__text__']
        if 'votes' in item['data']:
            eitem['votes'] = item['data']['votes'][0]['__text__']

        if "assigned_to" in issue:
            if "name" in issue["assigned_to"][0]:
                eitem["assigned"] = issue["assigned_to"][0]["name"]
            if "__text__" in issue["assigned_to"][0]:
                eitem["assignee_email"] = issue["assigned_to"][0]["__text__"]


        if "reporter" in issue:
            if "name" in issue["reporter"][0]:
                eitem["reporter_name"] = issue["reporter"][0]["name"]
                eitem["author_name"] = issue["reporter"][0]["name"]
            if "__text__" in issue["reporter"][0]:
                eitem["reporter_email"] = issue["reporter"][0]["__text__"]
                eitem["author_email"] = issue["reporter"][0]["__text__"]

        date_ts = parser.parse(issue['creation_ts'][0]['__text__'])
        eitem['creation_date'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')


        eitem["bug_id"] = issue['bug_id'][0]['__text__']
        eitem["status"]  = issue['bug_status'][0]['__text__']
        if "short_desc" in issue:
            if "__text__" in issue["short_desc"][0]:
                eitem["main_description"]  = issue['short_desc'][0]['__text__']
        if "summary" in issue:
            if "__text__" in issue["summary"][0]:
                eitem["summary"]  = issue['summary'][0]['__text__']


        # Fix dates
        date_ts = parser.parse(issue['delta_ts'][0]['__text__'])
        eitem['changeddate_date'] = date_ts.isoformat()
        eitem['delta_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')

        # Add extra JSON fields used in Kibana (enriched fields)
        eitem['comments'] = 0
        eitem['url'] = None

        if 'long_desc' in issue:
            eitem['comments'] = len(issue['long_desc'])
        eitem['url'] = item['origin'] + "/show_bug.cgi?id=" + issue['bug_id'][0]['__text__']
        eitem['resolution_days'] = \
            get_time_diff_days(eitem['creation_date'], eitem['delta_ts'])
        eitem['timeopen_days'] = \
            get_time_diff_days(eitem['creation_date'], datetime.utcnow())

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(item))

        eitem.update(self.get_grimoire_fields(eitem['creation_date'],"bug"))

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
