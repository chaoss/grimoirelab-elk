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

from time import time
from dateutil import parser
import json
import logging
import requests
from urllib.parse import urlparse

from grimoire.elk.enrich import Enrich


from perceval.utils import get_time_diff_days

import sortinghat.utils as utils
from sortinghat.db.database import Database
from sortinghat import api
from sortinghat.exceptions import AlreadyExistsError, NotFoundError


class BugzillaEnrich(Enrich):

    def __init__(self, bugzilla, **nouse):
        self.bugzilla = bugzilla
        self.elastic = None
        self.sh_db = Database("root", "", "ocean_sh", "mariadb")


    def set_elastic(self, elastic):
        self.elastic = elastic

    def enrich_issue(self, issue):

        def get_bugzilla_url():
            u = urlparse(self.bugzilla.url)
            return u.scheme+"//"+u.netloc

        def get_uuid(identity):
            iden = {}
            for field in ['email', 'name', 'username']:
                iden[field] = None
                if field in identity:
                    iden[field] = identity[field]
            id = utils.uuid(self.bugzilla.get_name(), iden['email'],
                            iden['name'], iden['username'])

            try:
                # Find the uuid for a given id. A bit hacky in SH yet
                api.add_identity(self.sh_db, self.bugzilla.get_name(),
                                 iden['email'], iden['name'],
                                 iden['username'])
            except AlreadyExistsError as ex:
                uuid = ex.uuid
                u = api.unique_identities(self.sh_db, uuid)[0]
                uuid = u.uuid
            except NotFoundError:
                logging.error("Identity found in Sorting Hat which is not unique")
                logging.error("%s %s" % (identity, uuid))
                uuid = None
            return uuid

        # Fix dates
        date_ts = parser.parse(issue['creation_ts'])
        issue['creation_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')
        date_ts = parser.parse(issue['delta_ts'])
        issue['delta_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')

        # Add extra JSON fields used in Kibana (enriched fields)
        issue['number_of_comments'] = 0
        issue['time_to_last_update_days'] = None
        issue['url'] = None

        issue['number_of_comments'] = len(issue['long_desc'])
        issue['url'] = get_bugzilla_url() + "show_bug.cgi?id=" + issue['bug_id']
        issue['time_to_last_update_days'] = \
            get_time_diff_days(issue['creation_ts'], issue['delta_ts'])

        # Sorting Hat integration: reporter and assigned_to uuids
        identity = {}
        if 'assigned_to' in issue:
            identity['username'] = issue['assigned_to']
            if 'assigned_to_name' in issue:
                identity['name'] = issue['assigned_to_name']
            issue['assignet_to_uuid'] = get_uuid(identity)
        identity = {}
        if 'reporter' in issue:
            identity['username'] = issue['reporter']
            if 'reporter_name' in issue:
                identity['name'] = issue['reporter_name']
            issue['reporter_uuid'] = get_uuid(identity)

        return issue


    def enrich_items(self, items):
        if self.bugzilla.detail == "list":
            self.issues_list_to_es(items)
        else:
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
                requests.put(url, data=bulk_json)
                bulk_json = ""
                total += current
                current = 0
                logging.debug("bulk packet sent (%.2f sec, %i total)"
                              % (time()-task_init, total))
            data_json = json.dumps(issue)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (issue["bug_id"])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        task_init = time()
        total += current
        requests.put(url, data=bulk_json)
        logging.debug("bulk packet sent (%.2f sec, %i total)"
                      % (time()-task_init, total))


    def issues_to_es(self, items):

        elastic_type = "issues"

        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        url = self.elastic.index_url+'/' + elastic_type + '/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))

        for issue in items:
            if current >= max_items:
                requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0
            self.enrich_issue(issue)
            data_json = json.dumps(issue)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (issue["bug_id"])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        requests.put(url, data=bulk_json)

        logging.debug("Adding issues to ES Done")


    def get_elastic_mappings(self):
        ''' Specific mappings needed for ES '''

        mapping = '''
        {
            "properties": {
               "product": {
                  "type": "string",
                  "index":"not_analyzed"
               },
               "component": {
                  "type": "string",
                  "index":"not_analyzed"
               },
               "assigned_to": {
                  "type": "string",
                  "index":"not_analyzed"
               }
            }
        }
        '''

        return {"items":mapping}
