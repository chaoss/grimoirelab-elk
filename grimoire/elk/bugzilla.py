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

from dateutil import parser
import json
import requests
from urllib.parse import urlparse

from perceval.utils import get_time_diff_days


class BugzillaElastic(object):

    def __init__(self, bugzilla, elastic):
        self.bugzilla = bugzilla
        self.elastic = elastic

    def enrich_issue(self, issue):

        def get_bugzilla_url():
            u = urlparse(self.bugzilla.url)
            return u.scheme+"//"+u.netloc

        # Fix dates
        date_ts = parser.parse(issue['creation_ts'])
        issue['creation_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')
        date_ts = parser.parse(issue['delta_ts'])
        issue['delta_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')

        # Add extra JSON fields used in Kibana
        issue['number_of_comments'] = 0
        issue['time_to_last_update_days'] = None
        issue['url'] = None

        issue['number_of_comments'] = len(issue['long_desc'])
        issue['url'] = get_bugzilla_url() + "show_bug.cgi?id=" + issue['bug_id']
        issue['time_to_last_update_days'] = \
            get_time_diff_days(issue['creation_ts'], issue['delta_ts'])

        return issue

    def issues_list_to_es(self):

        # TODO: use bulk API
        elastic_type = "issues_list"

        # In this client, we will publish all data in Elastic Search
        for issue in self.bugzilla.fetch():
            data_json = json.dumps(issue)
            url = self.elastic.index_url
            url += "/"+elastic_type
            url += "/"+str(issue["bug_id"])
            requests.put(url, data=data_json)

    def issues_to_es(self):

        # TODO: use bulk API

        elastic_type = "issues"

        for issue in self.bugzilla.fetch():
            self.enrich_issue(issue)
            data_json = json.dumps(issue)
            url = self.elastic.index_url
            url += "/"+elastic_type
            url += "/"+str(issue["bug_id"])
            requests.put(url, data=data_json)

    @classmethod
    def get_elastic_mappings(cls):
        ''' Specific mappings needed for ES '''

        elastic_mappings = {}

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

        elastic_mappings['issues_list'] = mapping

        return elastic_mappings
