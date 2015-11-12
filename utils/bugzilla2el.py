#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Bugzilla tickets for Elastic Search
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
# Bugzilla backend for Perseval

import argparse
import json
import logging
import requests
from dateutil import parser
from datetime import datetime
from urllib.parse import urlparse, urljoin

from perceval.backends.bugzilla import Bugzilla
from perceval.utils import get_time_diff_days

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user",
                        help="Bugzilla user")
    parser.add_argument("--password",
                        help="Bugzilla user password")
    parser.add_argument("-d", "--delay", default="1",
                        help="delay between requests in seconds (1s default)")
    parser.add_argument("-u", "--url", required=True,
                        help="Bugzilla url")
    parser.add_argument("-e", "--elastic_host",  default="127.0.0.1",
                        help="Host with elastic search" +
                        "(default: 127.0.0.1)")
    parser.add_argument("--elastic_port",  default="9200",
                        help="elastic search port " +
                        "(default: 9200)")
    parser.add_argument("--no_history",  action='store_true',
                        help="don't use history for repository")
    parser.add_argument("--detail",  default="change",
                        help="list, issue or change (default) detail")
    parser.add_argument("--nissues",  default=200, type=int,
                        help="Number of XML issues to get per query")
    parser.add_argument("--cache",  action='store_true',
                        help="Use perseval cache")


    args = parser.parse_args()
    return args


class ElasticSearch(object):

    def __init__(self, host, port, index, mappings, clean = False):
        ''' clean: remove already exiting index '''

        self.url = "http://" + host + ":" + port
        self.index = index
        self.index_raw = index+"_raw"
        self.index_url = self.url+"/"+self.index
        self.index_raw_url = self.url+"/"+self.index_raw

        if requests.get(self.index_url).status_code != 200:
            # Index does no exists
            requests.post(self.index_url)
            logging.info("Created index " + self.index_url)
        else:
            if clean:
                requests.delete(self.index_url)
                requests.post(self.index_url)
                logging.info("Deleted and created index " + self.index_url)
        if mappings:
            self.create_mapping(mappings)


    def create_mapping(self, mappings):

        for mapping in mappings:
            _type = mapping
            url = self.index_url
            url_type = url + "/" + _type

            url_map = url_type+"/_mapping"
            r = requests.put(url_map, data=mappings[mapping])

            if r.status_code != 200:
                logging.error("Error creating ES mappings %s" % (r.text))

    def get_last_date(self, _type, field):

        last_date = None

        url = self.index_url
        url += "/" + _type + "/_search"

        data_json = '''
        {
            "aggs": {
                "1": {
                  "max": {
                    "field": "%s"
                  }
                }
            }
        }
        ''' % (field)

        res = requests.post(url, data=data_json)
        res_json = res.json()

        if 'aggregations' in res_json:
            if "value_as_string" in res_json["aggregations"]["1"]:
                last_date = res_json["aggregations"]["1"]["value_as_string"]
                last_date = parser.parse(last_date).replace(tzinfo=None)
                last_date = last_date.isoformat(" ")

        return last_date



class BugzillaElastic(object):

    def __init__(self, bugzilla, elastic):
        self.bugzilla = bugzilla
        self.elastic = elastic

    def enrich_issue(self, issue):

        def get_bugzilla_url():
            u = urlparse(self.bugzilla.url)
            return u.scheme+"//"+u.netloc

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
        for issue in bugzilla.fetch():
            data_json = json.dumps(issue)
            url = self.elastic.index_url
            url += "/"+elastic_type
            url += "/"+str(issue["bug_id"])
            requests.put(url, data=data_json)

    def issues_to_es(self):

        # TODO: use bulk API

        elastic_type = "issues"

        for issue in bugzilla.fetch():
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


if __name__ == '__main__':
    app_init = datetime.now()
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)

    args = parse_args()

    bugzilla = Bugzilla(args.url, args.nissues, args.detail,
                        not args.no_history, args.cache)

    es_index_bugzilla = "bugzilla_" + bugzilla.get_id()
    es_mappings = BugzillaElastic.get_elastic_mappings()
    elastic = ElasticSearch(args.elastic_host,
                            args.elastic_port,
                            es_index_bugzilla, es_mappings, args.no_history)

    ebugzilla = BugzillaElastic(bugzilla, elastic)

    bugzilla.fetch()

    if False:
        if args.detail == "list":
            ebugzilla.issues_list_to_es()
        else:
            ebugzilla.issues_to_es()


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
