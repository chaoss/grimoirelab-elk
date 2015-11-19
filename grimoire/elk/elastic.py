#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Elastic Search basic utils
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
import requests
import time

class ElasticConnectException(Exception):
    message = "Can't connect to ElasticSearch"


class ElasticSearch(object):

    def __init__(self, host, port, index, mappings, clean = False):
        ''' clean: remove already exiting index '''

        self.url = "http://" + host + ":" + port
        self.index = index
        self.index_raw = index+"_raw"
        self.index_url = self.url+"/"+self.index
        self.index_raw_url = self.url+"/"+self.index_raw
        self.max_items_bulk = 500

        try:
            r = requests.get(self.index_url)
        except requests.exceptions.ConnectionError:
            raise ElasticConnectException()

        if r.status_code != 200:
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


    def bulk_upload(self, es_type, items, field_id):
        ''' Upload in controlled packs items to ES using bulk API '''

        max_items = self.max_items_bulk
        current = 0
        new_items = 0  # total items added with bulk
        total_search = 0  # total items found with search
        bulk_json = ""

        url = self.index_url+'/'+es_type+'/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))
        r = requests.get(self.index_url+'/'+es_type+'/_search?size=1')
        total = r.json()['hits']['total']  # Already existing items

        for item in items:
            if current >= max_items:
                requests.put(url, data=bulk_json)
                bulk_json = ""
                new_items += current
                current = 0
            data_json = json.dumps(item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (item[field_id])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        requests.put(url, data=bulk_json)
        new_items += current

        # Wait until in searches all items are returned
        total += new_items
        while total_search != total:
            time.sleep(0.1)
            r = requests.get(self.index_url+'/'+es_type+'/_search?size=1')
            total_search = r.json()['hits']['total']


    def create_mapping(self, mappings):

        for mapping in mappings:
            _type = mapping
            url = self.index_url
            url_type = url + "/" + _type

            url_map = url_type+"/_mapping"
            r = requests.put(url_map, data=mappings[mapping])

            if r.status_code != 200:
                logging.error("Error creating ES mappings %s" % (r.text))


    def get_last_date(self, _type, field, _filter = None):
        '''
            :_type: type in which to search the data
            :field: field with the data
            :_filter: additional filter to find the date
        '''

        last_date = None

        url = self.index_url
        url += "/" + _type + "/_search"

        if _filter:
            data_query = '''
                "query" : {
                    "term" : { "%s" : "%s"  }
                 },
            ''' % (_filter['name'], _filter['value'])

        else:
            data_query = ''

        data_agg = '''
            "aggs": {
                "1": {
                  "max": {
                    "field": "%s"
                  }
                }
            }
        ''' % (field)

        data_json = '''
        { %s  %s
        } ''' % (data_query, data_agg)

        res = requests.post(url, data=data_json)
        res_json = res.json()

        if 'aggregations' in res_json:
            if "value_as_string" in res_json["aggregations"]["1"]:
                last_date = res_json["aggregations"]["1"]["value_as_string"]
                last_date = parser.parse(last_date).replace(tzinfo=None)
                last_date = last_date.isoformat(" ")
            else:
                last_date = res_json["aggregations"]["1"]["value"]
                if last_date:
                    last_date = datetime.fromtimestamp(last_date)
                    last_date = last_date.isoformat(" ")

        return last_date

    def getGitHubCache(self, _type, _key):
        """ Get cache data for items of _type using _key as the cache dict key """

        cache = {}
        res_size = 100  # best size?
        _from = 0

        index_github = "github"

        elasticsearch_type = _type

        url = self.url + "/"+index_github
        url += "/"+elasticsearch_type
        url += "/_search" + "?" + "size=%i" % res_size
        r = requests.get(url)
        type_items = r.json()

        if 'hits' not in type_items:
            logging.info("No github %s data in ES" % (_type))

        else:
            while len(type_items['hits']['hits']) > 0:
                for hit in type_items['hits']['hits']:
                    item = hit['_source']
                    cache[item[_key]] = item
                _from += res_size
                r = requests.get(url+"&from=%i" % _from)
                type_items = r.json()
    
        return cache




