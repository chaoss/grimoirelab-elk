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
from time import time, sleep

class ElasticConnectException(Exception):
    message = "Can't connect to ElasticSearch"


class ElasticSearch(object):

    def __init__(self, host, port, index, mappings, clean = False):
        ''' clean: remove already exiting index '''

        self.url = "http://" + host + ":" + port
        self.index = index
        self.index_url = self.url+"/"+self.index
        self.max_items_bulk = 500
        self.wait_bulk_seconds = 2  # time to wait to complete a bulk operation

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
            self.create_mappings(mappings)


    def bulk_upload_sync(self, items, field_id, incremental = False):
        ''' Upload in controlled packs items to ES using bulk API
            and wait until the items appears in searches '''

        # After a bulk upload the searches are not refreshed real time
        # This method waits until the upload is visible in searches

        max_items = self.max_items_bulk
        current = 0
        new_items = 0  # total items added with bulk
        total_search = 0  # total items found with search
        bulk_json = ""

        url = self.index_url+'/items/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))
        r = requests.get(self.index_url+'/_search?size=1')
        total = r.json()['hits']['total']  # Already existing items


        for item in items:
            if current >= max_items:
                task_init = time()
                requests.put(url, data=bulk_json)
                bulk_json = ""
                new_items += current
                current = 0
                logging.debug("bulk packet sent (%.2f sec, %i total)"
                              % (time()-task_init, new_items))
            data_json = json.dumps(item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (item[field_id])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        task_init = time()
        requests.put(url, data=bulk_json)
        new_items += current
        logging.debug("bulk packet sent (%.2f sec prev, %i total)"
                      % (time()-task_init, new_items))

        # Wait until in searches all items are returned
        # In incremental update, some items are updates not additions so
        # this check will fail. Exist ok in the timeout for this case.
        total += new_items
        search_start = datetime.now()
        while total_search != total:
            sleep(0.1)
            r = requests.get(self.index_url+'/_search?size=1')
            total_search = r.json()['hits']['total']
            if (datetime.now()-search_start).total_seconds() > self.wait_bulk_seconds:
                logging.warning("Bulk data does not appear as NEW after %is" % (self.wait_bulk_seconds))
                logging.debug("%i item updates" % (total-total_search))
                if not incremental:
                    raise
                break


    def create_mappings(self, mappings):

        for _type in mappings:
            url_map = self.index_url + "/"+_type+"/_mapping"
            r = requests.put(url_map, data=mappings[_type])

            if r.status_code != 200:
                logging.error("Error creating ES mappings %s" % (r.text))


    def get_last_date(self, field, _filter = None):
        '''
            :field: field with the data
            :_filter: additional filter to find the date
        '''

        last_date = None

        url = self.index_url
        url += "/_search"

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



