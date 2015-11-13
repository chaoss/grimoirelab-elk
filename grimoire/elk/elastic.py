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


from dateutil import parser
import logging
import requests

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


