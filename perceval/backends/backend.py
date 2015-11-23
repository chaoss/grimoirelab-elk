#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Perceval Backend base class 
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

''' Backend define the API to be supported in Perceval data sources backends '''

import json
import logging
import os
from os.path import expanduser, join
import requests
import traceback

from perceval.cache import CacheItems


class Backend(object):

    # Public Perceval backend API 
    def fetch(self):
        ''' Returns an iterator for the data gathered '''
        raise NotImplementedError


    def get_id(self):
        '''Unique identifier for a backend instance '''
        raise NotImplementedError


    def set_elastic(self, elastic):
        ''' Elastic used to store last data source state '''
        self.elastic = elastic


    # Internal class implementation

    @classmethod
    def add_params(cls, cmdline_parser):
        ''' Shared params in all backends '''

        parser = cmdline_parser

        parser.add_argument("--no_incremental",  action='store_true',
                            help="don't use last state for data source")
        parser.add_argument("--cache",  action='store_true',
                            help="Use cache")
        parser.add_argument("--debug",  action='store_true',
                            help="Increase logging to debug")
        parser.add_argument("-e", "--elastic_host",  default="127.0.0.1",
                            help="Host with elastic search" +
                            "(default: 127.0.0.1)")
        parser.add_argument("--elastic_port",  default="9200",
                            help="elastic search port " +
                            "(default: 9200)")


    def __init__(self, use_cache = False, incremental = True):

        cache_dir = os.path.join(self._get_storage_dir(),"cache")
        self.cache = CacheItems(cache_dir, self.get_field_unique_id())
        self.use_cache = use_cache
        self.incremental = incremental

        # Create storage dir if it not exists
        storage_dir = self._get_storage_dir()
        if not os.path.isdir(storage_dir):
            os.makedirs(storage_dir)

        if self.use_cache:
            # Don't use history data. Will be generated from cache.
            self.incremental = False

        else:
            if not self.incremental:
                self.cache.clean()  # Cache will be refreshed


    def get_field_unique_id(self):
        ''' Field with the unique id for the JSON items '''
        raise NotImplementedError

    def get_field_date(self):
        ''' Field with the date in the JSON items '''
        raise NotImplementedError

    def get_elastic_mappings(self):
        ''' Specific mappings for the State in ES '''
        pass


    def _items_to_es(self, json_items):
        ''' Append items JSON to ES (data source state) '''

        if len(json_items) == 0:
            return

        logging.info("Adding items to state for %s (%i items)" %
                      (self.get_name(), len(json_items)))

        field_id = self.get_field_unique_id()

        self.elastic.bulk_upload_sync(json_items, field_id, self.incremental)


    @classmethod
    def get_name(cls):
        ''' Human name for the backend class '''

        return cls.name



    def _get_storage_dir(self):

        home = expanduser("~")
        '''Get directory in which to store backend data'''
        _dir = join(home, ".perceval", self.get_name(), self.get_id())

        return _dir


    # Iterator

    def _get_elastic_items(self):

        url = self.elastic.index_url
        url += "/_search?from=%i&size=%i" % (self.elastic_from,
                                             self.elastic_page)

        if self.incremental:
            date_field = self.get_field_date()
            last_date = self.elastic.get_last_date(date_field)
            last_date = last_date.replace(" ","T")  # elastic format


            filter_ = '''
            {
                "query": {
                    "bool": {
                        "must": [
                            {"range":
                                {"%s": {"gte": "%s"}}
                            }
                        ]
                    }
                }
            }
            ''' % (date_field, last_date)

            r = requests.post(url, data = filter_)

        else:
            r = requests.get(url)

        items = []

        for hit in r.json()["hits"]["hits"]:
            items.append(hit['_source'])

        return items

    def __iter__(self):

        self.elastic_from = 0
        self.elastic_page = 100
        self.iter_items = self._get_elastic_items()

        return self

    def __next__(self):

        if len(self.iter_items) > 0:
            return self.iter_items.pop()
        else:
            self.elastic_from += self.elastic_page
            self.iter_items = self._get_elastic_items()
            if len(self.iter_items) > 0:
                return self.__next__()
            else:
                raise StopIteration

