#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Ocean lib
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

"""Ocean feeder for Elastic from  Perseval data"""


from datetime import datetime
import logging
import requests

class ElasticOcean(object):

    @classmethod
    def add_params(cls, cmdline_parser):
        """ Shared params in all backends """

        parser = cmdline_parser

        parser.add_argument("-e", "--elastic_url",  default="http://127.0.0.1:9200",
                            help="Host with elastic search" +
                            "(default: http://127.0.0.1:9200)")


    def __init__(self, perceval_backend, from_date=None, fetch_cache=False):

        self.perceval_backend = perceval_backend
        self.last_update = None  # Last update in ocean items index for feed
        self.from_date = from_date  # fetch from_date
        self.fetch_cache = fetch_cache  # fetch from cache

    def set_elastic(self, elastic):
        """ Elastic used to store last data source state """
        self.elastic = elastic

    def get_field_date(self):
        """ Field with the update in the JSON items. Now the same in all. """
        return "metadata__updated_on"

    def get_field_unique_id(self):
        """ Field with unique identifier in an item  """
        raise NotImplementedError

    def get_elastic_mappings(self):
        """ Specific mappings for the State in ES """
        pass

    def get_last_update_from_es(self, _filter = None):
        last_update = self.elastic.get_last_date(self.get_field_date(), _filter)

        return last_update

    def get_connector_name(self):
        """ Find the name for the current connector """
        from ..utils import get_connector_name
        return get_connector_name(type(self))

    def drop_item(self, item):
        """ Drop items not to be inserted in Elastic """
        return False

    def _fix_item(self, item):
        """ Some buggy data sources need fixing (like mbox and message-id) """
        pass

    def add_update_date(self, item):
        """ All item['__metadata__']['updated_on'] from perceval is epoch """
        entry_lastUpdated = datetime.fromtimestamp(item['updated_on'])
        item['metadata__updated_on'] = entry_lastUpdated.isoformat()

    def feed(self, from_date=None):
        """ Feed data in Elastic from Perceval """

        filter_ = None
        if self.get_connector_name() == "git":
            filter_ = {"name":"origin",
                       "value":self.perceval_backend.origin}
        self.last_update = self.get_last_update_from_es(filter_)
        if self.get_connector_name() == "mbox":
            self.last_update = None  # mbox does not support incremental
        last_update = self.last_update
        # last_update = '2015-12-28 18:02:00'
        if from_date:
            # Forced from backend command line.
            last_update = from_date

        logging.info("Incremental from: %s" % (last_update))

        task_init = datetime.now()

        items_pack = []  # to feed item in packs
        drop = 0
        if self.fetch_cache:
            items = self.perceval_backend.fetch_from_cache()
        else:
            if last_update:
                # Perceval backend from_date must not include timezone
                # It always uses the server datetime
                last_update = last_update.replace(tzinfo=None)
                items = self.perceval_backend.fetch(from_date=last_update)
            else:
                items = self.perceval_backend.fetch()
        for item in items:
            # print("%s %s" % (item['url'], item['lastUpdated_date']))
            # Add date field for incremental analysis if needed
            self.add_update_date(item)
            self._fix_item(item)
            if len(items_pack) >= self.elastic.max_items_bulk:
                self._items_to_es(items_pack)
                items_pack = []
            if not self.drop_item(item):
                items_pack.append(item)
            else:
                drop +=1
        self._items_to_es(items_pack)


        total_time_min = (datetime.now()-task_init).total_seconds()/60

        logging.debug("Dropped %i items using drop_item filter" % (drop))
        logging.info("Finished in %.2f min" % (total_time_min))

        return self


    def _items_to_es(self, json_items):
        """ Append items JSON to ES (data source state) """

        if len(json_items) == 0:
            return

        logging.info("Adding items to Ocean for %s (%i items)" %
                      (self, len(json_items)))

        field_id = self.get_field_unique_id()

        self.elastic.bulk_upload_sync(json_items, field_id)

    # Iterator
    def _get_elastic_items(self):

        def get_origin_filter():
            # To fix the origin for items
            filter_ = None
            if self.get_connector_name() == "git" and \
                self.perceval_backend.origin != "":
                logging.info("Feeding from origin %s" % (self.perceval_backend.origin))
                filter_ = {"name":"metadata__origin",
                           "value":self.perceval_backend.origin}
                origin_filter = '''
                    {
                        "term" : { "%s" : "%s"  }
                     }
                ''' % (filter_['name'], filter_['value'])
            else:
                origin_filter = None

            return origin_filter



        url = self.elastic.index_url
        # 1 minute to process the results of size items
        # In gerrit enrich with 500 items per page we need >1 min
        max_process_items_pack_time = "3m"  # 3 minutes
        url += "/_search?scroll=%s&size=%i" % (max_process_items_pack_time,
                                               self.elastic_page)

        if self.from_date and not self.elastic_scroll_id:
            # The filter in scroll api should be added the first query
            date_field = self.get_field_date()
            from_date = self.from_date.isoformat()

            origin_filter = get_origin_filter()

            filters = '''
                {"range":
                    {"%s": {"gte": "%s"}}
                }
            ''' % (date_field, from_date)

            if origin_filter:
                filters += ", %s" % (origin_filter)



            query = """
            {
                "query": {
                    "bool": {
                        "must": [%s]
                    }
                }
            }
            """ % (filters)

            r = requests.post(url, data=query)

        else:
            if self.elastic_scroll_id:
                url = self.elastic.url
                url += "/_search/scroll"
                scroll_data = {
                    "scroll" : max_process_items_pack_time,
                    "scroll_id" : self.elastic_scroll_id
                    }
                # r = requests.post(url, data=json.dumps(scroll_data))
                # For compatibility with 1.7
                get_scroll_data = "scroll=%s&scroll_id=%s" % \
                    (max_process_items_pack_time, self.elastic_scroll_id)
                r = requests.get(url+"?"+ get_scroll_data)

            else:
                origin_filter = get_origin_filter()

                if origin_filter:
                    filters = origin_filter
                    query = """
                    {
                        "query": {
                            "bool": {
                                "must": [%s]
                            }
                        }
                    }
                    """ % (filters)
                    r = requests.post(url, data=query)
                else:
                    r = requests.get(url)

        items = []
        try:
            rjson = r.json()
        except:
            logging.warning("No JSON found in %s" % (r.text))
            logging.warning("No results found from %s" % (url))

        if rjson and "_scroll_id" in rjson:
            self.elastic_scroll_id = rjson["_scroll_id"]
        else:
            self.elastic_scroll_id = None

        if rjson and "hits" in rjson:
            for hit in rjson["hits"]["hits"]:
                items.append(hit['_source'])
        else:
            logging.warning("No results found from %s" % (url))

        return items


    def __iter__(self):

        self.elastic_scroll_id = None
        # In large projects like Eclipse commits, 100 is too much
        # self.elastic_page = 100
        self.elastic_page = 10
        self.iter_items = self._get_elastic_items()

        return self

    def __next__(self):

        if len(self.iter_items) > 0:
            return self.iter_items.pop()
        else:
            if self.elastic_scroll_id:
                self.iter_items = self._get_elastic_items()
            if len(self.iter_items) > 0:
                return self.__next__()
            else:
                raise StopIteration
