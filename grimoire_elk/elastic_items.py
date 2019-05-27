# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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

"""Generates items from ElasticSearch based on filters """


import json
import logging
import re

from .enriched.utils import get_repository_filter, grimoire_con
from .elastic_mapping import Mapping

HEADER_JSON = {"Content-Type": "application/json"}
MAX_BULK_UPDATE_SIZE = 1000

FILTER_DATA_ATTR = 'data.'
FILTER_SEPARATOR = r",\s*%s" % FILTER_DATA_ATTR
PROJECTS_JSON_LABELS_PATTERN = r".*(--labels=\[(.*)\]).*"

logger = logging.getLogger(__name__)


class ElasticItems:

    mapping = Mapping

    # In large projects like Eclipse commits, 100 is too much
    # Change it from p2o command line or mordred config
    scroll_size = 100

    def __init__(self, perceval_backend, from_date=None, insecure=True, offset=None):

        self.perceval_backend = perceval_backend
        self.last_update = None  # Last update in ocean items index for feed
        self.from_date = from_date  # fetch from_date
        self.offset = offset  # fetch from offset
        self.filter_raw = None  # to filter raw items from Ocean
        self.filter_raw_dict = []
        self.filter_raw_should = None  # to filter raw items from Ocean
        self.filter_raw_should_dict = []
        self.projects_json_repo = None
        self.repo_labels = None

        self.requests = grimoire_con(insecure)
        self.elastic = None
        self.elastic_url = None
        self.cfg_section_name = None

    def get_repository_filter_raw(self, term=False):
        """ Returns the filter to be used in queries in a repository items """
        perceval_backend_name = self.get_connector_name()
        filter_ = get_repository_filter(self.perceval_backend, perceval_backend_name, term)
        return filter_

    def get_field_date(self):
        """ Field with the update in the JSON items. Now the same in all. """
        return "metadata__updated_on"

    def get_incremental_date(self):
        """
        Field with the date used for incremental analysis.
        """
        return "metadata__timestamp"

    def set_projects_json_repo(self, repo):
        self.projects_json_repo = repo

    def set_repo_labels(self, labels):
        self.repo_labels = labels

    @staticmethod
    def extract_repo_labels(repo):
        """Extract the labels declared in the repositories within the projects.json, and
        remove them to avoid breaking already existing functionalities.

        :param repo: repo url in projects.json
        """
        processed_repo = repo
        labels_lst = []

        pattern = re.compile(PROJECTS_JSON_LABELS_PATTERN)
        matchObj = pattern.match(repo)

        if matchObj:
            labels_info = matchObj.group(1)
            labels = matchObj.group(2)
            labels_lst = [l.strip() for l in labels.split(',')]
            processed_repo = processed_repo.replace(labels_info, '')

        return processed_repo, labels_lst

    @staticmethod
    def __process_filter(fltr_raw):
        fltr = fltr_raw

        if not fltr_raw.startswith(FILTER_DATA_ATTR):
            fltr = FILTER_DATA_ATTR + fltr_raw

        fltr_params = fltr.split(":", 1)
        fltr_name = fltr_params[0].strip().replace('"', '')
        fltr_value = fltr_params[1].strip().replace('"', '')

        fltr_dict = {
            'name': fltr_name,
            'value': fltr_value
        }

        return fltr_dict

    def set_filter_raw(self, filter_raw):
        """Filter to be used when getting items from Ocean index"""

        self.filter_raw = filter_raw

        self.filter_raw_dict = []
        splitted = re.compile(FILTER_SEPARATOR).split(filter_raw)
        for fltr_raw in splitted:
            fltr = self.__process_filter(fltr_raw)

            self.filter_raw_dict.append(fltr)

    def set_filter_raw_should(self, filter_raw_should):
        """Bool filter should to be used when getting items from Ocean index"""

        self.filter_raw_should = filter_raw_should

        self.filter_raw_should_dict = []
        splitted = re.compile(FILTER_SEPARATOR).split(filter_raw_should)
        for fltr_raw in splitted:
            fltr = self.__process_filter(fltr_raw)

            self.filter_raw_should_dict.append(fltr)

    def get_connector_name(self):
        """ Find the name for the current connector """
        from .utils import get_connector_name
        return get_connector_name(type(self))

    def set_cfg_section_name(self, cfg_section_name):
        self.cfg_section_name = cfg_section_name

    def set_from_date(self, last_enrich_date):
        self.from_date = last_enrich_date

    # Items generator
    def fetch(self, _filter=None, ignore_incremental=False):
        """ Fetch the items from raw or enriched index. An optional _filter
        could be provided to filter the data collected """

        logger.debug("Creating a elastic items generator.")

        scroll_id = None
        page = self.get_elastic_items(scroll_id, _filter=_filter, ignore_incremental=ignore_incremental)

        if not page:
            return []

        scroll_id = page["_scroll_id"]
        scroll_size = page['hits']['total']

        if scroll_size == 0:
            logger.warning("No results found from %s", self.elastic.anonymize_url(self.elastic.index_url))
            return

        while scroll_size > 0:

            logger.debug("Fetching from %s: %d received", self.elastic.anonymize_url(self.elastic.index_url),
                         len(page['hits']['hits']))
            for item in page['hits']['hits']:
                eitem = item['_source']
                yield eitem

            page = self.get_elastic_items(scroll_id, _filter=_filter, ignore_incremental=ignore_incremental)

            if not page:
                break

            scroll_size = len(page['hits']['hits'])

        logger.debug("Fetching from %s: done receiving", self.elastic.anonymize_url(self.elastic.index_url))

    def get_elastic_items(self, elastic_scroll_id=None, _filter=None, ignore_incremental=False):
        """ Get the items from the index related to the backend applying and
        optional _filter if provided"""

        headers = {"Content-Type": "application/json"}

        if not self.elastic:
            return None
        url = self.elastic.index_url
        # 1 minute to process the results of size items
        # In gerrit enrich with 500 items per page we need >1 min
        # In Mozilla ES in Amazon we need 10m
        max_process_items_pack_time = "10m"  # 10 minutes
        url += "/_search?scroll=%s&size=%i" % (max_process_items_pack_time,
                                               self.scroll_size)

        if elastic_scroll_id:
            """ Just continue with the scrolling """
            url = self.elastic.url
            url += "/_search/scroll"
            scroll_data = {
                "scroll": max_process_items_pack_time,
                "scroll_id": elastic_scroll_id
            }
            query_data = json.dumps(scroll_data)
        else:
            # If using a perceval backends always filter by repository
            # to support multi repository indexes
            filters_dict = self.get_repository_filter_raw(term=True)
            if filters_dict:
                filters = json.dumps(filters_dict)
            else:
                filters = ''

            if self.filter_raw:
                for fltr in self.filter_raw_dict:
                    filters += '''
                        , {"term":
                            { "%s":"%s"  }
                        }
                    ''' % (fltr['name'], fltr['value'])

            if _filter:
                filter_str = '''
                    , {"terms":
                        { "%s": %s }
                    }
                ''' % (_filter['name'], _filter['value'])
                # List to string conversion uses ' that are not allowed in JSON
                filter_str = filter_str.replace("'", "\"")
                filters += filter_str

            # The code below performs the incremental enrichment based on the last value of `metadata__timestamp`
            # in the enriched index, which is calculated in the TaskEnrich before enriching the single repos that
            # belong to a given data source. The old implementation of the incremental enrichment, which consisted in
            # collecting the last value of `metadata__timestamp` in the enriched index for each repo, didn't work
            # for global data source (which are collected globally and only partially enriched).
            if self.from_date and not ignore_incremental:
                date_field = self.get_incremental_date()
                from_date = self.from_date.isoformat()

                filters += '''
                    , {"range":
                        {"%s": {"gte": "%s"}}
                    }
                ''' % (date_field, from_date)
            elif self.offset and not ignore_incremental:
                filters += '''
                    , {"range":
                        {"offset": {"gte": %i}}
                    }
                ''' % self.offset

            # Order the raw items from the old ones to the new so if the
            # enrich process fails, it could be resume incrementally
            order_query = ''
            order_field = None
            if self.perceval_backend:
                order_field = self.get_incremental_date()
            if order_field is not None:
                order_query = ', "sort": { "%s": { "order": "asc" }} ' % order_field

            filters_should = ''
            if self.filter_raw_should:
                for fltr in self.filter_raw_should_dict:
                    filters_should += '''
                        {"prefix":
                            { "%s":"%s"  }
                        },''' % (fltr['name'], fltr['value'])

                filters_should = filters_should.rstrip(',')
                query_should = '{"bool": {"should": [%s]}}' % filters_should
                filters += ", " + query_should

            # Fix the filters string if it starts with "," (empty first filter)
            if filters.lstrip().startswith(','):
                filters = filters.lstrip()[1:]

            query = """
            {
                "query": {
                    "bool": {
                        "filter": [%s]
                    }
                } %s
            }
            """ % (filters, order_query)

            logger.debug("Raw query to %s\n%s", self.elastic.anonymize_url(url),
                         json.dumps(json.loads(query), indent=4))
            query_data = query

        rjson = None
        try:
            res = self.requests.post(url, data=query_data, headers=headers)
            res.raise_for_status()
            rjson = res.json()
        except Exception:
            # The index could not exists yet or it could be empty
            logger.warning("No results found from %s", self.elastic.anonymize_url(url))

        return rjson
