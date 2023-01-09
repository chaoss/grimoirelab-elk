# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2023 Bitergia
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

"""Generates items from ElasticSearch based on filters """


import json
import logging
import re
import time

from .enriched.utils import get_repository_filter, get_confluence_spaces_filter, grimoire_con, anonymize_url
from .elastic_mapping import Mapping

HEADER_JSON = {"Content-Type": "application/json"}
MAX_BULK_UPDATE_SIZE = 1000

FILTER_DATA_ATTR = 'data.'
FILTER_SEPARATOR = r",\s*%s" % FILTER_DATA_ATTR
PROJECTS_JSON_LABELS_PATTERN = r".*(--labels=\[(.*)\]).*"
PROJECTS_JSON_SPACES_PATTERN = r".*(--spaces=\[(.*)\]).*"

logger = logging.getLogger(__name__)


class ElasticItems:

    mapping = Mapping

    # In large projects like Eclipse commits, 100 is too much
    # Change it from p2o command line or mordred config
    scroll_size = 100
    scroll_wait = 900

    def __init__(self, perceval_backend, from_date=None, insecure=True, offset=None, to_date=None):
        """Class to perform operations over the items stored in a ES index.

        :param perceval_backend: Perceval backend object
        :param from_date: Date obj used to extract the items in an ES index after a given date
        :param insecure: support https with invalid certificates
        :param offset: Offset number used to extract the items in an ES index after a given offset ID
        :param to_date: Date obj used to extract the items in an ES index before a given date
        """
        self.perceval_backend = perceval_backend
        self.last_update = None  # Last update in ocean items index for feed
        self.from_date = from_date  # fetch from_date
        self.to_date = to_date  # fetch to_date
        self.offset = offset  # fetch from offset
        self.filter_raw = None  # to filter raw items from Ocean
        self.filter_raw_dict = []
        self.projects_json_repo = None
        self.repo_labels = None
        self.repo_spaces = None

        self.requests = grimoire_con(insecure)
        self.elastic = None
        self.elastic_url = None
        self.cfg_section_name = None

    def get_repository_filter_raw(self, term=False):
        """Returns the filter to be used in queries in a repository items"""

        perceval_backend_name = self.get_connector_name()
        filter_ = get_repository_filter(self.perceval_backend, perceval_backend_name, term)
        return filter_

    def get_confluence_spaces(self, repo_spaces):
        """Returns the spaces to be used in queries in a confluence items"""

        perceval_backend_name = self.get_connector_name()
        filter_ = get_confluence_spaces_filter(repo_spaces, perceval_backend_name)
        return filter_

    def get_field_date(self):
        """Field with the update in the JSON items. Now the same in all."""

        return "metadata__updated_on"

    def get_incremental_date(self):
        """Field with the date used for incremental analysis."""

        return "metadata__timestamp"

    def set_projects_json_repo(self, repo):
        """Set the repo extracted from the projects.json

        :param repo: target repo
        """
        self.projects_json_repo = repo

    def set_repo_labels(self, labels):
        """Set the labels of the repo

        :param labels: list of labels (str)
        """
        self.repo_labels = labels

    def set_repo_spaces(self, spaces):
        """Set the spaces of the repo

        :param spaces: list of labels (str)
        """
        self.repo_spaces = spaces

    @staticmethod
    def extract_repo_tags(repo, tag="labels"):
        """Extract the tags declared in the repositories within the projects.json, and
        remove them to avoid breaking already existing functionalities.

        :param repo: repo url in projects.json
        :param tag: labels | spaces
        """
        processed_repo = repo
        tags_lst = []

        tag_pattern = PROJECTS_JSON_LABELS_PATTERN
        if tag == "spaces":
            tag_pattern = PROJECTS_JSON_SPACES_PATTERN
        pattern = re.compile(tag_pattern)
        matchObj = pattern.match(repo)

        if matchObj:
            labels_info = matchObj.group(1)
            labels = matchObj.group(2)
            tags_lst = [label.strip() for label in labels.split(',')]
            processed_repo = processed_repo.replace(labels_info, '').strip()

        return processed_repo, tags_lst

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
        """Filter to be used when getting items from Ocean index

        :param filter_raw: str representation of the filter (e.g., "data.product:Add-on SDK",)
        """
        self.filter_raw = filter_raw

        self.filter_raw_dict = []
        splitted = re.compile(FILTER_SEPARATOR).split(filter_raw)
        for fltr_raw in splitted:
            fltr = self.__process_filter(fltr_raw)

            self.filter_raw_dict.append(fltr)

    def get_connector_name(self):
        """Find the name for the current connector"""

        from .utils import get_connector_name
        return get_connector_name(type(self))

    def set_cfg_section_name(self, cfg_section_name):
        """Set the cfg section name

        :param cfg_section_name: name of the cfg section
        """
        self.cfg_section_name = cfg_section_name

    def set_from_date(self, last_enrich_date):
        """Set the from date

        :param last_enrich_date: date of last enrichment
        """
        self.from_date = last_enrich_date

    def free_scroll(self, scroll_id=None):
        """ Free scroll after use"""
        if not scroll_id:
            return

        logger.debug("Releasing scroll_id={}".format(scroll_id))
        url = self.elastic.url + "/_search/scroll"
        headers = {"Content-Type": "application/json"}
        scroll_data = {"scroll_id": scroll_id}
        query_data = json.dumps(scroll_data)
        try:
            res = self.requests.delete(url, data=query_data, headers=headers)
            res.raise_for_status()
        except Exception:
            logger.debug("Error releasing scroll: {}/{}".format(anonymize_url(url), scroll_id))
            logger.debug("Error releasing scroll: {}".format(res.json()))

    # Items generator
    def fetch(self, _filter=None, ignore_incremental=False):
        """Fetch the items from raw or enriched index. An optional _filter can be
        provided to filter the data collected

        :param _filter: optional filter of data collected
        :param ignore_incremental: if True, incremental collection is ignored
        """

        logger.debug("Creating a elastic items generator.")

        scroll_id = None
        page = self.get_elastic_items(scroll_id, _filter=_filter, ignore_incremental=ignore_incremental)
        if page and 'too_many_scrolls' in page:
            sec = self.scroll_wait
            while sec > 0:
                logger.debug("Too many scrolls open, waiting up to {} seconds".format(sec))
                time.sleep(1)
                sec -= 1
                page = self.get_elastic_items(scroll_id, _filter=_filter, ignore_incremental=ignore_incremental)
                if not page:
                    logger.debug("Waiting for scroll terminated")
                    break
                if 'too_many_scrolls' not in page:
                    logger.debug("Scroll acquired after {} seconds".format(self.scroll_wait - sec))
                    break

        if not page:
            return []

        scroll_id = page["_scroll_id"]
        total = page['hits']['total']
        scroll_size = total['value'] if isinstance(total, dict) else total

        if scroll_size == 0:
            logger.debug("No results found from {} and filter {}".format(
                         anonymize_url(self.elastic.index_url), _filter))
            self.free_scroll(scroll_id)
            return

        while scroll_size > 0:

            logger.debug("Fetching from {}: {} received".format(
                         anonymize_url(self.elastic.index_url), len(page['hits']['hits'])))
            for item in page['hits']['hits']:
                eitem = item['_source']
                yield eitem

            page = self.get_elastic_items(scroll_id, _filter=_filter, ignore_incremental=ignore_incremental)

            if not page:
                break

            scroll_size = len(page['hits']['hits'])

        self.free_scroll(scroll_id)
        logger.debug("Fetching from {}: done receiving".format(anonymize_url(self.elastic.index_url)))

    def get_elastic_items(self, elastic_scroll_id=None, _filter=None, ignore_incremental=False):
        """Get the items from the index related to the backend applying and
        optional _filter if provided

        :param elastic_scroll_id: If not None, it allows to continue scrolling the data
        :param _filter: if not None, it allows to define a terms filter (e.g., "uuid": ["hash1", "hash2, ...]
        :param ignore_incremental: if True, incremental collection is ignored
        """
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

            filters_spaces_dict = self.get_confluence_spaces(self.repo_spaces)
            if filters_spaces_dict:
                filters_spaces = json.dumps(filters_spaces_dict)
                filters += '''
                    , {"bool":%s}
                ''' % (filters_spaces)

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

            # Fix the filters string if it starts with "," (empty first filter)
            filters = filters.lstrip()[1:] if filters.lstrip().startswith(',') else filters

            query = """
            {
                "query": {
                    "bool": {
                        "filter": [%s]
                    }
                } %s
            }
            """ % (filters, order_query)

            logger.debug("Raw query to {}\n{}".format(anonymize_url(url),
                         json.dumps(json.loads(query), indent=4)))
            query_data = query

        rjson = None
        try:
            res = self.requests.post(url, data=query_data, headers=headers)
            if self.too_many_scrolls(res):
                return {'too_many_scrolls': True}
            res.raise_for_status()
            rjson = json.loads(json.dumps(res.json(), ensure_ascii=False)
                               .encode('utf-8', errors='ignore').decode('utf-8'))

        except Exception:
            # The index could not exists yet or it could be empty
            logger.debug("No results found from {}".format(anonymize_url(url)))

        return rjson

    def too_many_scrolls(self, res):
        """Check if result conatins 'too many scroll contexts' error"""
        r = res.json()
        return (
            r
            and 'status' in r
            and 'error' in r
            and r['status'] == 500
            and 'root_cause' in r['error']
            and len(r['error']['root_cause']) > 0
            and 'reason' in r['error']['root_cause'][0]
            and 'Trying to create too many scroll contexts' in r['error']['root_cause'][0]['reason']
        )
