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

from dateutil import parser
import json
import logging
import re
import sys
from time import time

import requests

from grimoire_elk.errors import ELKError
from grimoire_elk.enriched.utils import (unixtime_to_datetime,
                                         grimoire_con,
                                         get_diff_current_date)

logger = logging.getLogger(__name__)

HEADER_JSON = {"Content-Type": "application/json"}


class ElasticConnectException(Exception):
    message = "Can't connect to ElasticSearch"


class ElasticWriteException(Exception):
    message = "Can't write to ElasticSearch"


class ElasticSearch(object):

    max_items_bulk = 1000
    max_items_clause = 1000  # max items in search clause (refresh identities)

    def __init__(self, url, index, mappings=None, clean=False,
                 insecure=True, analyzers=None, aliases=None):
        ''' clean: remove already existing index
            insecure: support https with invalid certificates
        '''

        # Get major version of Elasticsearch instance
        self.major = self._check_instance(url, insecure)
        logger.debug("Found version of ES instance at %s: %s.",
                     self.anonymize_url(url), self.major)

        self.url = url

        # Valid index for elastic
        self.index = self.safe_index(index)
        self.aliases = aliases

        self.index_url = self.url + "/" + self.index
        self.wait_bulk_seconds = 2  # time to wait to complete a bulk operation

        self.requests = grimoire_con(insecure)

        res = self.requests.get(self.index_url)

        headers = {"Content-Type": "application/json"}
        if res.status_code != 200:
            # Index does no exists
            r = self.requests.put(self.index_url, data=analyzers,
                                  headers=headers)
            if r.status_code != 200:
                logger.error("Can't create index %s (%s)",
                             self.anonymize_url(self.index_url), r.status_code)
                raise ElasticWriteException()
            else:
                logger.info("Created index " + self.anonymize_url(self.index_url))
        else:
            if clean:
                res = self.requests.delete(self.index_url)
                res.raise_for_status()
                res = self.requests.put(self.index_url, data=analyzers,
                                        headers=headers)
                res.raise_for_status()
                logger.info("Deleted and created index " + self.anonymize_url(self.index_url))
        if mappings:
            map_dict = mappings.get_elastic_mappings(es_major=self.major)
            self.create_mappings(map_dict)

        if aliases:
            for alias in aliases:
                if self.alias_in_use(alias):
                    logger.debug("Alias %s won't be set on %s, it already exists on %s",
                                 alias, self.anonymize_url(self.index_url), self.anonymize_url(self.url))
                    continue

                self.add_alias(alias)

    @classmethod
    def safe_index(cls, unique_id):
        """ Return a valid elastic index generated from unique_id """
        index = unique_id
        if unique_id:
            index = unique_id.replace("/", "_").lower()
        return index

    @staticmethod
    def _check_instance(url, insecure):
        """Checks if there is an instance of Elasticsearch in url.

        Actually, it checks if GET on the url returns a JSON document
        with a field tagline "You know, for search",
        and a field version.number.

        :value      url: url of the instance to check
        :value insecure: don't verify ssl connection (boolean)
        :returns:        major version of Ellasticsearch, as string.
        """

        res = grimoire_con(insecure).get(url)
        if res.status_code != 200:
            logger.error("Didn't get 200 OK from url %s", url)
            raise ElasticConnectException
        else:
            try:
                version_str = res.json()['version']['number']
                version_major = version_str.split('.')[0]
                return version_major
            except Exception:
                logger.error("Could not read proper welcome message from url %s",
                             ElasticSearch.anonymize_url(url))
                logger.error("Message read: %s", res.text)
                raise ElasticConnectException

    @staticmethod
    def anonymize_url(url):
        anonymized = re.sub('^http.*@', 'http://', url)

        return anonymized

    def safe_put_bulk(self, url, bulk_json):
        """ Bulk PUT controlling unicode issues """

        headers = {"Content-Type": "application/x-ndjson"}

        try:
            res = self.requests.put(url + '?refresh=true', data=bulk_json, headers=headers)
            res.raise_for_status()
        except UnicodeEncodeError:
            # Related to body.encode('iso-8859-1'). mbox data
            logger.error("Encondig error ... converting bulk to iso-8859-1")
            bulk_json = bulk_json.encode('iso-8859-1', 'ignore')
            res = self.requests.put(url, data=bulk_json, headers=headers)
            res.raise_for_status()

        result = res.json()
        failed_items = []
        if result['errors']:
            # Due to multiple errors that may be thrown when inserting bulk data, only the first error is returned
            failed_items = [item['index'] for item in result['items'] if 'error' in item['index']]
            error = str(failed_items[0]['error'])

            logger.error("Failed to insert data to ES: %s, %s", error, self.anonymize_url(url))

        inserted_items = len(result['items']) - len(failed_items)

        # The exception is currently not thrown to avoid stopping ocean uploading processes
        try:
            if failed_items:
                raise ELKError(cause=error)
        except ELKError:
            pass

        logger.debug("%i items uploaded to ES (%s)", inserted_items, self.anonymize_url(url))
        return inserted_items

    def all_es_aliases(self):
        """List all aliases used in ES"""

        r = self.requests.get(self.url + "/_aliases", headers=HEADER_JSON, verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            logger.warning("Something went wrong when retrieving aliases on %s.",
                           self.anonymize_url(self.index_url))
            logger.warning(ex)
            return

        aliases = []
        for index in r.json().keys():
            aliases.extend(list(r.json()[index]['aliases'].keys()))

        aliases = list(set(aliases))
        return aliases

    def list_aliases(self):
        """List aliases linked to the index"""

        # check alias doesn't exist
        r = self.requests.get(self.index_url + "/_alias", headers=HEADER_JSON, verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            logger.warning("Something went wrong when retrieving aliases on %s.",
                           self.anonymize_url(self.index_url))
            logger.warning(ex)
            return

        aliases = r.json()[self.index]['aliases']
        return aliases

    def alias_in_use(self, alias):
        """Check that an alias is already used in the ElasticSearch database

        :param alias: target alias
        :return: bool
        """
        aliases = self.all_es_aliases()
        return alias in aliases

    def add_alias(self, alias):
        """Add an alias to the index set in the elastic obj

        :param alias: alias to add

        :returns: None
        """
        aliases = self.list_aliases()
        if alias in aliases:
            logger.debug("Alias %s already exists on %s.", alias, self.anonymize_url(self.index_url))
            return

        # add alias
        alias_data = """
        {
            "actions": [
                {
                    "add": {
                        "index": "%s",
                        "alias": "%s"
                    }
                }
            ]
        }
        """ % (self.index, alias)

        r = self.requests.post(self.url + "/_aliases", headers=HEADER_JSON, verify=False, data=alias_data)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            logger.warning("Something went wrong when adding an alias on %s. Alias not set.",
                           self.anonymize_url(self.index_url))
            logger.warning(ex)
            return

        logger.info("Alias %s created on %s.", alias, self.anonymize_url(self.index_url))

    def bulk_upload(self, items, field_id):
        """Upload in controlled packs items to ES using bulk API"""

        current = 0
        new_items = 0  # total items added with bulk
        bulk_json = ""

        if not items:
            return new_items

        url = self.index_url + '/items/_bulk'

        logger.debug("Adding items to %s (in %i packs)", self.anonymize_url(url), self.max_items_bulk)
        task_init = time()

        for item in items:
            if current >= self.max_items_bulk:
                task_init = time()
                new_items += self.safe_put_bulk(url, bulk_json)
                current = 0
                json_size = sys.getsizeof(bulk_json) / (1024 * 1024)
                logger.debug("bulk packet sent (%.2f sec, %i total, %.2f MB)"
                             % (time() - task_init, new_items, json_size))
                bulk_json = ""
            data_json = json.dumps(item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (item[field_id])
            bulk_json += data_json + "\n"  # Bulk document
            current += 1

        if current > 0:
            new_items += self.safe_put_bulk(url, bulk_json)
            json_size = sys.getsizeof(bulk_json) / (1024 * 1024)
            logger.debug("bulk packet sent (%.2f sec prev, %i total, %.2f MB)"
                         % (time() - task_init, new_items, json_size))

        return new_items

    def create_mappings(self, mappings):

        headers = {"Content-Type": "application/json"}

        for _type in mappings:

            url_map = self.index_url + "/" + _type + "/_mapping"

            # First create the manual mappings
            if mappings[_type] != '{}':
                res = self.requests.put(url_map, data=mappings[_type],
                                        headers=headers)
                if res.status_code != 200:
                    logger.error("Error creating ES mappings %s", res.text)
                    logger.error("Mapping: " + str(mappings[_type]))
                    res.raise_for_status()

            # By default all strings are not analyzed in ES < 6
            if self.major == '2' or self.major == '5':
                # Before version 6, strings were strings
                not_analyze_strings = """
                {
                  "dynamic_templates": [
                    { "notanalyzed": {
                          "match": "*",
                          "match_mapping_type": "string",
                          "mapping": {
                              "type":        "string",
                              "index":       "not_analyzed"
                          }
                       }
                    },
                    { "formatdate": {
                          "match": "*",
                          "match_mapping_type": "date",
                          "mapping": {
                              "type": "date",
                              "format" : "strict_date_optional_time||epoch_millis"
                          }
                       }
                    }
                  ]
                } """
            else:
                # After version 6, strings are keywords (not analyzed)
                not_analyze_strings = """
                {
                  "dynamic_templates": [
                    { "notanalyzed": {
                          "match": "*",
                          "match_mapping_type": "string",
                          "mapping": {
                              "type":        "keyword"
                          }
                       }
                    },
                    { "formatdate": {
                          "match": "*",
                          "match_mapping_type": "date",
                          "mapping": {
                              "type": "date",
                              "format" : "strict_date_optional_time||epoch_millis"
                          }
                       }
                    }
                  ]
                } """
            res = self.requests.put(url_map, data=not_analyze_strings, headers=headers)
            try:
                res.raise_for_status()
            except requests.exceptions.HTTPError:
                logger.warning("Can't add mapping %s: %s", self.anonymize_url(url_map), not_analyze_strings)

    def get_last_date(self, field, filters_=[]):
        '''
            :field: field with the data
            :filters_: additional filters to find the date
        '''

        last_date = self.get_last_item_field(field, filters_=filters_)

        return last_date

    def get_last_offset(self, field, filters_=[]):
        '''
            :field: field with the data
            :filters_: additional filters to find the date
        '''

        offset = self.get_last_item_field(field, filters_=filters_, offset=True)

        return offset

    def get_last_item_field(self, field, filters_=[], offset=False):
        '''
            :field: field with the data
            :filters_: additional filters to find the date
            :offset: Return offset field insted of date field
        '''

        last_value = None

        url = self.index_url
        url += "/_search"

        if filters_ is None:
            filters_ = []

        terms = []
        for filter_ in filters_:
            if not filter_:
                continue
            term = '''{"term" : { "%s" : "%s"}}''' % (filter_['name'], filter_['value'])
            terms.append(term)

        data_query = '''"query": {"bool": {"filter": [%s]}},''' % (','.join(terms))

        data_agg = '''
            "aggs": {
                "1": {
                  "max": {
                    "field": "%s"
                  }
                }
            }
        ''' % field

        data_json = '''
        { "size": 0, %s  %s
        } ''' % (data_query, data_agg)

        logger.debug("%s %s", self.anonymize_url(url), data_json)

        headers = {"Content-Type": "application/json"}

        res = self.requests.post(url, data=data_json, headers=headers)
        res.raise_for_status()
        res_json = res.json()

        if 'aggregations' in res_json:
            last_value = res_json["aggregations"]["1"]["value"]

            if offset:
                if last_value is not None:
                    last_value = int(last_value)
            else:
                if "value_as_string" in res_json["aggregations"]["1"]:
                    last_value = res_json["aggregations"]["1"]["value_as_string"]
                    last_value = parser.parse(last_value)
                else:
                    last_value = res_json["aggregations"]["1"]["value"]
                    if last_value:
                        try:
                            last_value = unixtime_to_datetime(last_value)
                        except ValueError:
                            # last_value is in microsecs
                            last_value = unixtime_to_datetime(last_value / 1000)
        return last_value

    def delete_items(self, retention_time, time_field="metadata__updated_on"):
        """Delete documents updated before a given date

        :param retention_time: maximum number of minutes wrt the current date to retain the data
        :param time_field: time field to delete the data
        """
        if retention_time is None:
            logger.debug("[items retention] Retention policy disabled, no items will be deleted.")
            return

        if retention_time <= 0:
            logger.debug("[items retention] Minutes to retain must be greater than 0.")
            return

        before_date = get_diff_current_date(minutes=retention_time)
        before_date_str = before_date.isoformat()

        es_query = '''
                    {
                      "query": {
                        "range": {
                            "%s": {
                                "lte": "%s"
                            }
                        }
                      }
                    }
                    ''' % (time_field, before_date_str)

        r = self.requests.post(self.index_url + "/_delete_by_query?refresh",
                               data=es_query, headers=HEADER_JSON, verify=False)
        try:
            r.raise_for_status()
            r_json = r.json()
            logger.debug("[items retention] %s items deleted from %s before %s.",
                         r_json['deleted'], self.anonymize_url(self.index_url), before_date)
        except requests.exceptions.HTTPError as ex:
            logger.error("[items retention] Error deleted items from %s.", self.anonymize_url(self.index_url))
            logger.error(ex)
            return

    def all_properties(self):
        """Get all properties of a given index"""

        properties = {}
        r = self.requests.get(self.index_url + "/_mapping", headers=HEADER_JSON, verify=False)
        try:
            r.raise_for_status()
            r_json = r.json()

            if 'items' not in r_json[self.index]['mappings']:
                return properties

            if 'properties' not in r_json[self.index]['mappings']['items']:
                return properties

            properties = r_json[self.index]['mappings']['items']['properties']
        except requests.exceptions.HTTPError as ex:
            logger.error("Error all attributes for %s.", self.anonymize_url(self.index_url))
            logger.error(ex)
            return

        return properties
