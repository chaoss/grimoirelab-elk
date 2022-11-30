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

import json
import logging
import sys
from time import time

import requests

from grimoirelab_toolkit.datetime import (str_to_datetime,
                                          unixtime_to_datetime,
                                          InvalidDateError)

from grimoire_elk.errors import ELKError, ElasticError
from grimoire_elk.enriched.utils import (grimoire_con,
                                         get_diff_current_date,
                                         anonymize_url)

logger = logging.getLogger(__name__)

HEADER_JSON = {"Content-Type": "application/json"}


class ElasticSearch(object):

    max_items_bulk = 1000
    max_items_clause = 1000  # max items in search clause (refresh identities)

    def __init__(self, url, index, mappings=None, clean=False,
                 insecure=True, analyzers=None, aliases=None):
        """Class to handle the operations with the ElasticSearch database, such as
        creating indexes, mappings, setting up aliases and uploading documents.

        :param url: ES url
        :param index: index name
        :param mappings: an instance of the Mapping class
        :param clean: if True, deletes an existing index and create it again
        :param insecure: support https with invalid certificates
        :param analyzers: analyzers for ElasticSearch
        :param aliases: list of aliases, defined as strings, to be added to the index
        """
        # Get major version of Elasticsearch instance
        major, distribution = self.check_instance(url, insecure)
        self.major = major
        self.distribution = distribution
        logger.debug("Found version of {} instance at {}: {}.".format(
                     self.distribution, anonymize_url(url), self.major))

        self.url = url

        # Valid index for elastic
        self.index = self.safe_index(index)
        self.aliases = aliases

        self.index_url = self.url + "/" + self.index
        self.wait_bulk_seconds = 2  # time to wait to complete a bulk operation

        self.requests = grimoire_con(insecure)

        analyzer_settings = None

        if analyzers:
            analyzers_dict = analyzers.get_elastic_analyzers(es_major=self.major)
            analyzer_settings = analyzers_dict['items']

        self.create_index(analyzer_settings, clean)

        if analyzers:
            self.update_analyzers(analyzer_settings)
        if mappings:
            map_dict = mappings.get_elastic_mappings(es_major=self.major)
            self.create_mappings(map_dict)

        if aliases:
            for alias in aliases:
                if self.alias_in_use(alias):
                    logger.debug("Alias {} won't be set on {}, it already exists on {}".format(
                                 alias, anonymize_url(self.index_url), anonymize_url(self.url)))
                    continue

                self.add_alias(alias)

    @classmethod
    def safe_index(cls, unique_id):
        """Return a valid elastic index generated from unique_id

        :param unique_id: index name
        """

        index = unique_id
        if unique_id:
            index = unique_id.replace("/", "_").lower()
        return index

    @staticmethod
    def check_instance(url, insecure):
        """Checks if there is an instance of ElasticSearch/OpenSearch in url.

        Actually, it checks if GET on the url returns a JSON document
        with a field tagline "You know, for search",
        and a field version.number.

        :value      url: url of the instance to check
        :value insecure: don't verify ssl connection (boolean)

        :returns:        major version, as str and the distribution name.
        """
        res = grimoire_con(insecure).get(url)
        if res.status_code != 200:
            msg = "Got {} from url {}".format(res.status_code, url)
            logger.error(msg)
            raise ElasticError(cause=msg)
        else:
            try:
                version_str = res.json()['version']['number']
                version_major = version_str.split('.')[0]
                distribution = res.json()['version'].get('distribution', 'elasticsearch')
                return version_major, distribution
            except Exception:
                msg = "Could not read proper welcome message from url {}, {}".format(
                    anonymize_url(url),
                    res.text
                )
                logger.error(msg)
                raise ElasticError(cause=msg)

    def create_index(self, analyzers=None, clean=False):
        """Create an index. If clean is `True`, the target index will be deleted and recreated.

        :param analyzers: set index analyzers
        :param clean: if True, the index is deleted and recreated
        """
        res = self.requests.get(self.index_url)

        headers = {"Content-Type": "application/json"}
        if res.status_code != 200:
            # Index does no exists
            res = self.requests.put(self.index_url, data=analyzers,
                                    headers=headers)
            if res.status_code != 200:
                msg = "Can't create index {} ({})".format(anonymize_url(self.index_url), res.status_code)
                logger.error(msg)
                raise ElasticError(cause=msg)
            else:
                logger.info("Created index {}".format(anonymize_url(self.index_url)))
        else:
            if clean:
                res = self.requests.delete(self.index_url)
                res.raise_for_status()
                res = self.requests.put(self.index_url, data=analyzers,
                                        headers=headers)
                res.raise_for_status()
                logger.info("Deleted and created index {}".format(anonymize_url(self.index_url)))

    def safe_put_bulk(self, url, bulk_json):
        """Bulk items to a target index `url`. In case of UnicodeEncodeError,
        the bulk is encoded with iso-8859-1.

        :param url: target index where to bulk the items
        :param bulk_json: str representation of the items to upload
        """
        headers = {"Content-Type": "application/x-ndjson"}

        try:
            res = self.requests.put(url + '?refresh=true', data=bulk_json, headers=headers)
            res.raise_for_status()
        except UnicodeEncodeError:
            # Related to body.encode('iso-8859-1'). mbox data
            logger.warning("Encondig error ... converting bulk to iso-8859-1")
            bulk_json = bulk_json.encode('iso-8859-1', 'ignore')
            res = self.requests.put(url, data=bulk_json, headers=headers)
            res.raise_for_status()

        result = res.json()
        failed_items = []
        error = ""
        if result['errors']:
            # Due to multiple errors that may be thrown when inserting bulk data, only the first error is returned
            failed_items = [item['index'] for item in result['items'] if 'error' in item['index']]
            error = str(failed_items[0]['error'])

            logger.error("Failed to insert data to ES: {}, {}".format(error, anonymize_url(url)))

        inserted_items = len(result['items']) - len(failed_items)

        # The exception is currently not thrown to avoid stopping ocean uploading processes
        try:
            if failed_items:
                raise ELKError(cause=error)
        except ELKError:
            pass

        logger.debug("{} items uploaded to ES ({})".format(inserted_items, anonymize_url(url)))
        return inserted_items

    def all_es_aliases(self):
        """List all aliases used in ES"""

        r = self.requests.get(self.url + "/_aliases", headers=HEADER_JSON, verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            logger.warning("Something went wrong when retrieving aliases on {}, {}".format(
                           anonymize_url(self.index_url), ex))
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
            logger.warning("Something went wrong when retrieving aliases on {}, {}".format(
                           anonymize_url(self.index_url), ex))
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
        alias_dict = alias
        if isinstance(alias, str):
            alias_dict = {
                "alias": alias
            }

        if aliases and alias_dict['alias'] in aliases:
            logger.debug("Alias {} already exists on {}.".format(
                alias_dict['alias'],
                anonymize_url(self.index_url)
            ))
            return

        # add alias
        alias_dict['index'] = self.index
        alias_action = {
            "actions": [
                {
                    "add": alias_dict
                }
            ]
        }

        r = self.requests.post(self.url + "/_aliases", headers=HEADER_JSON, verify=False, data=json.dumps(alias_action))
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            logger.warning("Something went wrong when adding an alias on {}. Alias not set. {}".format(
                           anonymize_url(self.index_url), ex))
            return

        logger.info("Alias {} created on {}.".format(alias, anonymize_url(self.index_url)))

    def get_bulk_url(self):
        """Get the bulk URL endpoint"""

        if not self.is_legacy():
            bulk_url = self.index_url + '/_bulk'
        else:
            bulk_url = self.index_url + '/items/_bulk'

        return bulk_url

    def get_mapping_url(self, _type=None):
        """Get the mapping URL endpoint

        :param _type: type of the mapping. In case of ES >= 7, it is None
        """
        if not self.is_legacy():
            mapping_url = self.index_url + "/_mapping"
        else:
            mapping_url = self.index_url + "/" + _type + "/_mapping"

        return mapping_url

    def bulk_upload(self, items, field_id):
        """Upload in controlled packs items to ES using bulk API

        :param items: list of items to be uploaded
        :param field_id: unique ID attribute used to differentiate the items
        """
        current = 0
        new_items = 0  # total items added with bulk
        bulk_json = ""

        if not items:
            return new_items

        url = self.get_bulk_url()

        logger.debug("Adding items to {} (in {} packs)".format(anonymize_url(url), self.max_items_bulk))
        task_init = time()

        for item in items:
            if current >= self.max_items_bulk:
                task_init = time()
                new_items += self.safe_put_bulk(url, bulk_json)
                current = 0
                json_size = sys.getsizeof(bulk_json) / (1024 * 1024)
                logger.debug("bulk packet sent ({:.2f} sec, {} total, {:.2f} MB)".format(
                             time() - task_init, new_items, json_size))
                bulk_json = ""
            data_json = json.dumps(item)
            bulk_json += '{{"index" : {{"_id" : "{}" }} }}\n'.format(item[field_id])
            bulk_json += data_json + "\n"  # Bulk document
            current += 1

        if current > 0:
            new_items += self.safe_put_bulk(url, bulk_json)
            json_size = sys.getsizeof(bulk_json) / (1024 * 1024)
            logger.debug("bulk packet sent ({:.2f} sec prev, {} total, {:.2f} MB)".format(
                         time() - task_init, new_items, json_size))

        return new_items

    def update_analyzers(self, analyzers):
        """Update the settings with the analyzer for a given index.
        To update the settings we have to:
        1. Close the index
        2. Update the settings
        3. Open the index.

        :param analyzers: elastic_analyzer.Analyzer object
        """
        if analyzers == '{}':
            return

        headers = {"Content-Type": "application/json"}

        # Check if the settings are already updated
        res = self.requests.get(self.index_url + "/_settings")
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError:
            logger.error("Error getting the index settings: {}".format(res.text))

        settings = res.json()
        index_settings = settings[self.index]['settings']['index']
        analysis = json.loads(analyzers)['settings']['analysis']
        if 'analysis' in index_settings and analysis == index_settings['analysis']:
            logger.debug("Index settings for {} is already updated. No need to update it".format(self.index))
            return

        close_index_url = "{}/_close".format(self.index_url)
        res = self.requests.post(close_index_url, headers=headers)
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError:
            logger.error("Error closing the index before updating settings: {}".format(res.text))

        url_set = "{}/_settings".format(self.index_url)
        res = self.requests.put(url_set, data=analyzers,
                                headers=headers)
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError:
            logger.error("Error updating index settings {}. Settings: {}".format(res.text, analyzers))

        open_index_url = "{}/_open".format(self.index_url)
        res = self.requests.post(open_index_url, headers=headers)
        try:
            res.raise_for_status()
            logger.debug("Index settings updated {}: {}".format(self.index, analyzers))
        except requests.exceptions.HTTPError:
            logger.error("Error opening the index after updating settings: {}".format(res.text))

    def create_mappings(self, mappings):
        """Create the mappings for a given index. It includes the index
        pattern plus dynamic templates.

        :param mappings: elastic_mapping.Mapping object
        """
        headers = {"Content-Type": "application/json"}

        for _type in mappings:

            url_map = self.get_mapping_url(_type)

            # First create the manual mappings
            if mappings[_type] != '{}':
                res = self.requests.put(url_map, data=mappings[_type],
                                        headers=headers)
                try:
                    res.raise_for_status()
                except requests.exceptions.HTTPError:
                    logger.error("Error creating ES mappings {}. Mapping: {}".format(res.text, str(mappings[_type])))

            # After version 6, strings are keywords (not analyzed)
            not_analyze_strings = """
            {
              "dynamic_templates": [
                { "notanalyzed": {
                      "match": "*",
                      "match_mapping_type": "string",
                      "mapping": {
                          "type": "keyword"
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
                logger.error("Can't add mapping {}: {}".format(anonymize_url(url_map), not_analyze_strings))

    def get_last_date(self, field, filters_=[]):
        """Find the date of the last item stored in the index

        :param field: field with the data
        :param filters_: additional filters to find the date
        """
        last_date = self.get_last_item_field(field, filters_=filters_)

        return last_date

    def get_last_offset(self, field, filters_=[]):
        """Find the offset of the last item stored in the index

        :param field: field with the data
        :param filters_: additional filters to find the date
        """
        offset = self.get_last_item_field(field, filters_=filters_, offset=True)

        return offset

    def get_last_item_field(self, field, filters_=[], offset=False):
        """Find the offset/date of the last item stored in the index.

        :param field: field with the data
        :param filters_: additional filters to find the date
        :param offset: if True, returns the offset field instead of date field
        """
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

        logger.debug("{} {}".format(anonymize_url(url), data_json))

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
                    last_value = str_to_datetime(last_value)
                else:
                    last_value = res_json["aggregations"]["1"]["value"]
                    if last_value:
                        try:
                            last_value = unixtime_to_datetime(last_value)
                        except InvalidDateError:
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
            logger.debug("[items retention] {} items deleted from {} before {}.".format(
                         r_json['deleted'], anonymize_url(self.index_url), before_date))
        except requests.exceptions.HTTPError as ex:
            logger.error("[items retention] Error deleted items from {}. {}".format(
                         anonymize_url(self.index_url), ex))
            return

    def all_properties(self):
        """Get all properties of a given index"""

        url = self.get_mapping_url(_type='items')
        r = self.requests.get(url, headers=HEADER_JSON, verify=False)
        try:
            r.raise_for_status()
            r_json = r.json()

            # ES 7.x
            properties = r_json[self.index]['mappings'].get('properties', {})

            # ES 6.x
            if not properties:
                items_mapping = r_json[self.index]['mappings'].get('items', {})
                properties = items_mapping.get('properties', {}) if items_mapping else {}

        except requests.exceptions.HTTPError as ex:
            logger.error("Error all attributes for {}. {}".format(anonymize_url(self.index_url), ex))
            return

        return properties

    def is_legacy(self):
        """ Simply calls the static version with it's own values """
        return ElasticSearch.is_legacy_static(self.major, self.distribution)

    @staticmethod
    def is_legacy_static(major, distribution):
        """ Returns true if ES < 7 or OS < 1, false otherwise.
        Static version exists because not every place that uses this check has an ES object."""
        if major is None:
            return False
        int_maj = int(major)
        return ((int_maj < 7 and distribution == 'elasticsearch')
                or (int_maj < 1 and distribution == 'opensearch'))
