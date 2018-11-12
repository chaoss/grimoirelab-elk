# -*- coding: utf-8 -*-
#
# Elastic Search basic utils
#
# Copyright (C) 2016 Bitergia
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
import json
from time import time

from elasticsearch import Elasticsearch, helpers, compat, exceptions
from elasticsearch.exceptions import NotFoundError
from elasticsearch.serializer import JSONSerializer
from elasticsearch.helpers import BulkIndexError

import warnings
warnings.filterwarnings("ignore")

from grimoire_elk.errors import ElasticException
from grimoire_elk.enriched.utils import unixtime_to_datetime


ES_TIMEOUT = 7200
ES_MAX_RETRIES = 50
ES_RETRY_ON_TIMEOUT = True
ES_VERIFY_CERTS = False

ES_SCROLL_SIZE = 100


logger = logging.getLogger(__name__)


DEFAULT_ITEMS_TYPE = 'items'


class JSONSerializerASCII(JSONSerializer):
    """Override elasticsearch library serializer to ensure it encodes utf characters during json dump.
    See original at: https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L42
    A description of how ensure_ascii encodes unicode characters to ensure they can be sent across the wire
    as ascii can be found here: https://docs.python.org/3/library/json.html#basic-usage
    """
    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)


class ElasticSearch:
    """ElasticSearch client to perform operations over the data, such as
    creating and deleting indexes, adding aliases, uploading and scrolling the data

    :param url: URL of ElasticSearch
    :param index: target index
    :param mappings: mappings to upload
    :param clean: if enabled, delete the index
    :param insecure: if enable, it ignores to verify certs
    :param analyzers: analyzers to be included in the mapping
    :param aliases: aliases to be added to index
    """

    max_items_bulk = 1000
    max_items_clause = 1000  # max items in search clause (refresh identities)

    def __init__(self, url, index, mappings=None, clean=False,
                 insecure=True, analyzers=None, aliases=None):

        self.url = url
        self.es_verify_certs = not insecure
        self.es = Elasticsearch([url], timeout=ES_TIMEOUT, max_retries=ES_MAX_RETRIES,
                                retry_on_timeout=ES_RETRY_ON_TIMEOUT,
                                verify_certs=self.es_verify_certs,
                                serializer=JSONSerializerASCII())

        # Get major version of Elasticsearch instance
        self.major = self.major_version_es()
        logger.debug("Found version of ES instance at %s: %s.", self.url, self.major)

        # Valid index for elastic
        self.index = self.safe_index(index)
        self.aliases = aliases
        self.index_url = self.url + "/" + self.index

        self.create_index(mappings, clean, analyzers)

        if aliases:
            [self.add_alias(alias) for alias in aliases]

    def safe_index(self, unique_id):
        """Return a valid elastic index generated from unique_id"""

        if not unique_id:
            msg = "Index cannot be none, %s" % self.url
            logger.error(cause=msg)
            raise ElasticException(cause=msg)

        index = unique_id.replace("/", "_").lower()

        if index != unique_id:
            logger.warning("Index %s not valid, changed to %s", unique_id, index)

        return index

    def major_version_es(self):
        """Get the info of an Elasticsearch DB.

        :returns: major version of Elasticsearch, as string.
        """
        try:
            result = self.es.info()
            version_number = result['version']['number']
            version_major = version_number.split('.')[0]
        except Exception as e:
            logger.error("Could not read ElasticSearch DB info %s", self.url, str(e))
            raise ElasticException(cause="Can't connect to ElasticSearch")

        return version_major

    @staticmethod
    def add_templates(index_mappings):

        templates = [
            {
                "notanalyzed": {
                    "match": "*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword"
                    }
                }
            },
            {
                "formatdate": {
                    "match": "*",
                    "match_mapping_type": "date",
                    "mapping": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_millis"
                    }
                }
            }]

        index_mappings['mappings']['items']['dynamic_templates'] = templates

    def create_index(self, mappings, clean, analyzers):
        if clean and self.exist_index():
            self.delete_index()

        index_mappings = {
            "mappings": {
                "items": {}
            }
        }

        if mappings:
            user_mappings = mappings.get_elastic_mappings(es_major=self.major)
            index_mappings['mappings']['items'] = user_mappings['items']
            self.add_templates(index_mappings)

        if analyzers:
            index_mappings['settings'] = {}
            index_mappings['settings']['analysis'] = analyzers['analysis']

        if not self.exist_index():
            result = self.es.indices.create(index=self.index, body=index_mappings)
            logger.info("Creating index %s: %s!" % (self.index_url, str(result)))

    def delete_index(self):
        result = self.es.indices.delete(index=self.index)
        logger.info("Deleting index %s: %s!" % (self.index_url, result))

    def exist_index(self):
        return self.es.indices.exists(index=self.index)

    def refresh_index(self):
        self.es.indices.refresh(index=self.index)

    def add_alias(self, alias):

        # check alias doesn't exist
        aliases = self.get_aliases()

        if alias in aliases:
            logger.warning("Alias %s already exists on %s.", alias, self.index_url)
            return

        if 'filter' in alias and 'alias' in alias:
            add = {
                "add": {
                    "index": self.index,
                    "alias": alias['alias'],
                    "filter": alias['filter']
                }
            }
        else:
            add = {
                "add": {
                    "index": self.index,
                    "alias": alias
                }
            }

        result = self.es.indices.update_aliases({
            "actions": [add]
        })

        logger.info("Alias %s created on %s.", alias, self.index_url)

    def count_docs(self, query=None):
        if not query:
            query = {
                "query": {
                    "match_all": {}
                }
            }

        result = self.es.count(index=self.index, body=query)
        return result['count']

    def get_aliases(self):
        aliases = []

        try:
            result = self.es.indices.get_alias(self.index)
            for alias in result[self.index]['aliases']:
                aliases.append(alias)
        except NotFoundError as e:
            logger.error('Error on retrieving aliases on %s, %s', self.index_url, str(e))
            raise e

        return aliases

    def es_data_format(self, item, field_id, event_id=None, items_type=DEFAULT_ITEMS_TYPE):
        id = item[field_id] + '_' + item[event_id] if event_id else item[field_id]

        return {"_index": self.index,
                "_id": id,
                "_source": item,
                "_type": items_type}

    def generate_es_data(self, items, field_id, event_id, items_type):
        for item in items:
            yield self.es_data_format(item, field_id, event_id, items_type)

    def bulk_upload(self, items, field_id, event_id=None, items_type=DEFAULT_ITEMS_TYPE):
        """Format items and upload them to ES"""

        es_data = self.generate_es_data(items, field_id, event_id, items_type)
        items_inserted = self.bulk(es_data)
        return items_inserted

    def bulk(self, items):
        """Bulk data to ES"""

        items_inserted = 0  # total items added with bulk

        if not items:
            return items_inserted

        logger.debug("Start adding items to %s (in %i packs)" % (self.index_url, self.max_items_bulk))
        start_time = time()
        try:
            result = helpers.bulk(self.es, items, chunk_size=self.max_items_bulk)
        except BulkIndexError as e:
            logger.error(str(e))
            raise e

        self.refresh_index()
        end_time = time()

        items_inserted = result[0]
        info = result[1]

        logger.debug("End adding items to %s, it took %s sec" % (self.index_url, end_time - start_time))

        if info:
            logger.error("Bulk to %s info %s" % (self.index_url, str(info)))

        return items_inserted

    def aggregations(self, query, scroll="10m", size=ES_SCROLL_SIZE):
        logger.debug("Query aggregation %s on %s", str(query), self.index_url)
        page = self.es.search(
            index=self.index,
            scroll=scroll,
            size=size,
            body=query
        )

        return page

    def search(self, scroll="10m", size=ES_SCROLL_SIZE, query=None):
        """Search data in ES"""

        total = 0

        if not query:
            query = {"query": {"match_all": {}}}

        page = self.es.search(
            index=self.index,
            scroll=scroll,
            size=size,
            body=query
        )

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']

        if scroll_size == 0:
            logger.warning("No data found!")
            return

        while scroll_size > 0:

            logger.debug("Searching on %s: %d items received", self.index_url, len(page['hits']['hits']))
            total += len(page['hits']['hits'])
            for item in page['hits']['hits']:
                eitem = item['_source']
                yield eitem

            page = self.es.scroll(scroll_id=sid, scroll='1m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])

        logger.debug("Searching from %s: done, total items received %s", self.index_url, total)

    def update_by_query(self, query):
        result = self.es.update_by_query(body=query, doc_type=DEFAULT_ITEMS_TYPE, index=self.index)

        if result['failures']:
            logger.error("Errors when updating %s, %s", self.index_url, str(result['failures']))

        logger.info("Items updated %s/%s on %s", result['updated'], result['total'], self.index_url)

    def get_last_date(self, field, filters_=[]):
        """Get date of last item inserted

        :param field: field with the data
        :param filters_: additional filters to find the date
        """

        last_value = None

        data_query = ''
        if filters_ is None:
            filters_ = []
        for filter_ in filters_:
            if not filter_:
                continue
            data_query += """
                        "query" : {
                            "term" : { "%s" : "%s"  }
                         },
                    """ % (filter_['name'], filter_['value'])

        data_agg = """
                    "aggs": {
                        "1": {
                          "max": {
                            "field": "%s"
                          }
                        }
                    }
                """ % (field)

        data_json = """
                { %s  %s
                } """ % (data_query, data_agg)

        logger.debug("Get last item date on %s, %s", self.index_url, data_json)
        result = self.search(query=data_json, size=0)

        if 'aggregations' in result:
            if "value_as_string" in result["aggregations"]["1"]:
                last_value = result["aggregations"]["1"]["value_as_string"]
                last_value = parser.parse(last_value)
            else:
                last_value = result["aggregations"]["1"]["value"]
                if last_value:
                    try:
                        last_value = unixtime_to_datetime(last_value)
                    except ValueError:
                        # last_value is in microsecs
                        last_value = unixtime_to_datetime(last_value / 1000)

        return last_value
