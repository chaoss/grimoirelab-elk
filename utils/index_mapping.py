#!/usr/bin/env python3
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import argparse
import json
import logging
import sys

import requests

from grimoire_elk.elastic import ElasticSearch
from grimoire_elk.utils import get_connectors, get_connector_name_from_cls_name

DEFAULT_LIMIT = 1000


def get_params():
    parser = argparse.ArgumentParser(usage="usage:index_mapping [options]",
                                     description="Ensure correct mappings in GrimoireLab indexes")
    parser.add_argument("-e", "--elastic-url", required=True, help="Elasticsearch URL for read/write")
    parser.add_argument("--elastic_url-write", help="Elasticsearch URL for write")
    parser.add_argument("-i", "--in-index", required=True,
                        help="ElasticSearch index from which to import items")
    parser.add_argument("-o", "--out-index", required=True,
                        help="ElasticSearch index in which to export the "
                             "items using a GrimoireLab mapping")
    parser.add_argument('-g', '--debug', dest='debug', action='store_true')
    parser.add_argument('-l', '--limit', dest='limit', default=DEFAULT_LIMIT, type=int,
                        help='Number of items to collect (default 100)')
    parser.add_argument('--search-after', dest='search_after', action='store_true',
                        help='Use search-after for scrolling the index')
    parser.add_argument('--search-after-init', dest='search_after_value',
                        nargs='+',
                        help='Initial value for starting the search-after')
    parser.add_argument('-c', '--copy', dest='copy', action='store_true',
                        help='Copy the indexes without modifying mappings')
    args = parser.parse_args()

    return args


def find_uuid(es_url, index):
    """ Find the unique identifier field for a given index """

    uid_field = None

    # Get the first item to detect the data source and raw/enriched type
    res = requests.get('%s/%s/_search?size=1' % (es_url, index))
    first_item = res.json()['hits']['hits'][0]['_source']
    fields = first_item.keys()

    if 'uuid' in fields:
        uid_field = 'uuid'
    else:
        # Non perceval backend
        uuid_value = res.json()['hits']['hits'][0]['_id']
        logging.debug("Finding unique id for %s with value %s", index, uuid_value)
        for field in fields:
            if first_item[field] == uuid_value:
                logging.debug("Found unique id for %s: %s", index, field)
                uid_field = field
                break

    if not uid_field:
        logging.error("Can not find uid field for %s. Can not copy the index.", index)
        logging.error("Try to copy it directly with elasticdump or similar.")
        sys.exit(1)

    return uid_field


def find_perceval_backend(es_url, index):

    backend = None

    # Backend connectors
    connectors = get_connectors()

    # Get the first item to detect the data source and raw/enriched type
    res = requests.get('%s/%s/_search?size=1' % (es_url, index))
    first_item = res.json()['hits']['hits'][0]['_source']
    fields = first_item.keys()
    if 'metadata__enriched_on' in fields:
        enrich_class = first_item['metadata__gelk_backend_name']
        logging.debug("Detected enriched index for %s", enrich_class)
        # Time to get the mapping
        con_name = get_connector_name_from_cls_name(enrich_class)
        logging.debug("Getting the mapping for %s", con_name)
        klass = connectors[con_name][2]
        backend = klass()
    elif 'perceval_version' in fields:
        logging.debug("Detected raw index for %s", first_item['backend_name'])
        con_name = get_connector_name_from_cls_name(first_item['backend_name'])
        klass = connectors[con_name][1]
        backend = klass(None)
    elif 'retweet_count' in fields:
        con_name = 'twitter'
        logging.debug("Detected raw index for %s", con_name)
    elif 'type' in fields and first_item['type'] == 'googleSearchHits':
        logging.debug("Detected raw index for googleSearchHits")
    elif 'httpversion' in fields:
        logging.debug("Detected raw index for apache")
    else:
        logging.error("Can not find is the index if raw or enriched: %s", index)
        sys.exit(1)

    return backend


def find_mapping(es_url, index):
    """ Find the mapping given an index """

    mapping = None

    backend = find_perceval_backend(es_url, index)

    if backend:
        mapping = backend.get_elastic_mappings()

    if mapping:
        logging.debug("MAPPING FOUND:\n%s", json.dumps(json.loads(mapping['items']), indent=True))
    return mapping


def get_elastic_items(elastic, elastic_scroll_id=None, limit=None):
    """ Get the items from the index """

    scroll_size = limit
    if not limit:
        scroll_size = DEFAULT_LIMIT

    if not elastic:
        return None

    url = elastic.index_url
    max_process_items_pack_time = "5m"  # 10 minutes
    url += "/_search?scroll=%s&size=%i" % (max_process_items_pack_time,
                                           scroll_size)

    if elastic_scroll_id:
        # Just continue with the scrolling
        url = elastic.url
        url += "/_search/scroll"
        scroll_data = {
            "scroll": max_process_items_pack_time,
            "scroll_id": elastic_scroll_id
        }
        res = requests.post(url, data=json.dumps(scroll_data))
    else:
        query = """
        {
            "query": {
                "bool": {
                    "must": []
                }
            }
        }
        """

        logging.debug("%s\n%s", url, json.dumps(json.loads(query), indent=4))
        res = requests.post(url, data=query)

    rjson = None
    try:
        rjson = res.json()
    except Exception:
        logging.error("No JSON found in %s", res.text)
        logging.error("No results found from %s", url)

    return rjson


def extract_mapping(elastic_url, in_index):

    mappings = None

    url = elastic_url + "/_mapping"

    res = requests.get(url)
    res.raise_for_status()

    rjson = None
    try:
        rjson = res.json()
    except Exception:
        logging.error("No JSON found in %s", res.text)
        logging.error("No results found from %s", url)

    mappings = rjson[in_index]['mappings']

    mappings['items'] = json.dumps(mappings['items'])

    return mappings


def get_elastic_items_search(elastic, search_after=None, size=None):
    """ Get the items from the index using search after scrolling """

    if not size:
        size = DEFAULT_LIMIT

    url = elastic.index_url + "/_search"

    search_after_query = ''

    if search_after:
        logging.debug("Search after: %s", search_after)
        # timestamp uuid
        search_after_query = ', "search_after": [%i, "%s"] ' % (search_after[0], search_after[1])

    query = """
    {
        "size": %i,
        "query": {
            "bool": {
                "must": []
            }
        },
        "sort": [
            {"metadata__timestamp": "asc"},
            {"uuid": "asc"}
        ] %s

    }
    """ % (size, search_after_query)

    # logging.debug("%s\n%s", url, json.dumps(json.loads(query), indent=4))
    res = requests.post(url, data=query)

    rjson = None
    try:
        rjson = res.json()
    except Exception:
        logging.error("No JSON found in %s", res.text)
        logging.error("No results found from %s", url)

    return rjson


# Items generator
def fetch(elastic, backend, limit=None, search_after_value=None, scroll=True):
    """ Fetch the items from raw or enriched index """

    logging.debug("Creating a elastic items generator.")

    elastic_scroll_id = None
    search_after = search_after_value

    while True:
        if scroll:
            rjson = get_elastic_items(elastic, elastic_scroll_id, limit)
        else:
            rjson = get_elastic_items_search(elastic, search_after, limit)

        if rjson and "_scroll_id" in rjson:
            elastic_scroll_id = rjson["_scroll_id"]

        if rjson and "hits" in rjson:
            if not rjson["hits"]["hits"]:
                break
            for hit in rjson["hits"]["hits"]:
                item = hit['_source']
                if 'sort' in hit:
                    search_after = hit['sort']
                try:
                    backend._fix_item(item)
                except Exception:
                    pass
                yield item
        else:
            logging.error("No results found from %s", elastic.index_url)
            break

    return


def export_items(elastic_url, in_index, out_index, elastic_url_out=None,
                 search_after=False, search_after_value=None, limit=None,
                 copy=False):
    """ Export items from in_index to out_index using the correct mapping """

    if not limit:
        limit = DEFAULT_LIMIT

    if search_after_value:
        search_after_value_timestamp = int(search_after_value[0])
        search_after_value_uuid = search_after_value[1]
        search_after_value = [search_after_value_timestamp, search_after_value_uuid]

    logging.info("Exporting items from %s/%s to %s", elastic_url, in_index, out_index)

    count_res = requests.get('%s/%s/_count' % (elastic_url, in_index))
    try:
        count_res.raise_for_status()
    except requests.exceptions.HTTPError:
        if count_res.status_code == 404:
            logging.error("The index does not exists: %s", in_index)
        else:
            logging.error(count_res.text)
        sys.exit(1)

    logging.info("Total items to copy: %i", count_res.json()['count'])

    # Time to upload the items with the correct mapping
    elastic_in = ElasticSearch(elastic_url, in_index)
    if not copy:
        # Create the correct mapping for the data sources detected from in_index
        ds_mapping = find_mapping(elastic_url, in_index)
    else:
        logging.debug('Using the input index mapping')
        ds_mapping = extract_mapping(elastic_url, in_index)

    if not elastic_url_out:
        elastic_out = ElasticSearch(elastic_url, out_index, mappings=ds_mapping)
    else:
        elastic_out = ElasticSearch(elastic_url_out, out_index, mappings=ds_mapping)

    # Time to just copy from in_index to our_index
    uid_field = find_uuid(elastic_url, in_index)
    backend = find_perceval_backend(elastic_url, in_index)
    if search_after:
        total = elastic_out.bulk_upload(fetch(elastic_in, backend, limit,
                                              search_after_value, scroll=False), uid_field)
    else:
        total = elastic_out.bulk_upload(fetch(elastic_in, backend, limit), uid_field)

    logging.info("Total items copied: %i", total)


if __name__ == '__main__':
    """Tool to ensure correct mappings in GrimoireLab indexes"""

    ARGS = get_params()

    if ARGS.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    if ARGS.limit:
        ElasticSearch.max_items_bulk = ARGS.limit

    export_items(ARGS.elastic_url, ARGS.in_index, ARGS.out_index,
                 ARGS.elastic_url_write, ARGS.search_after, ARGS.search_after_value,
                 ARGS.limit, ARGS.copy)
