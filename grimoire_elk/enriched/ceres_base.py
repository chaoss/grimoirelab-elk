# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2018 Bitergia
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
#     Alberto Pérez García-Plaza <alpgarcia@bitergia.com>
#

import logging
from collections import namedtuple
from grimoirelab_toolkit import datetime

from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Search


logger = logging.getLogger(__name__)


class Connector:
    """Abstract class for reading and writing items.

    This class provides methods for reading and writing items from/to a
    given data source.

    """

    def read_item(self, from_date=None):
        raise NotImplementedError

    def read_block(self, size, from_date=None):
        raise NotImplementedError

    def write(self, items):
        raise NotImplementedError

    def latest_date(self):
        raise NotImplementedError


class CeresBase:
    """Base class to process items and create enriched indexes.

    Provides basic structure needed to build cereslib based processors. This basic structure
    is focused on reading blocks of items, processing them and writing the resulting processed
    data. As this is a very general procedure, it is possible to be extend  this class to create
    other non-cereslib based processors.

    :param self._in: connector for reading source items from.
    :param self._out: connector to write processed items to.
    :param self._block_size: number of items to retrieve for each processing block.
    """

    ProcessResults = namedtuple('ProcessResults', ['processed', 'out_items'])

    def __init__(self, in_connector, out_connector, block_size):

        self._in = in_connector
        self._out = out_connector
        self._block_size = block_size

    def analyze(self):
        """Populate an enriched index by processing input items in blocks.

        :return: total number of out_items written.
        """
        from_date = self._out.latest_date()
        if from_date:
            logger.info("Reading items since " + from_date)
        else:
            logger.info("Reading items since the beginning of times")

        cont = 0
        total_processed = 0
        total_written = 0

        for item_block in self._in.read_block(size=self._block_size, from_date=from_date):
            cont = cont + len(item_block)

            process_results = self.process(item_block)
            total_processed += process_results.processed

            if len(process_results.out_items) > 0:
                self._out.write(process_results.out_items)
                total_written += len(process_results.out_items)
            else:
                logger.info("No new items to be written this time.")

            logger.info(
                "Items read/to be written/total read/total processed/total written: " +
                "{0}/{1}/{2}/{3}/{4}".format(str(len(item_block)),
                                             str(len(process_results.out_items)),
                                             str(cont),
                                             str(total_processed),
                                             str(total_written)))

        logger.info("SUMMARY: Items total read/total processed/total written: " +
                    "{0}/{1}/{2}".format(str(cont),
                                         str(total_processed),
                                         str(total_written)))

        logger.info("This is the end.")

        return total_written

    def process(self, items_block):
        """Process a sets of items.

        :param items_block: set of items to be processed. Its type depends on the connector used to read
                            them (`CeresBase._in`).
        :return: namedtuple containing:
            - processed: number of processed items.
            - out_items: items ready to be written. Must be compatible with out connector (`CeresBase._out`).

        There are two cases when processed should be different to the length of item list:
            - When creating more than one enriched item for a given source item.
            - When some items are left as they come because there is nothing we can do to process them (it is
              not possible to enrich them with new information for whatever reason). This might happen when
              enriching from an already enriched index to add new info, because when using RAW as input we
              will always process items.
        """
        raise NotImplementedError


class ESConnector(Connector):
    """Connector for ElasticSearch databases.

    :param self._es_conn: ElasticSearch connection for reading from/writing to.
    :param self._es_index: ElasticSearch index for reading from/writing to.
    :param self._sort_on_field: date field to sort results, important for incremental process.
    :param self._read_only: True to avoid unwanted writes.
    """

    def __init__(self, es_conn, es_index, sort_on_field='metadata__timestamp', read_only=True):

        self._es_conn = es_conn
        self._es_index = es_index
        self._sort_on_field = sort_on_field
        self._read_only = read_only

    def read_item(self, from_date=None):
        """Read items and return them one by one.

        :param from_date: start date for incremental reading.
        :return: next single item when any available.
        :raises ValueError: `metadata__timestamp` field not found in index
        :raises NotFoundError: index not found in ElasticSearch
        """
        search_query = self._build_search_query(from_date)
        for hit in helpers.scan(self._es_conn,
                                search_query,
                                scroll='300m',
                                index=self._es_index,
                                preserve_order=True):
            yield hit

    def read_block(self, size, from_date=None):
        """Read items and return them in blocks.

        :param from_date: start date for incremental reading.
        :param size: block size.
        :return: next block of items when any available.
        :raises ValueError: `metadata__timestamp` field not found in index
        :raises NotFoundError: index not found in ElasticSearch
        """
        search_query = self._build_search_query(from_date)
        hits_block = []
        for hit in helpers.scan(self._es_conn,
                                search_query,
                                scroll='300m',
                                index=self._es_index,
                                preserve_order=True):

            hits_block.append(hit)

            if len(hits_block) % size == 0:
                yield hits_block

                # Reset hits block
                hits_block = []

        if len(hits_block) > 0:
            yield hits_block

    def write(self, items):
        """Upload items to ElasticSearch.

        :param items: items to be uploaded.
        """
        if self._read_only:
            raise IOError("Cannot write, Connector created as Read Only")

        # Uploading info to the new ES
        docs = []
        for item in items:
            doc = {
                "_index": self._es_index,
                "_type": "item",
                "_id": item["_id"],
                "_source": item["_source"]
            }
            docs.append(doc)
        # TODO exception and error handling
        helpers.bulk(self._es_conn, docs)
        logger.info("Written: " + str(len(docs)))

    def create_index(self, mappings_file, delete=True):
        """Create a new index.

        :param mappings_file: index mappings to be used.
        :param delete: True to delete current index if exists.
        """

        if self._read_only:
            raise IOError("Cannot write, Connector created as Read Only")

        if delete:
            logger.info("Deleting index " + self._es_index)
            self._es_conn.indices.delete(self._es_index, ignore=[400, 404])

        # Read Mapping
        with open(mappings_file) as f:
            mapping = f.read()

        self._es_conn.indices.create(self._es_index, body=mapping)

    def latest_date(self):
        """Get date of most recent item available in ElasticSearch.

        :return: latest date based on `CeresBase._sort_on` field,
                 None if no values found for that field.

        :raises NotFoundError: index not found in ElasticSearch
        """
        latest_date = None

        search = Search(using=self._es_conn, index=self._es_index)
        # from:to parameters (=> from: 0, size: 0)
        search = search[0:0]
        search = search.aggs.metric('max_date', 'max', field=self._sort_on_field)

        try:
            response = search.execute()

            aggs = response.to_dict()['aggregations']
            if aggs['max_date']['value'] is None:
                logger.debug("No data for " + self._sort_on_field + " field found in " + self._es_index + " index")

            else:
                # Incremental case: retrieve items from last item in ES write index
                max_date = aggs['max_date']['value_as_string']
                latest_date = datetime.str_to_datetime(max_date).isoformat()

        except NotFoundError as nfe:
            raise nfe

        return latest_date

    def exists(self):
        """Check whether or not an index exists in ElasticSearch.

        :return: True if index already exists
        """

        return self._es_conn.indices.exists(index=self._es_index)

    def create_alias(self, alias_name):
        """Creates an alias pointing to the index configured in this connection"""

        return self._es_conn.indices.put_alias(index=self._es_index, name=alias_name)

    def exists_alias(self, alias_name, index_name=None):
        """Check whether or not the given alias exists

        :return: True if alias already exist"""

        return self._es_conn.indices.exists_alias(index=index_name, name=alias_name)

    def _build_search_query(self, from_date):
        """Build an ElasticSearch search query to retrieve items for read methods.

        :param from_date: date to start retrieving items from.
        :return:

        :raises ValueError: `metadata__timestamp` field not found in index
        :raises NotFoundError: index not found in ElasticSearch
        """

        if from_date:
            query = {"range": {self._sort_on_field: {"gte": from_date}}}
        else:
            query = {"match_all": {}}

        sort = [{self._sort_on_field: {"order": "asc"}}]

        search_query = {
            "query": query,
            "sort": sort
        }

        return search_query


class SimpleCopy(CeresBase):
    """Simple enricher to copy an index from one connector to another without modifying or adding any data.
    """

    def process(self, items_block):
        """Return items as they come, updating their metadata__enriched_on field.

        :param items_block:
        :return: hits blocks as they come, updating their metadata__enriched_on field. Namedtuple containing:
            - processed: number of processed hits
            - out_items: a list containing items ready to be written.
        """

        out_items = []

        for hit in items_block:
            if __name__ == '__main__':
                hit['_source']['metadata__enriched_on'] = datetime.datetime_utcnow().isoformat()
            out_items.append(hit)

        return self.ProcessResults(processed=0, out_items=out_items)
