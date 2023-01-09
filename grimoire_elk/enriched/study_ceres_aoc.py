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
#   Alberto Pérez García-Plaza <alpgarcia@bitergia.com>
#
import hashlib
import logging

from elasticsearch import helpers

from cereslib.dfutils.filter import FilterRows
from cereslib.enrich.enrich import FileType, FilePath, ToUTF8
from cereslib.events.events import Git, Events

from grimoire_elk.enriched.ceres_base import ESConnector, CeresBase


logger = logging.getLogger(__name__)


class ESPandasConnector(ESConnector):
    """ElasticSearch connector to ease data management with Pandas library.

    Extends ES connector to work directly with `_source` part of each hit
    nstead of using the hit as it comes from ES. This intends to ease the
    process of building a pandas dataframe.

    Writing is also ready to work directly with Pandas dataframes.
    """

    def __init__(self, es_conn, es_index, sort_on_field='metadata__timestamp', repo=None, read_only=True):

        super().__init__(es_conn=es_conn, es_index=es_index, sort_on_field=sort_on_field, repo=repo,
                         read_only=read_only)
        self.__log_prefix = "[" + es_index + "] study areas_of_code "

    @staticmethod
    def make_hashcode(uuid, filepath, file_event):
        """Generate a SHA1 based on the given arguments.
        :param uuid: perceval uuid of the item
        :param filepath: path of the corresponding file
        :param file_event: commit file event
        :returns: a SHA1 hash code
        """

        content = ':'.join([uuid, filepath, file_event])
        hashcode = hashlib.sha1(content.encode('utf-8'))
        return hashcode.hexdigest()

    def read_item(self, from_date=None):
        """Read items one by one.

        :param from_date: start date for incremental reading
        :return: _source field of each ES hit.
        :raises ValueError: `metadata__timestamp` field not found in index
        :raises NotFoundError: index not found in ElasticSearch
        """

        search_query = self._build_search_query(from_date)
        for hit in helpers.scan(self._es_conn,
                                search_query,
                                scroll='300m',
                                index=self._es_index,
                                preserve_order=True):
            yield hit["_source"]

    def read_block(self, size, from_date=None):
        """Read items block by block.

        :param from_date: start date for incremental reading.
        :param size: block maximum size.
        :return: list of _source fields of ES hits.
        :raises ValueError: `metadata__timestamp` field not found in index
        :raises NotFoundError: index not found in ElasticSearch
        """
        search_query = self._build_search_query(from_date)
        logger.debug(self.__log_prefix + str(search_query))
        hits_block = []
        for hit in helpers.scan(self._es_conn,
                                search_query,
                                scroll='300m',
                                size=500,
                                index=self._es_index,
                                preserve_order=True):

            hits_block.append(hit["_source"])

            if len(hits_block) % size == 0:
                yield hits_block

                # Reset hits block
                hits_block = []

        if len(hits_block) > 0:
            yield hits_block

    def write(self, items):
        """Write items into ElasticSearch.

        :param items: Pandas DataFrame
        """

        if self._read_only:
            raise IOError("Cannot write, Connector created as Read Only")

        # Uploading info to the new ES
        rows = items.to_dict("index")
        docs = []
        for row_index in rows.keys():
            row = rows[row_index]
            item_id = self.make_hashcode(row[Events.PERCEVAL_UUID], row[Git.FILE_PATH], row[Git.FILE_EVENT])
            row['uuid'] = item_id
            doc = {
                "_index": self._es_index,
                "_type": "items",
                "_id": item_id,
                "_source": row
            }

            if (self._es_major == '7' and self._es_distribution == 'elasticsearch') or\
               (self._es_major == '1' and self._es_distribution == 'opensearch'):
                doc.pop('_type')

            docs.append(doc)
        # TODO exception and error handling
        chunk_size = 2000
        chunks = [docs[i:i + chunk_size] for i in range(0, len(docs), chunk_size)]
        for chunk in chunks:
            helpers.bulk(self._es_conn, chunk)
        logger.debug("{} Written: {}".format(self.__log_prefix, len(docs)))


class AreasOfCode(CeresBase):
    """Enrich Git RAW data to add areas of code information.

    Generate one entry per each modified file involved in each commit, storing some specific fields
    like file name, path, path parts and extension.

    :param self._in: ESPandasConnector for reading source items from.
    :param self._out: ESPandasConnector to write processed items to.
    :param self._block_size: number of items to retrieve for each processing block.
    :param self._git_enrich: GitEnrich object to manage SortingHat affiliations.
    """

    MESSAGE_MAX_SIZE = 80

    def __init__(self, in_connector, out_connector, block_size, git_enrich):

        super().__init__(in_connector, out_connector, block_size)

        self._git_enrich = git_enrich
        self.__log_prefix = "[git] study areas_of_code"

    def process(self, items_block):
        """Process items to add file related information.

        Eventize items creating one new item per each file found in the commit (excluding
        files with no actions performed on them). For each event, file path, file name,
        path parts, file type and file extension are added as fields.

        :param items_block: items to be processed. Expects to find ElasticSearch hits _source part only.
        """

        logger.debug("{} New commits: {}".format(self.__log_prefix, len(items_block)))

        # Create events from commits
        git_events = Git(items_block, self._git_enrich)
        events_df = git_events.eventize(2)

        logger.debug("{} New events: {}".format(self.__log_prefix, len(events_df)))

        if len(events_df) > 0:
            # Filter information
            data_filtered = FilterRows(events_df)
            events_df = data_filtered.filter_(["filepath"], "-")

            logger.debug("{} New events filtered: {}".format(self.__log_prefix, len(events_df)))

            events_df['message'] = events_df['message'].str.slice(stop=AreasOfCode.MESSAGE_MAX_SIZE)
            logger.debug("{} Remove message content".format(self.__log_prefix))

            # Add filetype info
            enriched_filetype = FileType(events_df)
            events_df = enriched_filetype.enrich('filepath')

            logger.debug("{} New Filetype events: {}".format(self.__log_prefix, len(events_df)))

            # Split filepath info
            enriched_filepath = FilePath(events_df)
            events_df = enriched_filepath.enrich('filepath')

            logger.debug("{} New Filepath events: {}".format(self.__log_prefix, len(events_df)))

            events_df['origin'] = events_df['repository']

            # Deal with surrogates
            convert = ToUTF8(events_df)
            events_df = convert.enrich(["owner"])

        logger.debug("{} Final new events: {}".format(self.__log_prefix, len(events_df)))

        return self.ProcessResults(processed=len(events_df), out_items=events_df)


def areas_of_code(git_enrich, in_conn, out_conn, block_size=100):
    """Build and index for areas of code from a given Perceval RAW index.

    :param block_size: size of items block.
    :param git_enrich: GitEnrich object to deal with SortingHat affiliations.
    :param in_conn: ESPandasConnector to read from.
    :param out_conn: ESPandasConnector to write to.
    :return: number of documents written in ElasticSearch enriched index.
    """
    aoc = AreasOfCode(in_connector=in_conn, out_connector=out_conn, block_size=block_size,
                      git_enrich=git_enrich)
    ndocs = aoc.analyze()
    return ndocs
