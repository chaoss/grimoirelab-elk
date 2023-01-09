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

import logging
from datetime import datetime

import pandas
from elasticsearch import helpers, NotFoundError
from elasticsearch_dsl import Search, Q

from cereslib.enrich.enrich import Onion
from grimoirelab_toolkit import datetime as gl_dt
from grimoire_elk.enriched.ceres_base import ESConnector, CeresBase


logger = logging.getLogger(__name__)


class ESOnionConnector(ESConnector):
    """ElasticSearch connector to ease data management related to Onion metric.

    Extends ES connector to work directly with Pandas DataFrames instead of using
    hits as they come from ES. This intends to ease the process of Onion metric.

    Writing is also ready to work directly with Pandas DataFrames.

    :param self._es_conn: ElasticSearch connection for reading from/writing to.
    :param self._es_index: ElasticSearch index for reading from/writing to.
    :param self._timeframe_field: date field to sort onion results.
    :param self._sort_on: date field to sort source index results, important for incremental process.
    :param self._read_only: True to avoid unwanted writes.
    """

    AUTHOR_NAME = 'author_name'
    AUTHOR_ORG = 'author_org_name'
    AUTHOR_MULTI_ORG_NAMES = 'author_multi_org_names'
    AUTHOR_UUID = 'author_uuid'
    CONTRIBUTIONS = 'contributions'
    LATEST_TS = 'latest_ts'
    TIMEFRAME = 'timeframe'
    TIMESTAMP = 'metadata__timestamp'
    PROJECT = 'project'

    def __init__(self, es_conn, es_index, contribs_field,
                 timeframe_field='grimoire_creation_date',
                 sort_on_field='metadata__timestamp', read_only=True):

        super().__init__(es_conn=es_conn, es_index=es_index, sort_on_field=sort_on_field, read_only=read_only)

        self.contribs_field = contribs_field
        self._timeframe_field = timeframe_field
        data_source = es_index.split("_")[0]
        self.__log_prefix = "[" + data_source + "] study onion"

    def read_block(self, size=None, from_date=None):
        """Read author commits by Quarter, Org and Project.

        :param from_date: not used here. Incremental mode not supported yet.
        :param size: not used here.
        :return: DataFrame with commit count per author, split by quarter, org and project.
        """
        # label to check whether the input index contains multiple affiliations. The label is
        # initialized to True, and can change its value only if no organizations exist in the
        # attribute `author_multi_org_names`
        multi_org = True

        # Get quarters corresponding to All items (Incremental mode NOT SUPPORTED)
        quarters = self.__quarters()

        for quarter in quarters:

            logger.info("{} Quarter: {}".format(self.__log_prefix, quarter))

            date_range = {self._timeframe_field: {'gte': quarter.start_time, 'lte': quarter.end_time}}

            orgs = self.__list_uniques(date_range, self.AUTHOR_MULTI_ORG_NAMES)
            if not orgs:
                multi_org = False
                logger.warning("{} Attribute {} not found, using {}".format(
                    self.__log_prefix, self.AUTHOR_MULTI_ORG_NAMES, self.AUTHOR_ORG)
                )
                orgs = self.__list_uniques(date_range, self.AUTHOR_ORG)

            projects = self.__list_uniques(date_range, self.PROJECT)

            # Get global data
            s = self.__build_search(date_range)
            response = s.execute()

            for timing in response.aggregations[self.TIMEFRAME].buckets:
                yield self.__build_dataframe(timing).copy()

            # Get global data by Org
            for org_name in orgs:

                logger.debug("{} Quarter: {}  Org: {}".format(self.__log_prefix, quarter, org_name))

                s = self.__build_search(date_range, org_name=org_name, multi_org=multi_org)
                response = s.execute()

                for timing in response.aggregations[self.TIMEFRAME].buckets:
                    yield self.__build_dataframe(timing, org_name=org_name).copy()

            # Get project specific data
            for project in projects:

                logger.debug("{} Quarter: {}  Project: {}".format(
                             self.__log_prefix, quarter, project))

                # Global project
                s = self.__build_search(date_range, project_name=project)
                response = s.execute()

                for timing in response.aggregations[self.TIMEFRAME].buckets:
                    yield self.__build_dataframe(timing, project_name=project).copy()

                # Split by Org
                for org_name in orgs:

                    logger.debug("{} Quarter: {}  Project: {}  Org: {}".format(
                                 self.__log_prefix, quarter, project, org_name))

                    s = self.__build_search(date_range, project_name=project, org_name=org_name, multi_org=multi_org)
                    response = s.execute()

                    for timing in response.aggregations[self.TIMEFRAME].buckets:
                        yield self.__build_dataframe(timing, project_name=project, org_name=org_name).copy()

    def write(self, items):
        """Write items into ElasticSearch.

        :param items: Pandas DataFrame
        """
        if self._read_only:
            raise IOError("Cannot write, Connector created as Read Only")

        if len(items) == 0:
            logger.info("{} Nothing to write".format(self.__log_prefix))
            return

        # Uploading info to the new ES
        rows = items.to_dict("index")
        docs = []
        for row_index in rows.keys():
            row = rows[row_index]
            item_id = row[self.AUTHOR_ORG] + '_' + row[self.PROJECT] + '_' \
                + row[self.TIMEFRAME] + '_' + row[self.AUTHOR_UUID]
            item_id = item_id.replace(' ', '').lower()

            doc = {
                "_index": self._es_index,
                "_type": "item",
                "_id": item_id,
                "_source": row
            }

            if (self._es_major == '7' and self._es_distribution == 'elasticsearch') or \
               (self._es_major == '1' and self._es_distribution == 'opensearch'):
                doc.pop('_type')

            docs.append(doc)

        # TODO uncomment following lines for incremental version
        # # Delete old data if exists to ensure refreshing in case of deleted commits
        # timeframe = docs[0]['_source']['timeframe']
        # org = docs[0]['_source']['author_org_name']
        # project = docs[0]['_source']['project']
        # s = Search(using=self._es_conn, index=self._es_index)
        # s = s.filter('term', project=project)
        # s = s.filter('term', author_org_name=org)
        # s = s.filter('term', timeframe=timeframe)
        # response = s.execute()
        #
        # if response.hits.total > 0:
        #     response = s.delete()
        #     logger.info("[Onion] Deleted " + str(response.deleted) + " items for refreshing: " + timeframe + " "
        #                 + org + " " + project)

        # TODO exception and error handling
        helpers.bulk(self._es_conn, docs)
        logger.debug("{} Written: {}".format(self.__log_prefix, len(docs)))

    def latest_enrichment_date(self):
        """Get the most recent enrichment date.

        :return: latest date based on `metadata__enriched_on` field,
                 None if no values found for that field.

        :raises NotFoundError: index not found in ElasticSearch
        """
        latest_date = None

        search = Search(using=self._es_conn, index=self._es_index)
        # from:to parameters (=> from: 0, size: 0)
        search = search[0:0]
        search = search.aggs.metric('max_date', 'max', field='metadata__enriched_on')

        try:
            response = search.execute()

            aggs = response.to_dict()['aggregations']
            if aggs['max_date']['value'] is None:
                logger.debug("{} No data for metadata__enriched_on field found in {} index".format(
                             self.__log_prefix, self._es_index))

            else:
                # Incremental case: retrieve items from last item in ES write index
                max_date = aggs['max_date']['value_as_string']
                latest_date = gl_dt.str_to_datetime(max_date)

        except NotFoundError as nfe:
            raise nfe

        return latest_date

    def __quarters(self, from_date=None):
        """Get a set of quarters with available items from a given index date.

        :param from_date:
        :return: list of `pandas.Period` corresponding to quarters
        """
        s = Search(using=self._es_conn, index=self._es_index)
        if from_date:
            # Work around to solve conversion problem of '__' to '.' in field name
            q = Q('range')
            q.__setattr__(self._sort_on_field, {'gte': from_date})
            s = s.filter(q)

        # from:to parameters (=> from: 0, size: 0)
        s = s[0:0]

        s.aggs.bucket(self.TIMEFRAME, 'date_histogram', field=self._timeframe_field,
                      interval='quarter', min_doc_count=1)
        response = s.execute()

        quarters = []
        for quarter in response.aggregations[self.TIMEFRAME].buckets:
            period = pandas.Period(quarter.key_as_string, 'Q')
            quarters.append(period)

        return quarters

    def __list_uniques(self, date_range, field_name):
        """Retrieve a list of unique values in a given field within a date range.

        :param date_range:
        :param field_name:
        :return: list  of unique values.
        """
        # Get project list
        s = Search(using=self._es_conn, index=self._es_index)
        s = s.filter('range', **date_range)
        # from:to parameters (=> from: 0, size: 0)
        s = s[0:0]
        s.aggs.bucket('uniques', 'terms', field=field_name, size=1000)
        response = s.execute()
        uniques_list = []
        for item in response.aggregations.uniques.buckets:
            uniques_list.append(item.key)

        return uniques_list

    def __build_search(self, date_range, project_name=None, org_name=None, multi_org=False):
        s = Search(using=self._es_conn, index=self._es_index)
        s = s.filter('range', **date_range)
        if project_name:
            s = s.filter('term', project=project_name)

        if org_name:
            if multi_org:
                s = s.filter('term', author_multi_org_names=org_name)
            else:
                s = s.filter('term', author_org_name=org_name)

        s = s.filter('term', author_bot='false')

        # from:to parameters (=> from: 0, size: 0)
        s = s[0:0]

        # Get author_name and most recent metadata__timestamp for quarter (should be enough per quarter,
        # computing it by user probably is not needed as we are going to recalculate the whole quarter)

        # We are not keeping all metadata__* fields because we are grouping commits by author, so we can only
        # store one value per author.
        s.aggs.bucket(self.TIMEFRAME, 'date_histogram', field=self._timeframe_field, interval='quarter') \
            .metric(self.LATEST_TS, 'max', field=self._sort_on_field)\
            .bucket(self.AUTHOR_UUID, 'terms', field=self.AUTHOR_UUID, size=1000) \
            .metric(self.CONTRIBUTIONS, 'cardinality', field=self.contribs_field, precision_threshold=40000)\
            .bucket(self.AUTHOR_NAME, 'terms', field=self.AUTHOR_NAME, size=1)

        return s

    def __build_dataframe(self, timing, project_name=None, org_name=None):
        """Build a DataFrame from a time bucket.

        :param timing:
        :param project_name:
        :param org_name:
        :return:
        """
        date_list = []
        uuid_list = []
        name_list = []
        contribs_list = []
        latest_ts_list = []
        logger.debug("{} timing: {}".format(self.__log_prefix, timing.key_as_string))

        for author in timing[self.AUTHOR_UUID].buckets:
            latest_ts_list.append(timing[self.LATEST_TS].value_as_string)
            date_list.append(timing.key_as_string)
            uuid_list.append(author.key)
            if author[self.AUTHOR_NAME] and author[self.AUTHOR_NAME].buckets \
                    and len(author[self.AUTHOR_NAME].buckets) > 0:
                name_list.append(author[self.AUTHOR_NAME].buckets[0].key)
            else:
                name_list.append("Unknown")
            contribs_list.append(author[self.CONTRIBUTIONS].value)

        df = pandas.DataFrame()
        df[self.TIMEFRAME] = date_list
        df[self.AUTHOR_UUID] = uuid_list
        df[self.AUTHOR_NAME] = name_list
        df[self.CONTRIBUTIONS] = contribs_list
        df[self.TIMESTAMP] = latest_ts_list

        if not project_name:
            project_name = "_Global_"
        df[self.PROJECT] = project_name

        if not org_name:
            org_name = "_Global_"
        df[self.AUTHOR_ORG] = org_name

        return df


class OnionStudy(CeresBase):
    """Compute Onion metric on a Git enriched index.

    Generate one entry per each modified file involved in each commit, storing some specific fields
    like file name, path, path parts and extension.

    :param self._in: ESOnionConnector for reading source items from.
    :param self._out: ESOnionConnector to write processed items to.
    """

    def __init__(self, in_connector, out_connector, data_source):

        super().__init__(in_connector, out_connector, None)

        self.data_source = data_source
        self.__log_prefix = "[" + data_source + "] study onion"

    def process(self, items_block):
        """Process a DataFrame to compute Onion.

        :param items_block: items to be processed. Expects to find a pandas DataFrame.
        """

        logger.debug("{} Authors to process: {}".format(self.__log_prefix, len(items_block)))

        onion_enrich = Onion(items_block)
        df_onion = onion_enrich.enrich(member_column=ESOnionConnector.AUTHOR_UUID,
                                       events_column=ESOnionConnector.CONTRIBUTIONS)

        # Get and store Quarter as String
        df_onion['quarter'] = df_onion[ESOnionConnector.TIMEFRAME].map(lambda x: str(pandas.Period(x, 'Q')))

        # Add metadata: enriched on timestamp
        df_onion['metadata__enriched_on'] = datetime.utcnow().isoformat()
        df_onion['data_source'] = self.data_source
        df_onion['grimoire_creation_date'] = df_onion[ESOnionConnector.TIMEFRAME]

        logger.debug("{} Final new events: {}".format(self.__log_prefix, len(df_onion)))

        return self.ProcessResults(processed=len(df_onion), out_items=df_onion)


def onion_study(in_conn, out_conn, data_source):
    """Build and index for onion from a given Git index.

    :param in_conn: ESPandasConnector to read from.
    :param out_conn: ESPandasConnector to write to.
    :param data_source: name of the date source to generate onion from.
    :return: number of documents written in ElasticSearch enriched index.
    """
    onion = OnionStudy(in_connector=in_conn, out_connector=out_conn, data_source=data_source)
    ndocs = onion.analyze()
    return ndocs
