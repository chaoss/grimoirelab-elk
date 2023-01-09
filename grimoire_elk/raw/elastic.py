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
#   Quan Zhou <quan@bitergia.com>
#

"""Ocean feeder for Elastic from  Perseval data"""


import inspect
import logging

from grimoirelab_toolkit.datetime import unixtime_to_datetime

from datetime import datetime
from ..enriched.utils import get_repository_filter, anonymize_url
from ..elastic_items import ElasticItems
from ..elastic_mapping import Mapping
from ..errors import ELKError
from ..identities.identities import Identities

logger = logging.getLogger(__name__)


PRJ_JSON_FILTER_SEPARATOR = "--filter-"
PRJ_JSON_FILTER_OP_ASSIGNMENT = "="


class ElasticOcean(ElasticItems):

    mapping = Mapping
    identities = Identities

    @classmethod
    def add_params(cls, cmdline_parser):
        """ Shared params in all backends """

        parser = cmdline_parser

        parser.add_argument("-e", "--elastic_url", default="http://127.0.0.1:9200",
                            help="Host with elastic search (default: http://127.0.0.1:9200)")
        parser.add_argument("--elastic_url-enrich",
                            help="Host with elastic search and enriched indexes")

    def __init__(self, perceval_backend, from_date=None, fetch_archive=False,
                 project=None, insecure=True, offset=None, anonymize=False,
                 to_date=None):

        super().__init__(perceval_backend, from_date, insecure, offset, to_date)

        self.fetch_archive = fetch_archive  # fetch from archive
        self.project = project  # project to be used for this data source
        self.anonymize = anonymize

    def set_elastic_url(self, url):
        """ Elastic URL """
        self.elastic_url = url

    def set_elastic(self, elastic):
        """ Elastic used to store last data source state """
        self.elastic = elastic

    def get_field_date(self):
        """ Field with the update in the JSON items. Now the same in all. """
        return "metadata__updated_on"

    def get_field_unique_id(self):
        field = "uuid"
        return field

    def get_elastic_analyzers(self):
        """ Custom analyzers for our indexes  """

        return None

    def get_last_update_from_es(self, filters_=None):
        last_update = self.elastic.get_last_date(self.get_field_date(),
                                                 filters_=filters_)

        return last_update

    def get_connector_name(self):
        """ Find the name for the current connector """
        from ..utils import get_connector_name
        return get_connector_name(type(self))

    @classmethod
    def get_p2o_params_from_url(cls, url):
        """ Get the p2o params given a URL for the data source """

        # if the url doesn't contain a filter separator, return it
        if PRJ_JSON_FILTER_SEPARATOR not in url:
            return {"url": url}

        # otherwise, add the url to the params
        params = {'url': url.split(PRJ_JSON_FILTER_SEPARATOR, 1)[0].strip()}
        # tokenize the filter and add them to the param dict
        tokens = url.split(PRJ_JSON_FILTER_SEPARATOR)[1:]

        if len(tokens) > 1:
            cause = "Too many filters defined for {}, only the first one is considered".format(url)
            logger.warning(cause)

        token = tokens[0]
        filter_tokens = token.split(PRJ_JSON_FILTER_OP_ASSIGNMENT)

        if len(filter_tokens) != 2:
            cause = "Too many tokens after splitting for {} in {}".format(token, url)
            logger.error(cause)
            raise ELKError(cause=cause)

        fltr_name = filter_tokens[0].strip()
        fltr_value = filter_tokens[1].strip()

        params['filter-' + fltr_name] = fltr_value

        return params

    @classmethod
    def get_perceval_params_from_url(cls, url):
        """ Get the perceval params given a URL for the data source """
        return [url]

    def drop_item(self, item):
        """ Drop items not to be inserted in Elastic """
        return False

    def _fix_item(self, item):
        """ Some buggy data sources need fixing (like mbox and message-id) """
        pass

    def add_update_date(self, item):
        """ All item['updated_on'] from perceval is epoch """
        updated = unixtime_to_datetime(item['updated_on'])
        timestamp = unixtime_to_datetime(item['timestamp'])
        item['metadata__updated_on'] = updated.isoformat()
        # Also add timestamp used in incremental enrichment
        item['metadata__timestamp'] = timestamp.isoformat()

    def feed(self, from_date=None, from_offset=None, category=None, branches=None,
             latest_items=None, filter_classified=None, no_update=None, to_date=None):
        """Feed data in Elastic from Perceval"""

        if self.fetch_archive:
            items = self.perceval_backend.fetch_from_archive()
            self.feed_items(items)
            return

        if from_date and from_offset:
            raise RuntimeError("Can't not feed using from_date and from_offset.")

        # We need to filter by repository to support several repositories
        # in the same raw index

        filters_ = [get_repository_filter(self.perceval_backend, self.get_connector_name())]

        # Check if backend supports from_date
        signature = inspect.signature(self.perceval_backend.fetch)

        last_update = None
        if 'from_date' in signature.parameters:
            if from_date:
                last_update = from_date
            else:
                self.last_update = self.get_last_update_from_es(filters_=filters_)
                last_update = self.last_update

            logger.info("[{}] Incremental from: {} until {} for {}".format(
                        self.perceval_backend.__class__.__name__.lower(),
                        last_update, to_date, anonymize_url(self.perceval_backend.origin)))

        offset = None
        if 'offset' in signature.parameters:
            if from_offset:
                offset = from_offset
            else:
                offset = self.elastic.get_last_offset("offset", filters_=filters_)

            if offset is not None:
                logger.info("[{}] Incremental from: {} offset, for {}".format(
                            self.perceval_backend.__class__.__name__.lower(),
                            offset, anonymize_url(self.perceval_backend.origin)))
            else:
                logger.info("[{}] Not incremental".format(
                            self.perceval_backend.__class__.__name__.lower()))

        params = {}
        # category, filter_classified, and to_date params are shared
        # by all Perceval backends
        if category is not None:
            params['category'] = category
        if branches is not None:
            params['branches'] = branches
        if filter_classified is not None:
            params['filter_classified'] = filter_classified
        if to_date:
            params['to_date'] = to_date

        # no_update, latest_items, from_date and offset cannot be used together,
        # thus, the params dictionary is filled with the param available
        # and Perceval is executed
        if no_update:
            params['no_update'] = no_update
            items = self.perceval_backend.fetch(**params)
        elif latest_items:
            params['latest_items'] = latest_items
            items = self.perceval_backend.fetch(**params)
        elif last_update:
            last_update = last_update.replace(tzinfo=None)
            params['from_date'] = last_update
            items = self.perceval_backend.fetch(**params)
        elif offset is not None:
            params['offset'] = offset
            items = self.perceval_backend.fetch(**params)
        else:
            items = self.perceval_backend.fetch(**params)

        self.feed_items(items)
        self.update_items()

    def update_items(self):
        """Perform update operations over a raw index, just after the collection.
        It must be redefined in the raw connectors."""

        return

    def feed_items(self, items):
        task_init = datetime.now()

        items_pack = []  # to feed item in packs
        drop = 0
        added = 0

        for item in items:
            # print("%s %s" % (item['url'], item['lastUpdated_date']))
            # Add date field for incremental analysis if needed
            self.add_update_date(item)
            self._fix_item(item)
            if self.project:
                item['project'] = self.project
            if self.anonymize:
                self.identities.anonymize_item(item)
            if len(items_pack) >= self.elastic.max_items_bulk:
                self._items_to_es(items_pack)
                items_pack = []
            if not self.drop_item(item):
                items_pack.append(item)
                added += 1
            else:
                drop += 1
        self._items_to_es(items_pack)

        total_time_min = (datetime.now() - task_init).total_seconds() / 60

        logger.debug("[{}] Added {} items to index {}".format(
                     self.perceval_backend.__class__.__name__.lower(),
                     added, self.elastic.index))
        logger.debug("[{}] Dropped {} items using drop_item filter".format(
                     self.perceval_backend.__class__.__name__.lower(), drop))
        logger.debug("[{}] Finished in {:.2f} min".format(
                     self.perceval_backend.__class__.__name__.lower(),
                     total_time_min))
        return self

    def _items_to_es(self, json_items):
        """ Append items JSON to ES (data source state) """

        if len(json_items) == 0:
            return

        logger.debug("[{}] Adding items to Raw for {} ({} items)".format(
                     self.perceval_backend.__class__.__name__.lower(),
                     self, len(json_items)))

        field_id = self.get_field_unique_id()

        inserted = self.elastic.bulk_upload(json_items, field_id)

        if len(json_items) != inserted:
            missing = len(json_items) - inserted
            info = json_items[0]

            name = info['backend_name']
            version = info['backend_version']
            origin = info['origin']

            logger.warning("[{}] {}/{} missing JSON items for backend {} [ver. {}], origin {}".format(
                           self.perceval_backend.__class__.__name__.lower(),
                           missing, len(json_items), name, version, origin))

        return inserted
