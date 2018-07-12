# -*- coding: utf-8 -*-
#
# Ocean lib
#
# Copyright (C) 2015 Bitergia
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

"""Ocean feeder for Elastic from  Perseval data"""


import inspect
import logging

from datetime import datetime
from ..enriched.utils import unixtime_to_datetime, get_repository_filter
from ..elastic_items import ElasticItems
from ..elastic_mapping import Mapping

logger = logging.getLogger(__name__)


class ElasticOcean(ElasticItems):

    mapping = Mapping

    @classmethod
    def add_params(cls, cmdline_parser):
        """ Shared params in all backends """

        parser = cmdline_parser

        parser.add_argument("-e", "--elastic_url", default="http://127.0.0.1:9200",
                            help="Host with elastic search" +
                            "(default: http://127.0.0.1:9200)")
        parser.add_argument("--elastic_url-enrich",
                            help="Host with elastic search and enriched indexes")

    def __init__(self, perceval_backend, from_date=None, fetch_archive=False,
                 project=None, insecure=True, offset=None):

        super().__init__(perceval_backend, from_date, insecure, offset)

        self.fetch_archive = fetch_archive  # fetch from archive
        self.project = project  # project to be used for this data source

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

#    def get_elastic_mappings(self):
#        """ specific mappings implemented in each data source """
#        mapping = '{}'
#
#        return {"items": mapping}

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
        return {"url": url}

    @classmethod
    def get_perceval_params_from_url(cls, url):
        """ Get the perceval params given a URL for the data source """
        return [url]

    @classmethod
    def get_arthur_params_from_url(cls, url):
        """ Get the arthur params given a URL for the data source """
        return {"uri": url, "url": url}

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

    def feed(self, from_date=None, from_offset=None, category=None, latest_items=None, arthur_items=None):
        """ Feed data in Elastic from Perceval or Arthur """

        if self.fetch_archive:
            items = self.perceval_backend.fetch_from_archive()
            self.feed_items(items)
            return
        elif arthur_items:
            items = arthur_items
            self.feed_items(items)
            return

        if from_date and from_offset:
            raise RuntimeError("Can't not feed using from_date and from_offset.")

        # We need to filter by repository to support several repositories
        # in the same raw index
        filters_ = [get_repository_filter(self.perceval_backend,
                    self.get_connector_name())]

        # Check if backend supports from_date
        signature = inspect.signature(self.perceval_backend.fetch)

        last_update = None
        if 'from_date' in signature.parameters:
            if from_date:
                last_update = from_date
            else:
                self.last_update = self.get_last_update_from_es(filters_=filters_)
                last_update = self.last_update

            logger.info("Incremental from: %s", last_update)

        offset = None
        if 'offset' in signature.parameters:
            if from_offset:
                offset = from_offset
            else:
                offset = self.elastic.get_last_offset("offset", filters_=filters_)

            if offset is not None:
                logger.info("Incremental from: %i offset", offset)
            else:
                logger.info("Not incremental")

        if latest_items:
            if category:
                items = self.perceval_backend.fetch(latest_items=latest_items,
                                                    category=category)
            else:
                items = self.perceval_backend.fetch(latest_items=latest_items)
        elif last_update:
            # if offset used for incremental do not use date
            # Perceval backend from_date must not include timezone
            # It always uses the server datetime
            last_update = last_update.replace(tzinfo=None)
            if category:
                items = self.perceval_backend.fetch(from_date=last_update, category=category)
            else:
                items = self.perceval_backend.fetch(from_date=last_update)
        elif offset is not None:
            if category:
                items = self.perceval_backend.fetch(offset=offset, category=category)
            else:
                items = self.perceval_backend.fetch(offset=offset)
        else:
            if category:
                items = self.perceval_backend.fetch(category=category)
            else:
                items = self.perceval_backend.fetch()

        self.feed_items(items)

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

        logger.debug("Added %i items to index %s", added, self.elastic.index)
        logger.debug("Dropped %i items using drop_item filter" % (drop))
        logger.info("Finished in %.2f min" % (total_time_min))

        return self

    def _items_to_es(self, json_items):
        """ Append items JSON to ES (data source state) """

        if len(json_items) == 0:
            return

        logger.info("Adding items to Ocean for %s (%i items)" %
                    (self, len(json_items)))

        field_id = self.get_field_unique_id()

        inserted = self.elastic.bulk_upload(json_items, field_id)

        if len(json_items) != inserted:
            missing = len(json_items) - inserted
            info = json_items[0]

            name = info['backend_name']
            version = info['backend_version']
            origin = info['origin']

            logger.warning("%s/%s missing JSON items for backend %s [ver. %s], origin %s",
                           str(missing),
                           str(len(json_items)),
                           name, version, origin)

        return inserted
