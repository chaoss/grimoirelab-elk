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

import logging

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping
from grimoirelab_toolkit.datetime import str_to_datetime

logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
                "body_analyzed": {
                  "type": "text",
                  "fielddata": true,
                  "index": true
                }
           }
        } """

        return {"items": mapping}


class SupybotEnrich(Enrich):

    mapping = Mapping

    def get_field_author(self):
        return "nick"

    def get_identities(self, item):
        """ Return the identities from an item """

        user = self.get_sh_identity(item['data']['nick'])
        yield user

    def get_sh_identity(self, item, identity_field=None):
        identity = {}
        identity['username'] = None
        identity['email'] = None
        identity['name'] = None

        if not item:
            return identity

        nick = item
        if isinstance(item, dict) and 'data' in item:
            nick = item['data'][identity_field]

        identity['username'] = nick
        identity['name'] = nick

        return identity

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)
        # The real data
        message = item['data']

        # data fields to copy
        copy_fields = ["nick", "body", "type"]
        for f in copy_fields:
            if f in message:
                eitem[f] = message[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"body": "body_analyzed", "timestamp": "sent_date"}
        for fn in map_fields:
            eitem[map_fields[fn]] = message[fn]

        # Enrich dates
        eitem["update_date"] = str_to_datetime(item["metadata__updated_on"]).isoformat()
        eitem["channel"] = eitem["origin"]

        eitem.update(self.get_grimoire_fields(eitem["update_date"], "message"))
        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem
