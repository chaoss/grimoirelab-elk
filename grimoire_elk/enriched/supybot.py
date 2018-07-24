# -*- coding: utf-8 -*-
#
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

import logging

from dateutil import parser

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping


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
                  "fielddata": true
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
        identities = []
        user = self.get_sh_identity(item['data']['nick'])
        identities.append(user)
        return identities

    def get_sh_identity(self, item, identity_field=None):
        identity = {}
        identity['username'] = None
        identity['email'] = None
        identity['name'] = None

        if not item:
            return identity

        nick = item
        if 'data' in item and type(item) == dict:
            nick = item['data'][identity_field]

        identity['username'] = nick
        identity['name'] = nick

        return identity

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
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
        eitem["update_date"] = parser.parse(item["metadata__updated_on"]).isoformat()
        eitem["channel"] = eitem["origin"]

        eitem.update(self.get_grimoire_fields(eitem["update_date"], "message"))
        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        return eitem
