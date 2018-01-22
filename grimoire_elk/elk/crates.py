#!/usr/bin/python3
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

from copy import deepcopy

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

        if es_major != '2':
            mapping = """
            {
                "properties": {
                    "description_analyzed": {
                      "type": "text"
                      }
               }
            } """
        else:
            mapping = """
            {
                "properties": {
                    "description_analyzed": {
                      "type": "string",
                      "index": "analyzed"
                      }
               }
            } """

        return {"items": mapping}


class CratesEnrich(Enrich):

    mapping = Mapping

    def get_field_author(self):
        return 'owner_user_data'

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        field = self.get_field_author()
        identities.append(self.get_sh_identity(item, field))

        return identities

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if 'data' in item and isinstance(item, dict):
            users = item['data'][identity_field]['users']
            if not users:
                return identity
            else:
                user = users[0]

        identity['username'] = None
        identity['email'] = None
        identity['name'] = None
        if 'login' in user:
            identity['username'] = user['login']
        if 'name' in user:
            identity['name'] = user['name']

        return identity

    def get_field_event_unique_id(self):
        return "download_sample_id"

    def get_rich_events(self, item):
        """
        In the events there are some common fields with the crate. The name
        of the field must be the same in the create and in the downloads event
        so we can filer using it in crate and event at the same time.

        * Fields that don't change: the field does not change with the events
        in a create so the value is always the same in the events of a create.

        * Fields that change: the value of the field changes with events
        """
        events = []

        if "version_downloads_data" not in item['data']:
            return events

        # To get values from the task
        eitem = self.get_rich_item(item)

        for sample in item['data']["version_downloads_data"]["version_downloads"]:
            event = deepcopy(eitem)
            event['download_sample_id'] = sample['id']
            event['sample_date'] = sample['date']
            sample_date = parser.parse(event['sample_date'])
            event['sample_version'] = sample['version']
            event['sample_downloads'] = sample['downloads']
            event.update(self.get_grimoire_fields(sample_date.isoformat(), "downloads_event"))

            events.append(event)

        return events

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        crate = item['data']

        # data fields to copy
        copy_fields = ["id", "homepage", "name", "repository", "downloads",
                       "description", "recent_downloads", "max_version",
                       "keywords", "categories", "badges", "versions",
                       "updated_at", "created_at"]

        for f in copy_fields:
            if f in crate:
                eitem[f] = crate[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"description": "description_analyzed"}
        for fn in map_fields:
            eitem[map_fields[fn]] = crate[fn]

        # author info
        if crate['owner_user_data']['users']:
            author = crate['owner_user_data']['users'][0]
            eitem['author_id'] = author['id']
            eitem['author_login'] = author['login']
            eitem['author_url'] = author['url']
            eitem['author_avatar'] = author['avatar']

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(eitem['created_at'], "crates"))

        return eitem
