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
                "text_analyzed": {
                  "type": "text",
                  "fielddata": true,
                  "index": true
                }
           }
        } """

        return {"items": mapping}


class TelegramEnrich(Enrich):

    mapping = Mapping

    def get_field_author(self):
        return "from"

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        from_ = item
        if isinstance(item, dict) and 'data' in item:
            from_ = item['data']['message'][identity_field]

        identity['username'] = from_.get('username', None)
        identity['email'] = None
        identity['name'] = from_.get('first_name', None)

        return identity

    def get_identities(self, item):
        """ Return the identities from an item """

        message = item['data']['message']
        identity = self.get_sh_identity(message['from'])

        yield identity

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)
        eitem['update_id'] = item['data']['update_id']

        # The real data
        message = item['data']['message']

        # data fields to copy
        copy_fields = ["message_id", "sticker"]
        for f in copy_fields:
            if f in message:
                eitem[f] = message[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"text": "message",
                      "date": "sent_date,"
                      }
        for f in map_fields:
            if f in message:
                eitem[map_fields[f]] = message[f]
            else:
                eitem[map_fields[f]] = None

        if "text" in message:
            eitem["text_analyzed"] = message["text"]

        eitem["text_edited"] = False if "edited" not in message else message["edited"]

        eitem['chat_id'] = message['chat']['id']
        if 'title' in message['chat']:
            eitem['chat_title'] = message['chat']['title']
        eitem['chat_type'] = message['chat']['type']

        eitem['from_id'] = message['from']['id']
        eitem['author'] = message['from']['first_name']
        eitem['author_id'] = message['from']['id']
        if 'last_name' in message['from']:
            eitem['author_last_name'] = message['from']['last_name']
        if 'username' in message['from']:
            eitem['username'] = message['from']['username']

        if 'reply_to_message' in message:
            eitem['reply_to_message_id'] = message['reply_to_message']['message_id']
            eitem['reply_to_sent_date'] = message['reply_to_message']['date']
            if 'text' in message['reply_to_message']:
                eitem['reply_to_message'] = message['reply_to_message']['text']
            elif 'sticker' in message['reply_to_message']:
                eitem['reply_to_message'] = message['reply_to_message']['sticker']
            eitem['reply_to_chat_id'] = message['reply_to_message']['chat']['id']
            eitem['reply_to_chat_title'] = message['reply_to_message']['chat']['title']
            eitem['reply_to_chat_type'] = message['reply_to_message']['chat']['type']
            eitem['reply_to_author_id'] = message['reply_to_message']['from']['id']
            eitem['reply_to_author'] = message['reply_to_message']['from']['first_name']
            if 'last_name' in message['reply_to_message']['from']:
                eitem['reply_to_author_last_name'] = message['reply_to_message']['from']['last_name']
            if 'username' in message['reply_to_message']['from']:
                eitem['reply_to_username'] = message['reply_to_message']['from']['username']

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "telegram"))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem
