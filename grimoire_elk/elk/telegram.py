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

from dateutil import parser


from .enrich import Enrich, metadata


class TelegramEnrich(Enrich):

    def get_field_author(self):
        return "from"

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "text_analyzed": {
                  "type": "string",
                  "index":"analyzed"
                  }
           }
        } """

        return {"items":mapping}

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        from_ = item
        if 'data' in item and type(item) == dict:
            from_ = item['data']['message'][identity_field]

        identity['username'] = from_['username']
        identity['email'] = None
        identity['name'] = from_['username']
        if 'first_name' in from_:
            identity['name'] = from_['first_name']
        return identity


    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        message = item['data']['message']

        identity = self.get_sh_identity(message['from'])

        identities.append(identity)

        return identities


    @metadata
    def get_rich_item(self, item):
        eitem = {}

        # metadata fields to copy
        copy_fields = ["metadata__updated_on","metadata__timestamp","ocean-unique-id","origin"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None


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

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "telegram"))

        return eitem
