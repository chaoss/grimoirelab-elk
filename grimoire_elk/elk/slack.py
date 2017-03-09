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

from dateutil import parser

from .enrich import Enrich, metadata


logger = logging.getLogger(__name__)



class SlackEnrich(Enrich):

    def get_field_author(self):
        return "user_data"

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
        identity = {
            'username': None,
            'name': None,
            'email': None
        }

        from_ = item
        if 'data' in item and type(item) == dict:
            if self.get_field_author() not in item['data']:
                # Message from bot
                identity['username'] = item['data']['bot_id']
                return identity
            from_ = item['data'][self.get_field_author()]

        identity['username'] = from_['name']
        identity['name'] = from_['real_name']
        if 'profile' in from_:
            identity['email'] = from_['profile']['email']
        return identity

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        print(item['data'].keys())

        identity = self.get_sh_identity(item)

        identities.append(identity)

        return identities


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
        copy_fields = ["text", "type", "reply_count", "subscribed",
                       "unread_count", "user"]
        for f in copy_fields:
            if f in message:
                eitem[f] = message[f]
            else:
                eitem[f] = None

        eitem['number_attachs'] = 0
        if 'attachments' in message:
            eitem['number_attachs'] = len(message['attachments'])

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "Slack"))

        return eitem
