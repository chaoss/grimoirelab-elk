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


from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping


logger = logging.getLogger(__name__)

SLACK_URL = "https://slack.com/"


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
                  "fielddata": true
                  }
           }
        } """

        return {"items": mapping}


class SlackEnrich(Enrich):

    mapping = Mapping

    def get_field_author(self):
        return "user_data"

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
        identity['name'] = from_['name']
        if 'real_name' in from_:
            identity['name'] = from_['real_name']
        if 'profile' in from_:
            if 'email' in from_['profile']:
                identity['email'] = from_['profile']['email']
        return identity

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        identity = self.get_sh_identity(item)

        identities.append(identity)

        return identities

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        repo = repo.replace(SLACK_URL, "")  # only the channel id is included for the mapping
        return repo

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

        eitem["reply_count"] = 0  # be sure it is always included

        # data fields to copy
        copy_fields = ["text", "type", "reply_count", "subscribed", "subtype",
                       "unread_count", "user"]
        for f in copy_fields:
            if f in message:
                eitem[f] = message[f]
            else:
                eitem[f] = None

        eitem['text_analyzed'] = eitem['text']

        eitem['number_attachs'] = 0
        if 'attachments' in message and message['attachments']:
            eitem['number_attachs'] = len(message['attachments'])

        eitem['reaction_count'] = 0
        if 'reactions' in message:
            eitem['reaction_count'] = len(message['reactions'])
            eitem['reactions'] = []
            for rdata in message['reactions']:
                # {
                #         "count": 2,
                #         "users": [
                #            "U38J51N7J",
                #            "U3Q0VLHU3"
                #         ],
                #         "name": "+1"
                # }
                for i in range(0, rdata['count']):
                    eitem['reactions'].append(rdata["name"])

        if 'file' in message:
            eitem['file_type'] = message['file']['pretty_type']
            eitem['file_title'] = message['file']['title']
            eitem['file_size'] = message['file']['size']
            eitem['file_name'] = message['file']['name']
            eitem['file_mode'] = message['file']['mode']
            eitem['file_is_public'] = message['file']['is_public']
            eitem['file_is_external'] = message['file']['is_external']
            eitem['file_id'] = message['file']['id']
            eitem['file_is_editable'] = message['file']['editable']

        if 'user_data' in message:
            eitem['team_id'] = message['user_data']['team_id']
            if 'tz_offset' in message['user_data']:
                eitem['tz'] = message['user_data']['tz_offset']
                # tz must be in -12h to 12h interval, so seconds -> hours
                eitem['tz'] = round(int(eitem['tz']) / (60 * 60))
            if 'is_admin' in message['user_data']:
                eitem['is_admin'] = message['user_data']['is_admin']
            if 'is_owner' in message['user_data']:
                eitem['is_owner'] = message['user_data']['is_owner']
            if 'is_primary_owner' in message['user_data']:
                eitem['is_primary_owner'] = message['user_data']['is_primary_owner']
            if 'profile' in message['user_data']:
                if 'title' in message['user_data']['profile']:
                    eitem['profile_title'] = message['user_data']['profile']['title']
                eitem['avatar'] = message['user_data']['profile']['image_32']

        # Channel info
        channel = message['channel_info']
        eitem['channel_name'] = channel['name']
        eitem['channel_id'] = channel['id']
        eitem['channel_created'] = channel['created']
        # Due to a Slack API change, the list of members is returned paginated, thus the new attribute `num_members`
        # has been added to the Slack Perceval backend. In order to avoid breaking changes, the former
        # variable `members` is kept and used only if `num_members` is not present in the input item.
        eitem['channel_member_count'] = channel['num_members'] if 'num_members' in channel else len(channel['members'])
        if 'topic' in channel:
            eitem['channel_topic'] = channel['topic']
        if 'purpose' in channel:
            eitem['channel_purpose'] = channel['purpose']
        channel_bool_fields = ['is_archived', 'is_general', 'is_starred']
        for field in channel_bool_fields:
            eitem['channel_' + field] = 0
            if field in channel and channel[field]:
                eitem['channel_' + field] = 1

        eitem = self.__convert_booleans(eitem)

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "message"))

        return eitem

    def __convert_booleans(self, eitem):
        """ Convert True/False to 1/0 for better kibana processing """

        for field in eitem.keys():
            if isinstance(eitem[field], bool):
                if eitem[field]:
                    eitem[field] = 1
                else:
                    eitem[field] = 0
        return eitem  # not needed becasue we are modifying directly the dict
