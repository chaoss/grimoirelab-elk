# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
                  "fielddata": true,
                  "index": true
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
        if isinstance(item, dict) and 'data' in item:
            if self.get_field_author() not in item['data']:
                # Message from bot. For the rare cases where both user
                # and bot_id are not present, an empty identity is returned
                identity['username'] = item['data'].get('bot_id', None)
                return identity
            from_ = item['data'][self.get_field_author()]

        if not from_:
            return identity
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

        identity = self.get_sh_identity(item)
        yield identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        repo = repo.replace(SLACK_URL, "")  # only the channel id is included for the mapping
        return repo

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)

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

        if 'files' in message:
            eitem['number_files'] = len(message['files'])
            message_file_size = 0
            for file in message['files']:
                message_file_size += file.get('size', 0)
            eitem['message_file_size'] = message_file_size

        if 'user_data' in message and message['user_data']:
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

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
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
