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

from grimoirelab_toolkit.datetime import unixtime_to_datetime


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


class MattermostEnrich(Enrich):

    # This enricher must compatible with the Slack enricher to reuse the Kibiter panel

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
                # Message from bot
                identity['username'] = item['data']['bot_id']
                return identity
            from_ = item['data'][self.get_field_author()]

        identity['username'] = from_['username']
        identity['name'] = from_['username']

        if 'first_name' in from_:
            name_parts = []
            first_name = from_.get('first_name')
            if first_name:
                name_parts.append(first_name)

            last_name = from_.get('last_name')
            if last_name:
                name_parts.append(last_name)

            composed_name = ' '.join(name_parts)

            identity['name'] = composed_name if composed_name else None
        if 'email' in from_:
            email = from_['email']
            identity['email'] = email if email else None
        return identity

    def get_identities(self, item):
        """ Return the identities from an item """

        identity = self.get_sh_identity(item)
        yield identity

    def get_project_repository(self, eitem):
        # https://chat.openshift.io/8j366ft5affy3p36987pcugaoa
        tokens = eitem['origin'].rsplit("/", 1)
        return tokens[0] + " " + tokens[1]

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
            eitem['team_id'] = None  # not exists in Mattermost
            if 'timezone' in message['user_data']:
                if message['user_data']['timezone']['useAutomaticTimezone']:
                    eitem['tz'] = message['user_data']['timezone']['automaticTimezone']
                else:
                    eitem['tz'] = message['user_data']['timezone']['manualTimezone']
                # tz must be in -12h to 12h interval, so seconds -> hours
                if eitem['tz']:
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

        eitem['channel_name'] = message['channel_data']['name']
        eitem['channel_id'] = message['channel_data']['id']
        eitem['channel_created'] = unixtime_to_datetime(message['channel_data']['create_at'] / 1000).isoformat()
        eitem['channel_member_count'] = None

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
