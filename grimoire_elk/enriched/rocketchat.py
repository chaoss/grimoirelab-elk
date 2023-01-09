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
#   Animesh Kumar<animuz111@gmail.com>
#   Obaro Ikoh <obaroikohb@gmail.com>
#

import logging

from grimoirelab_toolkit.datetime import str_to_datetime

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
                "msg_analyzed": {
                  "type": "text",
                  "fielddata": true,
                  "index": true
                }
           }
        } """

        return {"items": mapping}


class RocketChatEnrich(Enrich):
    mapping = Mapping

    def get_field_author(self):
        return "u"

    def get_sh_identity(self, item, identity_field=None):
        identity = {
            'username': None,
            'name': None,
            'email': None
        }

        if self.get_field_author() not in item['data']:
            return identity
        user = item['data'][self.get_field_author()]

        identity['username'] = user.get('username', None)
        identity['name'] = user.get('name', None)

        return identity

    def get_identities(self, item):
        """ Return the identities from an item """

        identity = self.get_sh_identity(item)
        yield identity

    def get_project_repository(self, eitem):
        tokens = eitem['origin'].rsplit("/", 1)
        return tokens[0] + " " + tokens[1]

    @metadata
    def get_rich_item(self, item):

        eitem = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)

        message = item['data']

        eitem['msg_analyzed'] = message['msg']
        eitem['msg'] = message['msg']
        eitem['rid'] = message['rid']
        eitem['msg_id'] = message['_id']
        # parent exists in case message is a reply
        eitem['msg_parent'] = message.get('parent', None)

        author = message.get('u', None)
        if author:
            eitem['user_id'] = author.get('_id', None)
            eitem['user_name'] = author.get('name', None)
            eitem['user_username'] = author.get('username', None)

        eitem['is_edited'] = 0
        editor = message.get('editedBy', None)
        if editor:
            eitem['edited_at'] = str_to_datetime(message['editedAt']).isoformat()
            eitem['edited_by_username'] = editor.get('username', None)
            eitem['edited_by_user_id'] = editor.get('_id', None)
            eitem['is_edited'] = 1

        file = message.get('file', None)
        if file:
            eitem['file_id'] = file.get('_id', None)
            eitem['file_name'] = file.get('name', None)
            eitem['file_type'] = file.get('type', None)

        eitem['replies'] = len(message['replies']) if message.get('replies', None) else 0

        eitem['total_reactions'] = 0
        reactions = message.get('reactions', None)
        if reactions:
            reaction_types, total_reactions = self.__get_reactions(reactions)
            eitem.update({'reactions': reaction_types})
            eitem['total_reactions'] = total_reactions

        eitem['total_mentions'] = 0
        mentions = message.get('mentions', None)
        if mentions:
            eitem['mentions'] = self.__get_mentions(mentions)
            eitem['total_mentions'] = len(mentions)

        channel_info = message.get('channel_info', None)
        if channel_info:
            eitem.update(self.__get_channel_info(channel_info))

        eitem['total_urls'] = 0
        urls = message.get('urls', None)
        if urls:
            urls = [{'url': url['url']} for url in urls]
            eitem['message_urls'] = urls
            eitem['total_urls'] = len(urls)

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "message"))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem

    def __get_reactions(self, reactions):
        """Enrich reactions for the message"""

        reaction_types = []
        total_reactions = 0
        for reaction_type in reactions:
            reaction_data = reactions[reaction_type]
            usernames = reaction_data.get('usernames', [])
            names = reaction_data.get('names', [])
            reaction_type = {
                "type": reaction_type,
                "username": usernames,
                "names": names,
                "count": len(usernames)
            }
            total_reactions += len(usernames)
            reaction_types.append(reaction_type)

        return reaction_types, total_reactions

    def __get_mentions(self, mentioned):
        """Enrich users mentioned in the message"""

        rich_mentions = []

        for usr in mentioned:
            rich_mention = {
                'username': usr.get('username', None),
                'id': usr.get('_id', None),
                'name': usr.get('name', None)
            }
            rich_mentions.append(rich_mention)

        return rich_mentions

    def __get_channel_info(self, channel):
        """Enrich channel info of the message"""

        rich_channel = {
            'channel_id': channel['_id'],
            'channel_updated_at': str_to_datetime(channel['_updatedAt']).isoformat(),
            'channel_num_messages': channel.get('msgs', None),
            'channel_name': channel.get('name', ''),
            'channel_num_users': channel.get('usersCount', None),
            'channel_topic': channel.get('topic', ''),
            'avatar': ''
        }
        if 'lastMessage' in channel and channel['lastMessage']:
            rich_channel['avatar'] = channel['lastMessage']['avatar']

        return rich_channel
