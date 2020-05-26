# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2020 Bitergia
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
                "text_analyzed": {
                  "type": "text",
                  "fielddata": true,
                  "index": true
                }
           }
        } """

        return {"items": mapping}


class RocketChatEnrich(Enrich):
    mapping = Mapping

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

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

        eitem['text_analyzed'] = message['msg']
        eitem['msg'] = message['msg']
        eitem['rid'] = message['rid']
        eitem['id'] = message['_id']
        # parent exists in case message is a reply
        eitem['parent'] = message.get('parent', None)

        if 'u' in message and message['u']:
            author = message['u']
            eitem['user_id'] = author.get('_id', None)
            eitem['user_name'] = author.get('name', None)
            eitem['user_username'] = author.get('username', None)

        if 'editedBy' in message and message['editedBy']:
            eitem['edited_at'] = str_to_datetime(message['editedAt']).isoformat()
            editor = message['editedBy']
            eitem['edited_by_username'] = editor.get('username', None)
            eitem['edited_by_user_id'] = editor.get('_id', None)

        if 'file' in message and message['file']:
            eitem['file_id'] = message['file'].get('_id', None)
            eitem['file_name'] = message['file'].get('name', None)
            eitem['file_type'] = message['file'].get('type', None)

        if 'replies' in message and message['replies']:
            eitem['replies'] = message['replies']

        if 'reactions' in message and message['reactions']:
            eitem.update(self.__get_reactions(message))

        if 'mentions' in message and message['mentions']:
            eitem['mentions'] = self.__get_mentions(message['mentions'])

        if 'channel_info' in message and message['channel_info']:
            eitem.update(self.__get_channel_info(message['channel_info']))

        if 'urls' in message and message['urls']:
            eitem['message_urls'] = self.__get_urls(message['urls'])

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem

    def __get_reactions(self, item):
        """Enrich reactions for the message"""

        reactions = {}

        item_reactions = item.get('reactions', {})
        for reaction in item_reactions:
            reactions['reaction_{}'.format(reaction)] = item_reactions[reaction]

        return reactions

    def __get_mentions(self, mentioned):
        """Enrich users mentioned in the message"""

        rich_mentions = []

        for usr in mentioned:
            if '_id' in usr.keys():
                rich_mentions.append({'username': usr['username'], 'id': usr['_id'],
                                      'name': usr['name']})

        return rich_mentions

    def __get_channel_info(self, channel):
        """Enrich channel info of the message"""

        rich_channel = {'channel_id': channel['_id'],
                        'channel_updated_at': str_to_datetime(channel['_updatedAt']).isoformat(),
                        'channel_num_messages': channel['msgs'],
                        'channel_name': channel['name'],
                        'channel_num_users': channel['usersCount'],
                        }
        if 'lastMessage' in channel and channel['lastMessage']:
            rich_channel['channel_last_message_id'] = channel['lastMessage']['_id']
            rich_channel['channel_last_message'] = channel['lastMessage']['msg']

        return rich_channel

    def __get_urls(self, urls):
        """Enrich urls mentioned in the message"""

        rich_urls = []
        for url in urls:
            rich_url = {}
            if 'meta' in url:
                rich_url['url_metadata_description'] = url['meta'].get('description', None)
                rich_url['url_metadata_page_title'] = url['meta'].get('pageTitle', None)
            rich_url['url'] = url['url']

            rich_urls.append(rich_url)

        return rich_urls
