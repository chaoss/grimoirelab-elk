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
                "message_analyzed": {
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

    def __init__(self, db_sortinghat=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', db_path=None,
                 db_port=None, db_ssl=False):
        super().__init__(db_sortinghat=db_sortinghat, json_projects_map=json_projects_map,
                         db_user=db_user, db_password=db_password, db_host=db_host,
                         db_port=db_port, db_path=db_path, db_ssl=db_ssl)

        self.studies = []
        self.studies.append(self.enrich_demography)

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
        eitem['message_id'] = message['id']

        # data fields to copy
        copy_fields = ["message", "type", "reply_count", "hashtags", "is_pinned"]
        for f in copy_fields:
            if f in message:
                eitem[f] = message[f]
            else:
                eitem[f] = None

        eitem['message_analyzed'] = eitem['message']

        eitem['reaction_count'] = 0
        eitem['file_count'] = 0
        if 'metadata' in message:
            if 'reactions' in message['metadata']:
                eitem['reaction_count'] = len(message['metadata']['reactions'])
                eitem['reactions'] = self.__get_reactions(message['metadata']['reactions'])
            if 'files' in message['metadata']:
                eitem['file_count'] = len(message['metadata']['files'])
                eitem['files'] = self.__get_files(message['metadata']['files'])

        if 'user_data' in message:
            user_data = message['user_data']
            eitem['roles'] = user_data['roles']
            eitem['position'] = user_data['position']
            eitem['team_id'] = None  # not exists in Mattermost
            timezone = user_data.get('timezone', None)
            if timezone:
                if timezone['useAutomaticTimezone']:
                    eitem['tz'] = timezone['automaticTimezone']
                else:
                    eitem['tz'] = timezone['manualTimezone']
        if 'channel_data' in message:
            channel_data = message['channel_data']
            eitem['channel_name'] = channel_data['display_name']
            eitem['channel_id'] = channel_data['id']
            eitem['channel_create_at'] = unixtime_to_datetime(channel_data['create_at'] / 1000).isoformat()
            eitem['channel_delete_at'] = None if channel_data['delete_at'] == 0 else \
                unixtime_to_datetime(channel_data['delete_at'] / 1000).isoformat()
            eitem['channel_update_at'] = unixtime_to_datetime(channel_data['update_at'] / 1000).isoformat()
            eitem['channel_member_count'] = None
            eitem['channel_message_count'] = channel_data['total_msg_count']
            eitem['channel_team_id'] = channel_data['team_id']

        eitem['is_reply'] = False
        eitem['parent_id'] = message.get('parent_id', message.get('root_id', None))
        if eitem['parent_id']:
            eitem['is_reply'] = True

        eitem = self.__convert_booleans(eitem)

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "message"))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem

    def enrich_demography(self, ocean_backend, enrich_backend, alias, date_field="grimoire_creation_date",
                          author_field="author_uuid"):
        super().enrich_demography(ocean_backend, enrich_backend, alias, date_field, author_field=author_field)

    @staticmethod
    def __get_files(message):
        files = []
        for file in message:
            new_file = {
                'file_user_id': file['user_id'],
                'file_post_id': file['post_id'],
                'file_create_at': unixtime_to_datetime(file['create_at'] / 1000).isoformat(),
                'file_update_at': unixtime_to_datetime(file['update_at'] / 1000).isoformat(),
                'file_delete_at': None if file['delete_at'] == 0 else unixtime_to_datetime(
                    file['delete_at'] / 1000).isoformat(),
                'file_name': file['name'],
                'file_extension': file['extension'],
                'file_size': file['size'],
                'file_type': file['mime_type'],
                'file_mini_preview': file['mini_preview']
            }
            files.append(new_file)
        return files

    @staticmethod
    def __get_reactions(message):
        reactions = []
        for reaction in message:
            new_reaction = {
                'reaction_user_id': reaction['user_id'],
                'reaction_post_id': reaction['post_id'],
                'reaction_emoji_name': reaction['emoji_name'],
                'reaction_create_at': unixtime_to_datetime(reaction['create_at'] / 1000).isoformat(),
                'reaction_update_at': unixtime_to_datetime(reaction['update_at'] / 1000).isoformat(),
                'reaction_delete_at': None if reaction['delete_at'] == 0 else unixtime_to_datetime(
                    reaction['delete_at'] / 1000).isoformat()
            }
            reactions.append(new_reaction)
        return reactions

    def __convert_booleans(self, eitem):
        """ Convert True/False to 1/0 for better kibana processing """

        for field in eitem.keys():
            if isinstance(eitem[field], bool):
                if eitem[field]:
                    eitem[field] = 1
                else:
                    eitem[field] = 0
        return eitem  # not needed becasue we are modifying directly the dict
