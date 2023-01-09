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

import re
from .elastic import ElasticOcean
from ..elastic_mapping import Mapping as BaseMapping


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        Non dynamic discovery of type for:
            * data.attachments.ts

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = '''
            {
                "dynamic":true,
                "properties": {
                    "data": {
                        "properties": {
                            "attachments": {
                                "dynamic":false,
                                "properties": {}
                            },
                            "channel_info": {
                                "properties": {
                                    "latest": {
                                        "dynamic": false,
                                        "properties": {}
                                    }
                                }
                            },
                            "props": {
                                "dynamic":false,
                                "properties": {
                                    "meeting_id": {
                                        "type": "text",
                                        "index": true
                                    }
                                }
                            },
                            "root": {
                               "dynamic":false,
                                "properties": {}
                            }
                        }
                    }
                }
            }
            '''

        return {"items": mapping}


class MattermostOcean(ElasticOcean):
    """Mattermost Ocean feeder"""

    mapping = Mapping
    URL_REGEX = re.compile('^'  # Pattern starts from the begining of the string

                           # Both legacy and standard URLs start with the base URL
                           r'(?P<base>https?://[\w\.\-]+\.[\w\-]+)'

                           # Now match one of the types of URLs
                           r'(?:'

                           # Pattern for a standard URL
                           r'(/(?P<team>[a-z\-_\d]+)/channels/(?P<channel>[a-z\-_\d]+)/?)'

                           # OR
                           r'|'

                           # Pattern for a legacy URL
                           r'(/? (?P<channel_id>[a-z\d]{26}))'

                           r')$')  # Pattern ends at the end of the string

    @classmethod
    def get_perceval_params_from_url(cls, url):
        """ Get the perceval params given a mattermost URL

        Mattermost URLs can be passed in two formats:
        - Legacy, which looks like <base_url> <channel_id>
        - Standard, which is just the URL for a specific channel.

        An example Legacy "URL" looks like `https://my.mattermost.host/
        993bte1an3dyjmqdgxsr8jr5kh`

        An example Standard URL looks like
        `https://my.mattermost.host/my_team_name/channels/my_channel_name`
        """
        params = []

        # Match the provided URL against the validator/parser regex
        data = cls.URL_REGEX.match(url.lower())

        # If there was no match, the URL was formatted wrong
        if data is None:
            raise RuntimeError(f"Couldn't parse mattermost URL ({url}), unknown format\n"
                               "URLs must be either a url like "
                               "'https://my.mattermost.host/my_team_name/channels/my_channel_name' "
                               "or just a base URL and a channel ID, such as "
                               "'https://my.mattermost.host/ 993bte1an3dyjmqdgxsr8jr5kh'")

        # The `base` group should be matched for both URLs, and is the 1st parameter
        url = data.group('base')
        params.append(url)

        # Extract the other groups
        team = data.group('team')
        channel = data.group('channel')
        channel_id = data.group('channel_id')

        # Check if this is a Standard URL by the presence of the `team` group
        if team is not None:
            params.append(channel)
            params.append(team)
        else:
            params.append(channel_id)

        return params
