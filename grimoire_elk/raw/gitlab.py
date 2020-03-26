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
#   Valerio Cosentino <valcos@bitergia.com>
#

import hashlib

from .elastic import ElasticOcean
from ..elastic_mapping import Mapping as BaseMapping


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = '''
         {
            "dynamic":true,
                "properties": {
                    "data": {
                        "properties": {
                            "notes_data": {
                                "dynamic":false,
                                "properties": {
                                    "body": {
                                        "type": "text",
                                        "index": true
                                    }
                                }
                            },
                            "description": {
                                "type": "text",
                                "index": true
                            },
                            "versions_data": {
                                "dynamic":false,
                                "properties": {}
                            }
                        }
                    }
                }
        }
        '''

        return {"items": mapping}


class GitLabOcean(ElasticOcean):
    """GitLab Ocean feeder"""

    mapping = Mapping

    @classmethod
    def get_perceval_params_from_url(cls, url):
        """ Get the perceval params given a URL for the data source """
        params = []

        tokens = url.split(' ')
        repo = tokens[0]

        owner = repo.split('/')[-2]
        repository = repo.split('/')[-1]

        params.append(owner)
        params.append(repository)

        if len(tokens) > 1:
            params.extend(tokens[1:])

        return params

    def _hash(self, name):
        sha1 = hashlib.sha1(name.encode('UTF-8', errors="surrogateescape"))
        return sha1.hexdigest()

    def _anonymize_item(self, item):
        """ Remove or hash the fields that contain personal information """
        category = item['category']

        item = item['data']

        if category == "issue":
            identity_types = ['author', 'assignee']
        elif category == "merge_request":
            identity_types = ['author', 'merged_by']
        else:
            identity_types = []

        for identity in identity_types:
            if identity not in item:
                continue
            if not item[identity]:
                continue

            item[identity] = {
                'username': self._hash(item[identity]['username']),
                'name': self._hash(item[identity]['name']),
                'email': None,
                'organization': None,
                'location': None
            }
