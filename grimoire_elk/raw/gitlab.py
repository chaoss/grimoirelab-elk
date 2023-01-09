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
#   Valerio Cosentino <valcos@bitergia.com>
#

from .elastic import ElasticOcean
from ..elastic_mapping import Mapping as BaseMapping
from ..identities.gitlab import GitlabIdentities


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
                            "head_pipeline": {
                                "dynamic":false,
                                "properties": {
                                    "yaml_errors": {
                                        "type": "text",
                                        "index": true
                                    }
                                }
                            },
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
    identities = GitlabIdentities

    @classmethod
    def get_perceval_params_from_url(cls, url):
        """ Get the perceval params given a URL for the data source """
        params = []

        tokens = url.split(' ')
        repo = tokens[0]

        # This removes the last two components from the URL (user & project) leaving only
        # the host and protocol
        host = '/'.join(repo.split('/')[:-2])
        if host != 'https://gitlab.com':
            params.extend(("--enterprise-url", host))

        owner = repo.split('/')[-2]
        repository = repo.split('/')[-1]

        params.append(owner)
        params.append(repository)

        if len(tokens) > 1:
            params.extend(tokens[1:])

        return params
