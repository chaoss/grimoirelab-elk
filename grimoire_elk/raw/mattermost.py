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

    @classmethod
    def get_perceval_params_from_url(cls, url):
        """ Get the perceval params given a URL for the data source """
        params = []

        data = url.split()
        url = data[0]
        channel = data[1]
        params.append(url)
        params.append(channel)
        return params
