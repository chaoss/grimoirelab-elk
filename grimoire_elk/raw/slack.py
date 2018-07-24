# -*- coding: utf-8 -*-
#
# StackExchange Ocean feeder
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


class SlackOcean(ElasticOcean):
    """Slack Ocean feeder"""

    mapping = Mapping

    @classmethod
    def get_arthur_params_from_url(cls, url):
        # The URL is directly the slack group

        params = {"channel": url}

        return params
