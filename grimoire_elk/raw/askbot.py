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

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = '''
         {
            "dynamic":true,
                "properties": {
                    "data": {
                        "properties": {
                            "answers": {
                                "dynamic":false,
                                "properties": {}
                            },
                            "author": {
                                "dynamic":false,
                                "properties": {}
                            },
                            "comments": {
                                "dynamic":false,
                                "properties": {}
                            },
                            "summary": {
                                "type": "text",
                                "index": true
                            }
                        }
                    }
                }
        }
        '''

        return {"items": mapping}


class AskbotOcean(ElasticOcean):
    """Askbot Ocean feeder"""

    mapping = Mapping

    def _fix_item(self, item):
        # item["ocean-unique-id"] = str(item["data"]["id"])+"_"+item['origin']
        item["ocean-unique-id"] = item["uuid"]

    @classmethod
    def get_p2o_params_from_url(cls, url):
        # askbot could include in the URL a  filters-raw-prefix T1721
        # "https://ask.openstack.org filter-raw=data.tags:rdo"
        params = {}

        tokens = url.split(' ', 1)  # Just split the URL not the filter
        params['url'] = tokens[0]

        if len(tokens) > 1:
            filter_raw = tokens[1].split(" ", 1)[1]
            # Create a filters array
            params['filter-raw'] = filter_raw

        return params
