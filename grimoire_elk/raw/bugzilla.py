# -*- coding: utf-8 -*-
#
# Bugzilla Ocean feeder
#
# Copyright (C) 2016 Bitergia
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

'''Bugzilla Ocean feeder'''

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
                            "long_desc": {
                                "dynamic": false,
                                "properties": {}
                            },
                            "short_desc": {
                                "dynamic": false,
                                "properties": {
                                    "__text__": {
                                        "type": "text",
                                        "index": true
                                    }
                                }
                            },
                            "activity": {
                                "dynamic": false,
                                "properties": {}
                            }
                        }
                    }
                }
        }
        '''

        return {"items": mapping}


class BugzillaOcean(ElasticOcean):
    """Bugzilla Ocean feeder"""

    mapping = Mapping

    def _fix_item(self, item):
        # Could be used for filtering
        product = item['data']['product'][0]['__text__']
        item['product'] = product

    @classmethod
    def get_p2o_params_from_url(cls, url):
        # Bugzilla could include in the URL a filter-raw T1720
        # https://bugzilla.redhat.com/ filter-raw=product:OpenShift Origin
        params = {}

        tokens = url.split(' ', 1)  # Just split the URL not the filter
        params['url'] = tokens[0]

        if len(tokens) > 1:
            f = tokens[1].split("=")[1]
            params['filter-raw'] = f

        return params
