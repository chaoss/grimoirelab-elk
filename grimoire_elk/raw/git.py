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
from ..identities.git import GitIdentities
from ..enriched.utils import anonymize_url


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        Ensure data.message is string, since it can be very large

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = '''
         {
            "dynamic":true,
            "properties": {
                "data": {
                    "properties": {
                        "message": {
                            "type": "text",
                            "index": true
                        }
                    }
                }
            }
        }
        '''

        return {"items": mapping}


class GitOcean(ElasticOcean):
    """Git Ocean feeder"""

    mapping = Mapping
    identities = GitIdentities

    def _fix_item(self, item):
        item['origin'] = anonymize_url(item['origin'])
        item['tag'] = anonymize_url(item['tag'])

    @classmethod
    def get_perceval_params_from_url(cls, url):
        params = []
        tokens = url.split(' ', 1)  # Just split the URL not the filter
        url = tokens[0]
        params.append(url)

        return params
