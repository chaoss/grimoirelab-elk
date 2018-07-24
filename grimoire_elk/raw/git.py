# -*- coding: utf-8 -*-
#
# Git Ocean feeder
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

    @classmethod
    def get_p2o_params_from_url(cls, url):
        # Git could include in the URL a  filters-raw-prefix T1722
        # https://github.com/VizGrimoire/GrimoireLib --filters-raw-prefix \
        #  data.files.file:grimoirelib_alch data.files.file:README.md
        params = {}

        tokens = url.split(' ', 1)  # Just split the URL not the filter
        params['url'] = tokens[0]

        if len(tokens) > 1:
            f = tokens[1].split(" ", 1)[1]
            # Create a filters array
            params['filters-raw-prefix'] = f.split(" ")

        return params

    @classmethod
    def get_perceval_params_from_url(cls, url):
        params = []
        tokens = url.split(' ', 1)  # Just split the URL not the filter
        url = tokens[0]
        params.append(url)

        return params
