# -*- coding: utf-8 -*-
#
# JIRA Ocean feeder
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
            * data.renderedFields.description

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = '''
         {
            "dynamic":true,
            "properties": {
                "data": {
                    "properties": {
                        "renderedFields": {
                            "dynamic":false,
                            "properties": {}
                        },
                        "operations": {
                            "dynamic":false,
                            "properties": {}
                        },
                        "fields": {
                            "dynamic":true,
                            "properties": {
                                "description": {
                                    "type": "text",
                                    "index": true
                                }
                            }
                        },
                        "changelog": {
                            "properties": {
                                "histories": {
                                    "dynamic":false,
                                    "properties": {}
                                }
                            }
                        }
                    }
                }
            }
        }
        '''

        return {"items": mapping}


class JiraOcean(ElasticOcean):
    """JIRA Ocean feeder"""

    mapping = Mapping

    @classmethod
    def get_arthur_params_from_url(cls, url):
        """ Get the arthur params given a URL for the data source """
        return {"url": url}

    def _fix_item(self, item):
        # Remove all custom fields to avoid the 1000 fields limit in ES

        if "fields" not in item["data"]:
            return

        fields = list(item["data"]["fields"].keys())
        for field in fields:
            if field.lower().startswith("customfield_"):
                item["data"]["fields"].pop(field)
