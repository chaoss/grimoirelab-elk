# -*- coding: utf-8 -*-
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
            * data.transaction: has string arrays and dicts arrays
        Specific type for:
            * data.fields.priority.subpriority (float)

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = '''
        {
            "dynamic":true,
            "properties": {
                "data": {
                    "properties": {
                        "transactions": {
                            "dynamic":false,
                            "properties": {}
                        },
                        "fields": {
                            "properties": {
                                "priority" : {
                                    "properties": {
                                        "subpriority" : {"type": "float"}
                                     }
                                 },
                                 "description": {
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


class PhabricatorOcean(ElasticOcean):
    """Phabricator Ocean feeder"""

    mapping = Mapping

    def _fix_item(self, item):
        # fields cannot contain dots in ES 2.2. For consistency reason, this fix is applied also
        # to more recent versions of ES

        for field in item["data"]["fields"]:
            if '.' in field:
                undotted_field = field.replace('.', '_')
                item["data"]["fields"][undotted_field] = item["data"]["fields"][field]
                item["data"]['fields'].pop(field)
