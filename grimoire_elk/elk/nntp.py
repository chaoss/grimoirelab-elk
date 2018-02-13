#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
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

from ..elastic_mapping import Mapping as BaseMapping
from .mbox import MBoxEnrich


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        geopoints type is not created in dynamic mapping

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        if es_major != 2:
            mapping = """
            {
                "properties": {
                    "Subject": {
                        "type": "text",
                        "index": true
                    },
                    "Subject_analyzed": {
                        "type": "text",
                        "index": true
                    }
                }
            }
            """
        else:
            mapping = """
            {
                "properties": {
                    "Subject": {
                        "type": "string",
                        "index": "analyzed"
                    },
                    "Subject_analyzed": {
                        "type": "string",
                        "index": "analyzed"
                    }
                }
            }
            """

        return {"items": mapping}


class NNTPEnrich(MBoxEnrich):

    mapping = Mapping

    def get_project_repository(self, eitem):
        # origin: news.mozilla.org-mozilla.community.drumbeat
        # projects repo: news.mozilla.org mozilla.community.drumbeat

        return eitem['origin'].replace("-", " ")
