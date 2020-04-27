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
#   Valerio Cosentino <valcos@bitergia.com>
#

import logging

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping
from .sortinghat_gelk import SortingHat


logger = logging.getLogger(__name__)

GITHUB_BACKEND = "github"


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
                "date": {
                    "type": "keyword"
                }
            }
        }
        '''

        return {"items": mapping}


class FinosMeetingsEnrich(Enrich):

    mapping = Mapping

    def get_sh_identity(self, item, identity_field=None):
        identity = {}
        for field in ['name', 'email', 'username']:
            identity[field] = None

        user = item['data'] if isinstance(item, dict) and 'data' in item else item

        if 'name' in user and user['name']:
            identity['name'] = user['name']
        if 'email' in user and user['email']:
            identity['email'] = user['email']
        if 'githubid' in user and user['githubid']:
            identity['username'] = user['githubid']

        return identity

    def get_field_author(self):
        return 'email'

    def add_sh_github_identity(self, github_login):
        identity = {
            'username': github_login,
            'email': None,
            'name': None
        }

        SortingHat.add_identity(self.sh_db, identity, GITHUB_BACKEND)

    def get_identities(self, item):
        """ Return the identities from an item """

        data = item['data']
        identity = self.get_sh_identity(data)

        if identity['username']:
            self.add_sh_github_identity(identity['username'])

        yield identity

    def has_identities(self):
        """ Return whether the enriched items contains identities """

        return True

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)

        entry = item['data']

        for e in entry.keys():
            if e == '_id_columns':
                continue
            elif e == 'org':
                eitem['csv_org'] = entry[e]
            else:
                eitem[e] = entry[e]

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "entry"))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem

    def get_item_project(self, eitem):

        project_info = {
            "project": eitem['cm_title'],
            "project_1": eitem['cm_title']
        }

        return project_info
