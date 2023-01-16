# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2023 Bitergia
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
#   Quan Zhou <quan@bitergia.com>
#


import logging

from grimoirelab_toolkit.datetime import str_to_datetime

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping


logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns: dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
                "text_analyzed": {
                  "type": "text",
                  "fielddata": true,
                  "index": true
                }
           }
        } """

        return {"items": mapping}


class WeblateEnrich(Enrich):

    mapping = Mapping

    def __init__(self, db_sortinghat=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', db_path=None,
                 db_port=None, db_ssl=False):
        super().__init__(db_sortinghat=db_sortinghat, json_projects_map=json_projects_map,
                         db_user=db_user, db_password=db_password, db_host=db_host,
                         db_port=db_port, db_path=db_path, db_ssl=db_ssl)

        self.studies = []
        self.studies.append(self.enrich_demography)

    def get_field_author(self):
        return "author_data"

    def get_sh_identity(self, item, identity_field=None):
        # email not available for gitter
        identity = {
            'username': None,
            'name': None,
            'email': None
        }

        author = item['data'][self.get_field_author()] \
            if self.get_field_author() in item['data'] else None
        if not author:
            return identity

        identity['username'] = author.get('username', None)
        identity['name'] = author.get('full_name', None)
        identity['email'] = author.get('email', None)

        return identity

    def get_identities(self, item):
        """ Return the identities from an item """

        identity = self.get_sh_identity(item)
        yield identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        return repo

    @metadata
    def get_rich_item(self, item):

        eitem = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)

        change = item['data']

        change_timestamp = str_to_datetime(eitem['metadata__updated_on'])
        eitem['tz'] = int(change_timestamp.strftime("%H"))
        eitem['action'] = change['action']
        eitem['action_name'] = change['action_name']
        eitem['glossary_term'] = change.get('glossary_term', None)
        eitem['change_id'] = change['id']
        eitem['target'] = change['target']
        eitem['change_api_url'] = change['url']

        eitem['author_api_url'] = change['author']
        author_data = change.get('author_data', None)
        if author_data:
            for data in author_data:
                eitem['author_' + data] = author_data[data]

        eitem['user_api_url'] = change.get('user', None)
        if eitem['user_api_url']:
            username = eitem['user_api_url']
            eitem['user_name'] = username.split('api/users/')[1].split('/')[0] if username else None

        user_data = change.get('user_data', None)
        if user_data:
            for data in user_data:
                eitem['user_' + data] = user_data[data]

        eitem['unit_api_url'] = change['unit']
        unit_data = change.get('unit_data', None)
        if unit_data:
            for data in unit_data:
                if 'has_' in data:
                    eitem['unit_' + data] = 1 if unit_data[data] else 0
                else:
                    eitem['unit_' + data] = unit_data[data]

        eitem['component_api_url'] = change.get('component', None)
        if eitem['component_api_url']:
            component = eitem['component_api_url']
            # component = <source>/api/components/<project>/<component>
            project_name = component.split('api/components/')[1].split('/')[0] if component else None
            component_name = component.split('api/components/')[1].split('/')[1] if component else None
            eitem['project_name'] = project_name
            eitem['component_name'] = component_name

        eitem['translation_api_url'] = change.get('translation', None)
        if eitem['translation_api_url']:
            translation = eitem['translation_api_url']
            # component = <source>/api/translations/<project>/<component>/<translation>
            project_name = translation.split('api/translations/')[1].split('/')[0] if translation else None
            component_name = translation.split('api/translations/')[1].split('/')[1] if translation else None
            translation_name = translation.split('api/translations/')[1].split('/')[2] if translation else None
            eitem['project_name'] = project_name
            eitem['component_name'] = component_name
            eitem['translation_name'] = translation_name

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "message"))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem

    def enrich_demography(self, ocean_backend, enrich_backend, alias, date_field="grimoire_creation_date",
                          author_field="author_uuid"):

        super().enrich_demography(ocean_backend, enrich_backend, alias, date_field, author_field=author_field)
