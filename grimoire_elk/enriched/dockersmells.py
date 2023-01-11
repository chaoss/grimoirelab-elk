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
#   Valerio Cosentino <valcos@bitergia.com>
#

import logging

from .enrich import (Enrich,
                     metadata)
from .utils import fix_field_date
from ..elastic_mapping import Mapping as BaseMapping
from perceval import backend

MAX_SIZE_BULK_ENRICHED_ITEMS = 200

logger = logging.getLogger(__name__)


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
                "id": {
                    "type": "keyword"
                },
                "commit_sha": {
                    "type": "keyword"
                },
                "file_path" : {
                    "type" : "keyword"
                },
                "smell" : {
                    "type" : "keyword"
                },
                "origin" : {
                    "type" : "keyword"
                }
            }
        }
        '''

        return {"items": mapping}


class Dockersmells(Enrich):

    def __init__(self, db_sortinghat=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', db_path=None,
                 db_port=None, db_ssl=False):
        super().__init__(db_sortinghat=db_sortinghat, json_projects_map=json_projects_map,
                         db_user=db_user, db_password=db_password, db_host=db_host,
                         db_port=db_port, db_path=db_path, db_ssl=db_ssl)

        self.studies = []

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        return identities

    def has_identities(self):
        """ Return whether the enriched items contains identities """

        return False

    def get_field_unique_id(self):
        return "id"

    @metadata
    def get_rich_item(self, item, file_path, smell):
        commit = item['data']
        smell_info = smell.split()
        smell_line = smell_info[1]
        smell_code = smell_info[2]
        smell_details = ' '.join(smell_info[3:])
        eitem = {
            'file_path': file_path,
            'smell_line': smell_line,
            'smell_code': smell_code,
            'smell': smell_details,
            'commit_sha': commit['commit'],
            'origin': item['origin'],
            'author_date': fix_field_date(commit['AuthorDate']),
            'commit_date': fix_field_date(commit['CommitDate'])
        }

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        # uuid
        uuid_sha1 = backend.uuid(eitem['file_path'], eitem['smell'])
        eitem['id'] = "{}_{}".format(eitem['commit_sha'], uuid_sha1)
        eitem.update(self.get_grimoire_fields(eitem["author_date"], "file"))
        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)

        return eitem

    def enrich_items(self, ocean_backend, events=False):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        for item in ocean_backend.fetch():
            analysis_data = item['data']['analysis']
            for file_path in analysis_data:
                for smell in analysis_data[file_path]['smells']:
                    eitem = self.get_rich_item(item, file_path, smell)
                    items_to_enrich.append(eitem)

            if len(items_to_enrich) < MAX_SIZE_BULK_ENRICHED_ITEMS:
                continue

            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())
            items_to_enrich = []

        if len(items_to_enrich) > 0:
            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())

        if num_items != ins_items:
            missing = num_items - ins_items
            logger.error("[dockersmells] {}/{} missing items".format(
                         missing, num_items))
        else:
            logger.info("[dockersmells] {} items inserted".format(
                        num_items))

        return num_items
