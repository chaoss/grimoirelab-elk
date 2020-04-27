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

import logging

from grimoirelab_toolkit.datetime import datetime_utcnow

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping

from .utils import get_time_diff_days


logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
                "description_analyzed": {
                    "type": "text",
                    "index": true
                },
                "subject_analyzed": {
                    "type": "text",
                    "index": true
                }
           }
        } """

        return {"items": mapping}


class RedmineEnrich(Enrich):

    mapping = Mapping

    roles = ['assigned_to', 'author']

    def get_identities(self, item):
        """ Return the identities from an item """

        data = item['data']
        if 'assigned_to' in data:
            user = self.get_sh_identity(data, 'assigned_to')
            yield user
        author = self.get_sh_identity(data, 'author')
        yield author
        # TODO: identities in journals not added yet

    def get_field_author(self):
        return "author"

    def get_sh_identity(self, data, rol):
        identity = {}

        identity['email'] = None
        identity['username'] = None
        identity['name'] = None

        if 'data' in data:
            # data is the full raw item from perceval
            data = data['data']

        if rol + "_data" in data:
            if 'mail' in data[rol + "_data"]:
                identity['email'] = data[rol + "_data"]['mail']
        if rol in data:
            identity['username'] = data[rol]['id']
            identity['name'] = data[rol]['name']

        return identity

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)
        # The real data
        ticket = item['data']

        # data fields to copy
        copy_fields = ["subject", "description", "updated_on",
                       "due_date", "estimated_hours", "done_ratio",
                       "id", "spent_hours", "start_date", "subject",
                       "last_update"]
        for f in copy_fields:
            if f in ticket:
                eitem[f] = ticket[f]
            else:
                eitem[f] = None
        eitem['subject'] = eitem['subject'][:self.KEYWORD_MAX_LENGTH]
        eitem['subject_analyzed'] = eitem['subject']

        eitem['description_analyzed'] = eitem['description']
        if eitem['description']:
            eitem['description'] = eitem['description'][:self.KEYWORD_MAX_LENGTH]

        # Fields which names are translated
        map_fields = {"due_date": "estimated_closing_date",
                      "created_on": "creation_date",
                      "closed_on": "closing_date"}
        for fn in map_fields:
            if fn in ticket:
                eitem[map_fields[fn]] = ticket[fn]
        # Common format
        common = ['category', 'fixed_version', 'priority', 'project', 'status',
                  'tracker', 'author']
        for f in common:
            if f in ticket:
                eitem[f + '_id'] = ticket[f]['id']
                eitem[f + '_name'] = ticket[f]['name']

        len_fields = ['attachments', 'changesets', 'journals', 'relations']
        for f in len_fields:
            if f in ticket:
                eitem[f] = len(ticket[f])

        if 'parent' in ticket:
            eitem['parent_id'] = ticket['parent']['id']

        eitem['url'] = eitem['origin'] + "/issues/" + str(eitem['id'])

        # Time to
        if "closing_date" in eitem:
            eitem['timeopen_days'] = \
                get_time_diff_days(eitem['creation_date'], eitem['closing_date'])
            eitem['timeworking_days'] = \
                get_time_diff_days(eitem['start_date'], eitem['closing_date'])
        else:
            eitem['timeopen_days'] = \
                get_time_diff_days(eitem['creation_date'], datetime_utcnow().replace(tzinfo=None))
            eitem['timeworking_days'] = \
                get_time_diff_days(eitem['start_date'], datetime_utcnow().replace(tzinfo=None))

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "job"))

        if self.sortinghat:
            eitem.update(self.get_item_sh(item, self.roles))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem
