# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2020 Bitergia
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
#   Quan Zhou <quan@bitergia.com>
#

import logging

from ..elastic_mapping import Mapping as BaseMapping
from .enrich import Enrich, metadata
from .utils import get_time_diff_days

from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)


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
               "main_description_analyzed": {
                    "type": "text",
                    "index": true
               },
               "summary_analyzed": {
                    "type": "text",
                    "index": true
               }
           }
        }"""

        return {"items": mapping}


class BugzillaRESTEnrich(Enrich):

    mapping = Mapping
    roles = ['assigned_to_detail', 'qa_contact_detail', 'creator_detail']

    def get_field_author(self):
        return 'creator_detail'

    def get_project_repository(self, eitem):
        return eitem['origin']

    def get_identities(self, item):
        """ Return the identities from an item """

        for rol in self.roles:
            if rol in item['data']:
                yield self.get_sh_identity(item["data"][rol])

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is used
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]

        identity['username'] = user['name'].split("@")[0] if user.get('name', None) else None
        identity['email'] = user.get('email', None)
        identity['name'] = user.get('real_name', None)
        return identity

    @metadata
    def get_rich_item(self, item):

        if 'id' not in item['data']:
            logger.warning("[bugzillarest] Dropped bug without bug_id {}".format(item))
            return None

        eitem = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)

        # The real data
        issue = item['data']

        if "assigned_to_detail" in issue and "real_name" in issue["assigned_to_detail"]:
            eitem["assigned_to"] = issue["assigned_to_detail"]["real_name"]

        if "creator_detail" in issue and "real_name" in issue["creator_detail"]:
            eitem["creator"] = issue["creator_detail"]["real_name"]

        eitem["id"] = issue['id']
        eitem["status"] = issue['status']
        if "summary" in issue:
            eitem["summary"] = issue['summary'][:self.KEYWORD_MAX_LENGTH]
            # Share the name field with bugzilla and share the panel
            eitem["main_description"] = eitem["summary"]

            eitem["summary_analyzed"] = issue['summary']
            eitem["main_description_analyzed"] = issue['summary']

        # Component and product
        eitem["component"] = issue['component']
        eitem["product"] = issue['product']

        # Fix dates
        date_ts = str_to_datetime(issue['creation_time'])
        eitem['creation_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')
        date_ts = str_to_datetime(issue['last_change_time'])
        eitem['changeddate_date'] = date_ts.isoformat()
        eitem['delta_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')

        # Add extra JSON fields used in Kibana (enriched fields)
        eitem['comments'] = 0
        eitem['number_of_comments'] = 0
        eitem['time_to_last_update_days'] = None
        eitem['url'] = None

        # Add the field to know if the ticket is open
        eitem['is_open'] = issue.get('is_open', None)

        if 'long_desc' in issue:
            eitem['number_of_comments'] = len(issue['long_desc'])
        if 'comments' in issue:
            eitem['comments'] = len(issue['comments'])
        eitem['url'] = item['origin'] + "/show_bug.cgi?id=" + str(issue['id'])
        eitem['time_to_last_update_days'] = \
            get_time_diff_days(eitem['creation_ts'], eitem['delta_ts'])

        eitem['timeopen_days'] = get_time_diff_days(eitem['creation_ts'], datetime_utcnow().replace(tzinfo=None))
        if 'is_open' in issue and not issue['is_open']:
            eitem['timeopen_days'] = eitem['time_to_last_update_days']

        eitem['changes'] = 0
        for history in issue['history']:
            if 'changes' in history:
                eitem['changes'] += len(history['changes'])

        if issue['whiteboard'] != "":
            eitem['whiteboard'] = issue['whiteboard']

        if self.sortinghat:
            eitem.update(self.get_item_sh(item, self.roles))
            # To reuse the name of the fields in Bugzilla and share the panel
            eitem['assigned_to_org_name'] = eitem['assigned_to_detail_org_name']
            eitem['assigned_to_uuid'] = eitem['assigned_to_detail_uuid']

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(issue['creation_time'], "bugrest"))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem
