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
#   Nitish Gupta <imnitish.ng@gmail.com>
#

import logging

from grimoirelab_toolkit.datetime import (datetime_utcnow)

from .utils import get_time_diff_days

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping

logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        geopoints type is not created in dynamic mapping

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
               "description": {
                    "type": "text",
                    "index": true
               }
           }
        }
        """
        return {"items": mapping}


class LaunchpadEnrich(Enrich):

    mapping = Mapping
    roles = ['assignee_data', 'owner_data']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

    def get_field_author(self):
        return "owner_data"

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_sh_identity(self, item, identity_field=None):
        """ Return a Sorting Hat identity using launchpad user data """

        identity = {
            'username': None,
            'name': None,
            'email': None
        }

        item = item['data']
        if isinstance(item, dict) and item[identity_field]:
            identity['username'] = item[identity_field]['name']
            identity['name'] = item[identity_field]['display_name']

        return identity

    def get_identities(self, item):
        """ Return the identities from an item """

        for rol in self.roles:
            if rol in item['data']:
                user = self.get_sh_identity(item, rol)
                yield user

    @metadata
    def get_rich_item(self, item):

        eitem = {}

        self.__copy_fields(self.RAW_FIELDS_COPY, item, eitem)

        # data fields to copy
        copy_fields = ["title", "web_link", "date_created", "date_incomplete", "is_complete",
                       "status", "bug_target_name", "importance", "date_triaged", "date_left_new"]

        self.__copy_fields(copy_fields, item['data'], eitem)

        if self.sortinghat:
            eitem.update(self.get_item_sh(item, self.roles))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.__get_rich_bugs(item['data']))

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "message"))

        return eitem

    def __copy_fields(self, copy_fields, source, target):

        for f in copy_fields:
            if f in source:
                target[f] = source[f]
            else:
                target[f] = None

    def __get_rich_bugs(self, data):
        """Create enriched data for bugs"""

        rich_bugtask = {}

        # Time to
        rich_bugtask["created_to_assigned"] = get_time_diff_days(data['date_created'], data['date_assigned'])
        rich_bugtask['assigned_to_closed'] = get_time_diff_days(data['date_assigned'], data['date_closed'])
        rich_bugtask["time_to_close_days"] = get_time_diff_days(data['date_created'], data['date_closed'])

        rich_bugtask['time_to_close_days'] = \
            get_time_diff_days(data['date_created'], data['date_closed'])

        if not data['date_closed'] and not data['is_complete']:
            rich_bugtask['time_still_open_days'] = \
                get_time_diff_days(data['date_created'], datetime_utcnow().replace(tzinfo=None))

        if not data['date_closed']:
            rich_bugtask['time_open_days'] = \
                get_time_diff_days(data['date_created'], datetime_utcnow().replace(tzinfo=None))
            if not data['is_complete']:
                rich_bugtask['time_still_open_days'] = rich_bugtask['time_open_days']
        else:
            rich_bugtask['time_open_days'] = rich_bugtask['time_to_close_days']

        if data['activity_data']:
            rich_bugtask['time_to_last_update_days'] = \
                get_time_diff_days(data['date_created'], data['activity_data'][-1]['datechanged'])

        rich_bugtask['reopened'] = 1 if data['date_left_closed'] else 0

        rich_bugtask['time_to_fix_commit'] = get_time_diff_days(data['date_created'], data['date_fix_committed'])

        rich_bugtask['time_worked_on'] = get_time_diff_days(data['date_in_progress'], data['date_fix_committed'])

        rich_bugtask['time_to_confirm'] = get_time_diff_days(data['date_created'], data['date_confirmed'])

        # Author and assignee data
        owner = data.get('owner_data', None)
        if owner is not None and owner:
            rich_bugtask['user_login'] = owner['name']
            rich_bugtask['user_name'] = owner['display_name']
            rich_bugtask['user_joined'] = owner['date_created']
            rich_bugtask['user_karma'] = owner['karma']
            rich_bugtask['user_time_zone'] = owner['time_zone']
        else:
            rich_bugtask['user_name'] = None
            rich_bugtask['user_realname'] = None
            rich_bugtask['user_joined'] = None
            rich_bugtask['user_karma'] = None
            rich_bugtask['user_time_zone'] = None

        assignee = data.get('assignee_data', None)
        if assignee and assignee is not None:
            assignee = data['assignee_data']
            rich_bugtask['assignee_login'] = assignee['name']
            rich_bugtask['assignee_name'] = assignee['display_name']
            rich_bugtask['assignee_joined'] = assignee['date_created']
            rich_bugtask['assignee_karma'] = assignee['karma']
            rich_bugtask['assignee_time_zone'] = assignee['time_zone']
        else:
            rich_bugtask['assignee_login'] = None
            rich_bugtask['assignee_name'] = None
            rich_bugtask['assignee_joined'] = None
            rich_bugtask['assignee_karma'] = None
            rich_bugtask['assignee_time_zone'] = None

        # Extract info related to bug
        rich_bugtask.update(self.__extract_bug_info(data['bug_data']))

        if len(data['activity_data']) > 1:
            rich_bugtask['time_to_first_activity'] = \
                get_time_diff_days(data['date_created'], data['activity_data'][1]['datechanged'])
        else:
            rich_bugtask['time_to_first_activity'] = None

        rich_bugtask['activity_count'] = len(data['activity_data'])

        return rich_bugtask

    def __extract_bug_info(self, bug_data):

        rich_bug_info = {}

        copy_fields = ["latest_patch_uploaded", "security_related", "private", "users_affected_count",
                       "title", "description", "tags", "date_last_updated", "message_count", "heat"]

        rich_bug_info['time_to_last_update_days'] = \
            get_time_diff_days(bug_data['date_created'], bug_data['date_last_updated'])

        rich_bug_info['bug_name'] = bug_data['name']
        rich_bug_info['bug_id'] = bug_data['id']

        self.__copy_fields(copy_fields, bug_data, rich_bug_info)

        return rich_bug_info
