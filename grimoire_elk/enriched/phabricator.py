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
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import logging

from grimoirelab_toolkit.datetime import (unixtime_to_datetime)

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping

from .utils import get_time_diff_days


TASK_OPEN_STATUS = 'Open'
CLOSED_STATUSES = ['resolved', 'wontfix', 'invalid', 'duplicate', 'spite']


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
                "main_description_analyzed": {
                  "type": "text",
                  "index": true
                },
                "assigned_to_roles": {
                  "type": "text",
                  "fielddata": true,
                  "index": true
                },
                "tags_analyzed": {
                  "type": "text",
                  "fielddata": true,
                  "index": true
                }
           }
        } """

        return {"items": mapping}


class PhabricatorEnrich(Enrich):

    mapping = Mapping

    roles = ['authorData', 'ownerData']

    def __init__(self, db_sortinghat=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', db_path=None,
                 db_port=None, db_ssl=False, db_verify_ssl=True, db_tenant=None):
        super().__init__(db_sortinghat=db_sortinghat, json_projects_map=json_projects_map,
                         db_user=db_user, db_password=db_password, db_host=db_host,
                         db_port=db_port, db_path=db_path, db_ssl=db_ssl, db_verify_ssl=db_verify_ssl,
                         db_tenant=db_tenant)

        self.phab_ids_names = {}  # To convert from phab ids to phab names

    def get_field_event_unique_id(self):
        return "transactionID"

    def get_field_author(self):
        return "authorData"

    def get_identities(self, item):
        """ Return the identities from an item """

        if 'authorData' in item['data']['fields']:
            user = self.get_sh_identity(item['data']['fields']['authorData'])
            yield user

        if 'ownerData' in item['data']['fields']:
            user = self.get_sh_identity(item['data']['fields']['ownerData'])
            yield user

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        identity['email'] = None
        identity['username'] = None
        identity['name'] = None

        if item is None:
            return identity

        user = item
        if isinstance(item, dict) and 'data' in item:
            user = item['data']['fields'][identity_field]

        if user is None:
            return identity

        if 'userName' in user:
            identity['username'] = user['userName']
        if 'realName' in user:
            identity['name'] = user['realName']

        return identity

    def get_users_data(self, item):
        """ If user fields are inside the global item dict """
        if 'data' in item:
            users_data = item['data']['fields']
        else:
            # the item is directly the data (kitsune answer)
            users_data = item
        return users_data

    def get_rich_events(self, item):
        """
        In the events there are some common fields with the task. The name
        of the field must be the same in the task and in the event
        so we can filer using it in task and event at the same time.

        * Fields that don't change: the field does not change with the events
        in a task so the value is always the same in the events of a task.

        * Fields that change: the value of teh field changes with events
        """
        # To get values from the task
        eitem = self.get_rich_item(item)

        # Fields that don't change never
        task_fields_nochange = ['author_userName', 'creation_date', 'url', 'id', 'bug_id']

        # Follow changes in this fields
        task_fields_change = ['priority_value', 'status', 'assigned_to_userName', 'tags_custom_analyzed']
        task_change = {}
        for f in task_fields_change:
            task_change[f] = None
        task_change['status'] = TASK_OPEN_STATUS
        task_change['tags_custom_analyzed'] = eitem['tags_custom_analyzed']

        # Events are in transactions field (changes in fields)
        transactions = item['data']['transactions']

        if not transactions:
            return []

        for t in transactions:
            event = {}
            # Needed for incremental updates from the item
            event['metadata__updated_on'] = item['metadata__updated_on']
            event['origin'] = item['origin']
            # Real event data
            event['transactionID'] = t['transactionID']
            event['type'] = t['transactionType']
            event['username'] = None
            if 'authorData' in t and 'userName' in t['authorData']:
                event['event_author_name'] = t['authorData']['userName']
            event['update_date'] = unixtime_to_datetime(float(t['dateCreated'])).isoformat()
            event['oldValue'] = ''
            event['newValue'] = ''
            if event['type'] == 'core:edge':
                for val in t['oldValue']:
                    if val in self.phab_ids_names:
                        val = self.phab_ids_names[val]
                    event['oldValue'] += "," + val
                event['oldValue'] = event['oldValue'][1:]  # remove first comma
                for val in t['newValue']:
                    if val in self.phab_ids_names:
                        val = self.phab_ids_names[val]
                    event['newValue'] += "," + val
                event['newValue'] = event['newValue'][1:]  # remove first comma
            elif event['type'] in ['status', 'description', 'priority', 'reassign', 'title', 'space', 'core:create', 'parent']:
                # Convert to str so the field is always a string
                event['oldValue'] = str(t['oldValue'])
                if event['oldValue'] in self.phab_ids_names:
                    event['oldValue'] = self.phab_ids_names[event['oldValue']]
                event['newValue'] = str(t['newValue'])
                if event['newValue'] in self.phab_ids_names:
                    event['newValue'] = self.phab_ids_names[event['newValue']]
            elif event['type'] == 'core:comment':
                event['newValue'] = t['comments']
            elif event['type'] == 'core:subscribers':
                event['newValue'] = ",".join(t['newValue'])
            else:
                # logger.debug("Event type %s old to new value not supported", t['transactionType'])
                pass

            for f in task_fields_nochange:
                # The field name must be the same than in task for filtering
                event[f] = eitem[f]

            # To track history of some fields
            if event['type'] in ['status']:
                task_change['status'] = event['newValue']
            elif event['type'] == 'priority':
                task_change['priority'] = event['newValue']
            elif event['type'] == 'core:edge':
                task_change['tags_custom_analyzed'] = [event['newValue']]
            if event['type'] in ['reassign']:
                # Try to get the userName and not the user id
                if event['newValue'] in self.phab_ids_names:
                    task_change['assigned_to_userName'] = self.phab_ids_names[event['newValue']]
                    event['newValue'] = task_change['assigned_to_userName']
                else:
                    task_change['assigned_to_userName'] = event['newValue']
                if event['oldValue'] in self.phab_ids_names:
                    # Try to get the userName and not the user id
                    event['oldValue'] = self.phab_ids_names[event['oldValue']]

            for f in task_change:
                event[f] = task_change[f]

            yield event

    def __fill_phab_ids(self, item):
        """ Get mappings between phab ids and names """
        for p in item['projects']:
            if p and 'name' in p and 'phid' in p:
                self.phab_ids_names[p['phid']] = p['name']
        if 'authorData' not in item['fields'] or not item['fields']['authorData']:
            return
        self.phab_ids_names[item['fields']['authorData']['phid']] = item['fields']['authorData']['userName']
        if 'ownerData' in item['fields'] and item['fields']['ownerData']:
            self.phab_ids_names[item['fields']['ownerData']['phid']] = item['fields']['ownerData']['userName']
        if 'priority' in item['fields']:
            val = item['fields']['priority']['value']
            self.phab_ids_names[str(val)] = item['fields']['priority']['name']
        for t in item['transactions']:
            if 'authorData' in t and t['authorData'] and 'userName' in t['authorData']:
                self.phab_ids_names[t['authorData']['phid']] = t['authorData']['userName']
            elif t['authorData'] and 'name' in t['authorData']:
                # Herald
                self.phab_ids_names[t['authorData']['phid']] = t['authorData']['name']

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        self.__fill_phab_ids(item['data'])

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)
        # The real data
        phab_item = item['data']

        # data fields to copy
        copy_fields = ["phid", "id", "type"]
        for f in copy_fields:
            if f in phab_item:
                eitem[f] = phab_item[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {
            "id": "bug_id"
        }
        for f in map_fields:
            if f in phab_item:
                eitem[map_fields[f]] = phab_item[f]
            else:
                eitem[map_fields[f]] = None

        eitem['num_changes'] = len(phab_item['transactions'])

        if 'authorData' in phab_item['fields'] and phab_item['fields']['authorData']:
            # eitem['author_roles'] = ",".join(phab_item['fields']['authorData']['roles'])
            eitem['author_roles'] = phab_item['fields']['authorData']['roles']
            eitem['author_userName'] = phab_item['fields']['authorData']['userName']
            eitem['author_realName'] = phab_item['fields']['authorData']['realName']
        if 'ownerData' in phab_item['fields'] and phab_item['fields']['ownerData']:
            eitem['assigned_to_roles'] = phab_item['fields']['ownerData']['roles']
            eitem['assigned_to_userName'] = phab_item['fields']['ownerData']['userName']
            eitem['assigned_to_realName'] = phab_item['fields']['ownerData']['realName']

        eitem['priority'] = phab_item['fields']['priority']['name']
        eitem['priority_value'] = phab_item['fields']['priority']['value']
        eitem['status'] = phab_item['fields']['status']['name']
        eitem['creation_date'] = unixtime_to_datetime(phab_item['fields']['dateCreated']).isoformat()
        eitem['modification_date'] = unixtime_to_datetime(phab_item['fields']['dateModified']).isoformat()
        eitem['update_date'] = unixtime_to_datetime(item['updated_on']).isoformat()
        # raise
        eitem['main_description'] = phab_item['fields']['name']
        eitem['main_description_analyzed'] = eitem['main_description']
        eitem['url'] = eitem['origin'] + "/T" + str(eitem['bug_id'])

        # Time to assign (time to open -> time to assign)
        eitem['time_to_assign_days'] = None
        # Time to attend (time to assign-> time to first activity from assignee)
        eitem['time_to_attend_days'] = None
        # Time to close (time open -> time last updated for closed tasks)
        eitem['closed_at'] = None

        eitem['changes'] = len(phab_item['transactions'])
        # Number of assignments changes
        eitem['changes_assignment'] = 0
        # Number of assignees in the changes
        eitem['changes_assignee_number'] = 0
        # List the changes assignees
        changes_assignee_list = []
        first_assignee_phid = None
        first_assignee_date = None
        # We need to revert them to go from older to newer
        phab_item['transactions'].reverse()
        for change in phab_item['transactions']:
            change_date = unixtime_to_datetime(float(change['dateCreated'])).isoformat()
            if change["transactionType"] == "reassign":
                if not eitem['time_to_assign_days']:
                    eitem['time_to_assign_days'] = get_time_diff_days(eitem['creation_date'], change_date)
                    first_assignee_phid = change['newValue']
                    first_assignee_date = change_date
                if 'authorData' in change and change['authorData'] and 'userName' in change['authorData'] \
                        and change['authorData']['userName'] not in changes_assignee_list:
                    changes_assignee_list.append(change['authorData']['userName'])
                eitem['changes_assignment'] += 1
            elif change["transactionType"] in ["mergedinto"]:
                eitem['closed_at'] = change_date
                eitem['time_to_close_days'] = get_time_diff_days(eitem['creation_date'], eitem['closed_at'])
            elif change["transactionType"] in ["status"]:
                if change['newValue'] in CLOSED_STATUSES:
                    eitem['closed_at'] = change_date
                    eitem['time_to_close_days'] = get_time_diff_days(eitem['creation_date'], eitem['closed_at'])
            if not eitem['time_to_attend_days'] and first_assignee_phid:
                if 'authorData' in change and change['authorData'] and change['authorData']['phid'] == first_assignee_phid:
                    eitem['time_to_attend_days'] = get_time_diff_days(first_assignee_date, change_date)
        eitem['changes_assignee_number'] = len(changes_assignee_list)
        eitem['changes_assignee_list'] = ','.join(changes_assignee_list)
        eitem['comments'] = 0
        for tr in phab_item['transactions']:
            if tr['comments']:
                eitem['comments'] += 1

        eitem['tags'] = []
        for project in phab_item['projects']:
            if project:
                eitem['tags'].append(project['name'])
        eitem['tags_analyzed'] = eitem['tags']
        eitem['tags_custom_analyzed'] = eitem['tags']

        if self.sortinghat:
            eitem.update(self.get_item_sh(item, self.roles))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(eitem['creation_date'], "task"))

        assigned_to = {}
        for f in eitem.keys():
            if 'ownerData' in f:
                # Copy all ownerData data fields to assigned_to fields
                of = f.split('ownerData')[1]
                assigned_to['assigned_to' + of] = eitem[f]
        eitem.update(assigned_to)

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem
