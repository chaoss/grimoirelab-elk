# -*- coding: utf-8 -*-
#
# JIRA to Elastic class helper
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

import logging

from datetime import datetime

from .enrich import Enrich, metadata

from .utils import get_time_diff_days


logger = logging.getLogger(__name__)


class JiraEnrich(Enrich):

    roles = ["assignee", "reporter", "creator"]

    def get_fields_uuid(self):
        return ["assignee_uuid", "reporter_uuid"]

    def get_field_author(self):
        return "reporter"

    def get_sh_identity(self, item, identity_field=None):
        """ Return a Sorting Hat identity using jira user data """

        identity = {}

        user = item
        if 'data' in item and type(item) == dict:
            user = item['data']['fields'][identity_field]

        if not user:
            return identity

        identity['name'] = None
        identity['username'] = None
        identity['email'] = None

        if 'displayName' in user:
            identity['name'] = user['displayName']
        if 'name' in user:
            identity['username'] = user['name']
        if 'emailAddress' in user:
            identity['email'] = user['emailAddress']

        return identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        if eitem['origin'][-1] != "/":
            repo += "/"
        repo += "projects/" + eitem['project_key']
        return repo

    def get_users_data(self, item):
        """ If user fields are inside the global item dict """
        if 'data' in item:
            users_data = item['data']['fields']
        else:
            # the item is directly the data (kitsune answer)
            users_data = item
        return users_data

    def get_identities(self, item):
        ''' Return the identities from an item '''

        identities = []

        item = item['data']

        for field in ["assignee", "reporter", "creator"]:
            if field not in item["fields"]:
                continue
            if item["fields"][field]:
                identities.append(self.get_sh_identity(item["fields"][field]))

        return identities

    @staticmethod
    def fix_value_null(value):
        """Fix <null> values in some Jira parameters.

        In some values fields, as returned by the Jira API,
        some fields appear as <null>. This function convert Them
        to None.

        :param value: string found in a value fields
        :return: same as value, or None
        """
        if value == '<null>':
            return None
        else:
            return value

    @classmethod
    def enrich_fields(cls, fields, eitem):
        """Enrich the fields property of an issue.

        Loops through al properties in issue['fields'],
        using those that are relevant to enrich eitem with new properties.
        Those properties are user defined, depending on options
        configured in Jira. For example, if SCRUM is activated,
        we have a field named "Story Points".

        :param fields: fields property of an issue
        :param eitem: enriched item, which will be modified adding more properties
        """

        for field in fields:
            if field.startswith('customfield_'):
                if type(fields[field]) is dict:
                    if 'name' in fields[field]:
                        if fields[field]['name'] == "Story Points":
                            eitem['story_points'] = fields[field]['value']
                        elif fields[field]['name'] == "Sprint":
                            value = fields[field]['value']
                            if value:
                                sprint = value[0].partition(",name=")[2].split(',')[0]
                                sprint_start = value[0].partition(",startDate=")[2].split(',')[0]
                                sprint_end = value[0].partition(",endDate=")[2].split(',')[0]
                                sprint_complete = value[0].partition(",completeDate=")[2].split(',')[0]
                                eitem['sprint'] = sprint
                                eitem['sprint_start'] = cls.fix_value_null(sprint_start)
                                eitem['sprint_end'] = cls.fix_value_null(sprint_end)
                                eitem['sprint_complete'] = cls.fix_value_null(sprint_complete)

    @metadata
    def get_rich_item(self, item):

        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        issue = item['data']

        # Fields that are the same in item and eitem
        copy_fields = ["assignee", "reporter"]
        for f in copy_fields:
            if f in issue:
                eitem[f] = issue[f]
            else:
                eitem[f] = None

        eitem['changes'] = issue['changelog']['total']

        if issue["fields"]["assignee"]:
            eitem['assignee'] = issue["fields"]["assignee"]["displayName"]
            if "timeZone" in issue["fields"]["assignee"]:
                eitem['assignee_tz'] = issue["fields"]["assignee"]["timeZone"]

        if issue["fields"]["creator"] and "creator" in issue["fields"]:
            eitem['author_name'] = issue["fields"]["creator"]["displayName"]
            eitem['author_login'] = issue["fields"]["creator"]["name"]
            if "timeZone" in issue["fields"]["creator"]:
                eitem['author_tz'] = issue["fields"]["creator"]["timeZone"]

        eitem['creation_date'] = issue["fields"]['created']

        if 'description' in issue["fields"] and issue["fields"]['description']:
            eitem['main_description'] = issue["fields"]['description'][:self.KEYWORD_MAX_SIZE]

        eitem['issue_type'] = issue["fields"]['issuetype']['name']
        eitem['issue_description'] = issue["fields"]['issuetype']['description']

        if 'labels' in issue['fields']:
            eitem['labels'] = issue['fields']['labels']

        if ('priority' in issue['fields'] and issue['fields']['priority'] and
            'name' in issue['fields']['priority']):
            eitem['priority'] = issue['fields']['priority']['name']

        # data.fields.progress.percent not exists in Puppet JIRA
        if 'progress'in issue['fields']:
            eitem['progress_total'] = issue['fields']['progress']['total']
        eitem['project_id'] = issue['fields']['project']['id']
        eitem['project_key'] = issue['fields']['project']['key']
        eitem['project_name'] = issue['fields']['project']['name']

        if 'reporter' in issue['fields'] and issue['fields']['reporter']:
            eitem['reporter_name'] = issue['fields']['reporter']['displayName']
            eitem['reporter_login'] = issue['fields']['reporter']['name']
            if "timeZone" in issue["fields"]["reporter"]:
                eitem['reporter_tz'] = issue["fields"]["reporter"]["timeZone"]

        if issue['fields']['resolution']:
            eitem['resolution_id'] = issue['fields']['resolution']['id']
            eitem['resolution_name'] = issue['fields']['resolution']['name']
            eitem['resolution_description'] = issue['fields']['resolution']['description']
            eitem['resolution_self'] = issue['fields']['resolution']['self']
        eitem['resolution_date'] = issue['fields']['resolutiondate']
        eitem['status_description'] = issue['fields']['status']['description']
        eitem['status'] = issue['fields']['status']['name']
        eitem['summary'] = issue['fields']['summary']
        if 'timeoriginalestimate' in issue['fields']:
            eitem['original_time_estimation'] = issue['fields']['timeoriginalestimate']
            if eitem['original_time_estimation']:
                eitem['original_time_estimation_hours'] = int(eitem['original_time_estimation']) / 3600
        if 'timespent' in issue['fields']:
            eitem['time_spent'] = issue['fields']['timespent']
            if eitem['time_spent']:
                eitem['time_spent_hours'] = int(eitem['time_spent']) / 3600
        if 'timeestimate' in issue['fields']:
            eitem['time_estimation'] = issue['fields']['timeestimate']
            if eitem['time_estimation']:
                eitem['time_estimation_hours'] = int(eitem['time_estimation']) / 3600
        eitem['watchers'] = issue['fields']['watches']['watchCount']
        eitem['key'] = issue['key']

        # Add extra JSON fields used in Kibana (enriched fields)
        eitem['number_of_comments'] = 0
        eitem['time_to_last_update_days'] = None
        eitem['url'] = None

        if 'long_desc' in issue:
            eitem['number_of_comments'] = len(issue['long_desc'])
        eitem['url'] = item['origin'] + "/browse/" + issue['key']
        eitem['time_to_close_days'] = \
            get_time_diff_days(issue['fields']['created'], issue['fields']['updated'])
        eitem['time_to_last_update_days'] = \
            get_time_diff_days(issue['fields']['created'], datetime.utcnow())

        if 'fixVersions' in issue['fields']:
            eitem['releases'] = []
            for version in issue['fields']['fixVersions']:
                eitem['releases'] += [version['name']]

        self.enrich_fields(issue['fields'], eitem)

        if self.sortinghat:
            eitem.update(self.get_item_sh(item, self.roles))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(issue['fields']['created'], "issue"))

        return eitem
