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
#   Venu Vardhan Reddy Tekula <venuvardhanreddytekula8@gmail.com>
#


import logging

from .qmenrich import QMEnrich

from perceval.backend import uuid
from grimoirelab_toolkit.datetime import str_to_datetime

MAX_SIZE_BULK_ENRICHED_ITEMS = 200

ISSUE_TYPE = 'issue'
MERGE_TYPE = 'merge_request'

logger = logging.getLogger(__name__)


class GitLabQMEnrich(QMEnrich):

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.comments = {
            'number_comments': {},
            'number_attended': {}
        }

        self.date_items = {
            'data': {}
        }

        self.studies = []

    def get_identities(self, item):
        """Return the identities from an item"""
        identities = []

        return identities

    def has_identities(self):
        """ Return whether the enriched items contains identities """

        return False

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_unique_id(self):
        return "id"

    def normalized_date(self, dt):

        processed_dt = None

        if dt and dt is not None:
            processed_dt = str_to_datetime(dt).replace(hour=0, minute=0, second=0, microsecond=0)
            processed_dt = processed_dt.isoformat()

        return processed_dt

    def extract_project(self, item):
        return item['search_fields']['project']

    def extract_comment_metric(self, item):

        comments = item['notes_data']

        author = item['author']['username']

        if comments and comments is not None:
            for comment in comments:
                comment_date = self.normalized_date(comment['created_at'])
                comment_author = comment['author']['username']

                if comment_date in self.comments['number_comments'].keys():
                    self.comments['number_comments'][comment_date] += 1
                else:
                    self.comments['number_comments'][comment_date] = 1

                if comment_author != author:
                    if comment_date in self.comments['number_attended'].keys():
                        self.comments['number_attended'][comment_date].add(item['iid'])
                    else:
                        self.comments['number_attended'][comment_date] = {item['iid']}

    def extract_issue_metric(self, item):
        issue = item['data']

        self.extract_comment_metric(issue)

        created_at = self.normalized_date(issue['created_at'])
        closed_at = self.normalized_date(issue['closed_at'])

        if 'project' in self.date_items.keys():

            if created_at in self.date_items['data']['created_issue'].keys():
                self.date_items['data']['created_issue'][created_at] += 1
            else:
                self.date_items['data']['created_issue'][created_at] = 1

            if closed_at and closed_at is not None:
                if closed_at in self.date_items['data']['closed_issue'].keys():
                    self.date_items['data']['closed_issue'][closed_at] += 1
                else:
                    self.date_items['data']['closed_issue'][closed_at] = 1

        else:
            project = self.extract_project(item)

            created = {}
            closed = {}

            created[created_at] = 1
            if closed_at and closed_at is not None:
                closed[closed_at] = 1

            self.date_items = {
                "project": project,
                "data": {
                    "created_issue": created,
                    "closed_issue": closed,
                    "issue_comment": {},
                    "issue_attended": {}
                }
            }

    def extract_merge_metric(self, item):
        merge = item['data']

        self.extract_comment_metric(merge)

        created_at = self.normalized_date(merge['created_at'])
        closed_at = self.normalized_date(merge['closed_at'])
        merged_at = self.normalized_date(merge['merged_at'])

        if 'project' in self.date_items.keys():

            if created_at in self.date_items['data']['created_merge'].keys():
                self.date_items['data']['created_merge'][created_at] += 1
            else:
                self.date_items['data']['created_merge'][created_at] = 1

            if closed_at and closed_at is not None:
                if closed_at in self.date_items['data']['closed_merge'].keys():
                    self.date_items['data']['closed_merge'][closed_at] += 1
                else:
                    self.date_items['data']['closed_merge'][closed_at] = 1

            if merged_at and merged_at is not None:
                if merged_at in self.date_items['data']['merged_merge'].keys():
                    self.date_items['data']['merged_merge'][merged_at] += 1
                else:
                    self.date_items['data']['merged_merge'][merged_at] = 1

        else:
            project = self.extract_project(item)

            created = {}
            closed = {}
            merged = {}

            created[created_at] = 1
            if closed_at and closed_at is not None:
                closed[closed_at] = 1
            if merged_at and merged_at is not None:
                merged[merged_at] = 1

            self.date_items = {
                "project": project,
                "data": {
                    "created_merge": created,
                    "closed_merge": closed,
                    "merged_merge": merged,
                    "merge_comment": {},
                    "merge_attended": {}
                }
            }

    def update_metric_items(self, category):
        eitem = {
            "metric_es_compute": 'sample',
            "metric_type": 'LineChart'
        }

        edict = {}

        if category == 'created_issue':
            edict = {
                "metric_class": 'issues',
                "metric_id": 'issues.numberCreatedIssues',
                "metric_desc": 'The number of issues created on a current date.',
                "metric_name": 'Number of Created Issues'
            }
        elif category == 'closed_issue':
            edict = {
                "metric_class": 'issues',
                "metric_id": 'issues.numberClosedIssues',
                "metric_desc": 'The number of issues closed on a current date.',
                "metric_name": 'Number of Closed Issues'
            }
        elif category == 'issue_comment':
            edict = {
                "metric_class": 'issues',
                "metric_id": 'issues.numberIssueComments',
                "metric_desc": 'The number of issue comments posted on a current date.',
                "metric_name": 'Number of Issue Comments'
            }
        elif category == 'issue_attended':
            edict = {
                "metric_class": 'issues',
                "metric_id": 'issues.numberIssueAttended',
                "metric_desc": 'The number of issues attended on a current date.',
                "metric_name": 'Number of Issues Attended'
            }
        elif category == 'created_merge':
            edict = {
                "metric_class": 'merges',
                "metric_id": 'merges.numberCreatedMerges',
                "metric_desc": 'The number of merge requests created on a current date.',
                "metric_name": 'Number of Created Merge Requests'
            }
        elif category == 'closed_merge':
            edict = {
                "metric_class": 'merges',
                "metric_id": 'merges.numberClosedMerges',
                "metric_desc": 'The number of merge requests closed on a current date.',
                "metric_name": 'Number of Closed Merge Requests'
            }
        elif category == 'merged_merge':
            edict = {
                "metric_class": 'merges',
                "metric_id": 'merges.numberMergedMerges',
                "metric_desc": 'The number of merge requests merged on a current date.',
                "metric_name": 'Number of Merged Merge Requests'
            }
        elif category == 'merge_comment':
            edict = {
                "metric_class": 'merges',
                "metric_id": 'merges.numberMergeComments',
                "metric_desc": 'The number of merge comments posted on a current date.',
                "metric_name": 'Number of Merge Comments'
            }
        elif category == 'merge_attended':
            edict = {
                "metric_class": 'merges',
                "metric_id": 'merges.numberMergeAttended',
                "metric_desc": 'The number of merge requests attended on a current date.',
                "metric_name": 'Number of Merge Requests Attended'
            }

        eitem.update(edict)

        return eitem

    def get_rich_item(self, dt, category):
        edate = {}

        edate.update(self.update_metric_items(category))

        edate['project'] = self.date_items['project']
        edate['datetime'] = dt
        edate['metric_es_value'] = self.date_items['data'][category][dt]
        edate['metric_es_value_weighted'] = self.date_items['data'][category][dt]
        edate['uuid'] = uuid(edate['metric_id'], edate['project'], edate['datetime'])
        edate['id'] = '{}_{}'.format(category, edate['uuid'])

        edate.update(self.get_grimoire_fields(dt, "date"))

        return edate

    def enrich_items(self, ocean_backend):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        item_tag = None

        for item in ocean_backend.fetch():

            if item['category'] == ISSUE_TYPE:
                self.extract_issue_metric(item)
                item_tag = 'issue'
            elif item['category'] == MERGE_TYPE:
                self.extract_merge_metric(item)
                item_tag = 'merge'

        self.date_items['data']['{}_comment'.format(item_tag)] = self.comments['number_comments']

        for k, v in self.comments['number_attended'].items():
            self.date_items['data']['{}_attended'.format(item_tag)][k] = len(v)

        for category in self.date_items['data'].keys():
            for dt in self.date_items['data'][category].keys():
                eitem = self.get_rich_item(dt, category)

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
            logger.error("%s/%s missing items for GitLab QM", str(missing), str(num_items))
        else:
            logger.info("%s items inserted for GitLab QM", str(num_items))

        return num_items
