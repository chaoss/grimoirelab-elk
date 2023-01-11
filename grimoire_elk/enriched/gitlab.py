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
import re

from datetime import datetime

from grimoirelab_toolkit.datetime import str_to_datetime

from ..errors import ELKError
from .utils import get_time_diff_days

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping


logger = logging.getLogger(__name__)

GITLAB = 'https://gitlab.com/'
NO_MILESTONE_TAG = "-empty-"
GITLAB_MERGES = "gitlab-merges"
GITLAB_ISSUES = "gitlab-issues"


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
               "title_analyzed": {
                 "type": "text",
                 "index": true
               }
            }
        }
        """

        return {"items": mapping}


class GitLabEnrich(Enrich):

    mapping = Mapping

    issue_roles = ['author', 'assignee']
    merge_roles = ['author']

    def __init__(self, db_sortinghat=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', db_path=None,
                 db_port=None, db_ssl=False):
        super().__init__(db_sortinghat=db_sortinghat, json_projects_map=json_projects_map,
                         db_user=db_user, db_password=db_password, db_host=db_host,
                         db_port=db_port, db_path=db_path, db_ssl=db_ssl)

        self.users = {}  # cache users
        self.studies = []
        self.studies.append(self.enrich_onion)

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_author(self):
        return "author"

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "grimoire_creation_date"

    def get_identities(self, item):
        """ Return the identities from an item """

        item = item['data']
        for identity in self.issue_roles:
            if item[identity]:
                user = self.get_sh_identity(item[identity])
                if user:
                    yield user

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]

        if not user:
            return identity

        identity['username'] = user['username']
        identity['email'] = None
        identity['name'] = None
        if 'email' in user:
            identity['email'] = user['email']
        if 'name' in user:
            identity['name'] = user['name']
        return identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        return repo

    def get_time_to_first_attention(self, item):
        """Get the first date at which a comment or reaction was made to the issue by someone
        other than the user who created the issue
        """
        comment_dates = [str_to_datetime(comment['created_at']).replace(tzinfo=None) for comment
                         in item['notes_data'] if item['author']['username'] != comment['author']['username']]
        reaction_dates = [str_to_datetime(reaction['created_at']).replace(tzinfo=None) for reaction
                          in item['award_emoji_data'] if item['author']['username'] != reaction['user']['username']]
        reaction_dates.extend(comment_dates)
        if reaction_dates:
            return min(reaction_dates)
        return None

    @metadata
    def get_rich_item(self, item):

        rich_item = {}
        if item['category'] == 'issue':
            rich_item = self.__get_rich_issue(item)
        elif item['category'] == 'merge_request':
            rich_item = self.__get_rich_merge(item)
        else:
            logger.error("[gerrit] rich item not defined for GitLab category {}".format(item['category']))

        self.add_repository_labels(rich_item)
        self.add_metadata_filter_raw(rich_item)
        return rich_item

    def __get_rich_issue(self, item):

        rich_issue = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_issue)
        # The real data
        issue = item['data']

        rich_issue['time_to_close_days'] = \
            get_time_diff_days(issue['created_at'], issue['closed_at'])

        if issue['state'] != 'closed':
            rich_issue['time_open_days'] = \
                get_time_diff_days(issue['created_at'], datetime.utcnow())
        else:
            rich_issue['time_open_days'] = rich_issue['time_to_close_days']

        author = issue.get('author', None)
        if author:
            rich_issue['author_username'] = issue['author']['username']
            rich_issue['author_name'] = author['name']
            if 'email' in author and author['email']:
                rich_issue["author_domain"] = self.get_email_domain(author['email'])
            if 'organization' in author and author['organization']:
                rich_issue['author_org'] = author['organization']
            if 'location' in author and author['location']:
                rich_issue['author_location'] = author['location']
        else:
            rich_issue['author_username'] = None
            rich_issue['author_name'] = None
            rich_issue["author_domain"] = None
            rich_issue['author_org'] = None
            rich_issue['author_location'] = None

        assignee = None

        if 'assignee' in issue and issue['assignee']:
            assignee = issue['assignee']
            rich_issue['assignee_username'] = assignee['username']
            rich_issue['assignee_name'] = assignee['name']
            if 'email' in assignee and assignee['email']:
                rich_issue["assignee_domain"] = self.get_email_domain(assignee['email'])
            if 'organization' in assignee and assignee['organization']:
                rich_issue['assignee_org'] = assignee['organization']
            if 'location' in assignee and assignee['location']:
                rich_issue['assignee_location'] = assignee['location']
        else:
            rich_issue['assignee_username'] = None
            rich_issue['assignee_name'] = None
            rich_issue["assignee_domain"] = None
            rich_issue['assignee_org'] = None
            rich_issue['assignee_location'] = None

        rich_issue['id'] = issue['id']
        rich_issue['id_in_repo'] = issue['iid']
        rich_issue['repository'] = issue['web_url'].rsplit("/", 2)[0].split("/-")[0]
        rich_issue['title'] = issue['title']
        rich_issue['title_analyzed'] = issue['title']
        rich_issue['state'] = issue['state']
        rich_issue['created_at'] = issue['created_at']
        rich_issue['updated_at'] = issue['updated_at']
        rich_issue['closed_at'] = issue['closed_at']
        rich_issue['url'] = issue['web_url']
        rich_issue['labels'] = issue['labels']

        rich_issue['gitlab_repo'] = rich_issue['repository'].replace(GITLAB, '')
        rich_issue['gitlab_repo'] = re.sub('.git$', '', rich_issue['gitlab_repo'])
        rich_issue["url_id"] = issue['web_url'].replace(GITLAB, '')

        self.__add_milestone_info(issue, rich_issue)

        if self.prjs_map:
            rich_issue.update(self.get_item_project(rich_issue))

        if 'project' in item:
            rich_issue['project'] = item['project']

        rich_issue['time_to_first_attention'] = None
        if len(issue['notes_data']) + len(issue['award_emoji_data']) != 0:
            rich_issue['time_to_first_attention'] = \
                get_time_diff_days(issue['created_at'], self.get_time_to_first_attention(issue))

        rich_issue.update(self.get_grimoire_fields(issue['created_at'], "issue"))

        item[self.get_field_date()] = rich_issue[self.get_field_date()]
        rich_issue.update(self.get_item_sh(item, self.issue_roles))

        return rich_issue

    def __get_rich_merge(self, item):
        rich_mr = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_mr)
        # The real data
        merge_request = item['data']

        # merge requests can end up in two states, merged and closed. The former concerns merge requests
        # that were finally merged to the code base, while the latter represents rejected merge requests.
        # `time_to_close_days` and `time_to_merge_days` are aligned to the aforementioned states.
        time_to_close_days = get_time_diff_days(merge_request['created_at'], merge_request['closed_at'])
        time_to_merge_days = get_time_diff_days(merge_request['created_at'], merge_request['merged_at'])

        rich_mr['time_to_close_days'] = time_to_merge_days if time_to_merge_days else time_to_close_days

        if merge_request['state'] not in ['merged', 'closed']:
            rich_mr['time_open_days'] = \
                get_time_diff_days(merge_request['created_at'], datetime.utcnow())
        else:
            rich_mr['time_open_days'] = rich_mr['time_to_close_days']

        author = merge_request.get('author', None)
        if author:
            rich_mr['author_username'] = merge_request['author']['username']
            rich_mr['author_name'] = author.get('name', None)
            if 'email' in author and author['email']:
                rich_mr["author_domain"] = self.get_email_domain(author['email'])
            if 'organization' in author and author['organization']:
                rich_mr['author_org'] = author['organization']
            if 'location' in author and author['location']:
                rich_mr['author_location'] = author['location']
        else:
            rich_mr['author_username'] = None
            rich_mr['author_name'] = None
            rich_mr["author_domain"] = None
            rich_mr['author_org'] = None
            rich_mr['author_location'] = None

        merged_by = None

        if merge_request['merged_by'] is not None:
            merged_by = merge_request['merged_by']
            rich_mr['merge_author_login'] = merged_by['username']
            rich_mr['merge_author_name'] = merged_by['name']
            if 'email' in merged_by and merged_by['email']:
                rich_mr["merge_author_domain"] = self.get_email_domain(merged_by['email'])
            if 'organization' in merged_by and merged_by['organization']:
                rich_mr['merge_author_org'] = merged_by['organization']
            if 'location' in merged_by and merged_by['location']:
                rich_mr['merge_author_location'] = merged_by['location']
        else:
            rich_mr['merge_author_name'] = None
            rich_mr['merge_author_login'] = None
            rich_mr["merge_author_domain"] = None
            rich_mr['merge_author_org'] = None
            rich_mr['merge_author_location'] = None

        rich_mr['id'] = merge_request['id']
        rich_mr['id_in_repo'] = merge_request['iid']
        rich_mr['repository'] = merge_request['web_url'].rsplit("/", 2)[0].split("/-")[0]
        rich_mr['title'] = merge_request['title']
        rich_mr['title_analyzed'] = merge_request['title']
        rich_mr['state'] = merge_request['state']
        rich_mr['created_at'] = merge_request['created_at']
        rich_mr['updated_at'] = merge_request['updated_at']
        rich_mr['merged_at'] = merge_request['merged_at']
        rich_mr['closed_at'] = merge_request['closed_at']
        rich_mr['solved_at'] = rich_mr['merged_at'] if rich_mr['merged_at'] else rich_mr['closed_at']
        rich_mr['url'] = merge_request['web_url']
        rich_mr['merged'] = rich_mr['state'] == 'merged'
        rich_mr['num_notes'] = len(merge_request['notes_data'])

        rich_mr['labels'] = merge_request['labels']

        rich_mr['merge_request'] = True
        rich_mr['item_type'] = 'merge_request request'

        # GMD code development metrics
        rich_mr['code_merge_duration'] = get_time_diff_days(merge_request['created_at'],
                                                            merge_request['merged_at'])
        rich_mr['num_versions'] = len(merge_request['versions_data'])

        rich_mr['gitlab_repo'] = rich_mr['repository'].replace(GITLAB, '')
        rich_mr['gitlab_repo'] = re.sub('.git$', '', rich_mr['gitlab_repo'])
        rich_mr["url_id"] = merge_request['web_url'].replace(GITLAB, '')

        rich_mr['time_to_first_attention'] = None
        if len(merge_request['notes_data']) + len(merge_request['award_emoji_data']) != 0:
            rich_mr['time_to_first_attention'] = \
                get_time_diff_days(merge_request['created_at'], self.get_time_to_first_attention(merge_request))

        self.__add_milestone_info(merge_request, rich_mr)

        if self.prjs_map:
            rich_mr.update(self.get_item_project(rich_mr))

        if 'project' in item:
            rich_mr['project'] = item['project']

        rich_mr.update(self.get_grimoire_fields(merge_request['created_at'], "merge_request"))

        item[self.get_field_date()] = rich_mr[self.get_field_date()]
        rich_mr.update(self.get_item_sh(item, self.merge_roles))

        return rich_mr

    def __add_milestone_info(self, item_data, eitem):
        """Add milestone tag, start date and due date to the enriched item"""

        eitem['milestone'] = NO_MILESTONE_TAG
        eitem['milestone_start_date'] = None
        eitem['milestone_due_date'] = None
        eitem['milestone_url'] = None
        eitem['milestone_id'] = None
        eitem['milestone_iid'] = None

        if 'milestone' in item_data and item_data['milestone']:
            milestone = item_data['milestone']

            eitem['milestone'] = milestone.get('title', None)
            eitem['milestone_url'] = milestone.get('web_url', None)
            eitem['milestone_id'] = milestone.get('id', None)
            eitem['milestone_iid'] = milestone.get('iid', None)

            start_date_str = milestone.get('start_date', None)
            due_date_str = milestone.get('due_date', None)

            if start_date_str:
                eitem['milestone_start_date'] = str_to_datetime(start_date_str).replace(tzinfo=None).isoformat()

            if due_date_str:
                eitem['milestone_due_date'] = str_to_datetime(due_date_str).replace(tzinfo=None).isoformat()

    def enrich_onion(self, ocean_backend, enrich_backend, alias,
                     in_index, out_index, data_source=None, no_incremental=False,
                     contribs_field='uuid',
                     timeframe_field='grimoire_creation_date',
                     sort_on_field='metadata__timestamp',
                     seconds=Enrich.ONION_INTERVAL):

        if not data_source:
            raise ELKError(cause="Missing data_source attribute")

        if data_source not in [GITLAB_MERGES, GITLAB_ISSUES]:
            logger.warning("[gitlab] data source value {} should be: {} or {}".format(
                           data_source, GITLAB_ISSUES, GITLAB_MERGES))

        super().enrich_onion(enrich_backend=enrich_backend,
                             alias=alias,
                             in_index=in_index,
                             out_index=out_index,
                             data_source=data_source,
                             contribs_field=contribs_field,
                             timeframe_field=timeframe_field,
                             sort_on_field=sort_on_field,
                             no_incremental=no_incremental,
                             seconds=seconds)
