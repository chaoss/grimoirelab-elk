# -*- coding: utf-8 -*-
#
# Github to Elastic class helper
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
#   Valerio Cosentino <valcos@bitergia.com>
#

import logging
import re

from datetime import datetime
from dateutil import parser

from .utils import get_time_diff_days

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping


logger = logging.getLogger(__name__)

GITLAB = 'https://gitlab.com/'


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
                 "type": "text"
               }
            }
        }
        """

        return {"items": mapping}


class GitLabEnrich(Enrich):

    mapping = Mapping

    issue_roles = ['author', 'assignee']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.users = {}  # cache users

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_author(self):
        return "author"

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "grimoire_creation_date"

    def get_fields_uuid(self):
        return ["author_uuid", "assignee_uuid", "user_uuid"]

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        item = item['data']
        for identity in self.issue_roles:
            if item[identity]:
                user = self.get_sh_identity(item[identity])
                if user:
                    identities.append(user)
        return identities

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
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
        comment_dates = [parser.parse(comment['created_at']).replace(tzinfo=None) for comment
                         in item['notes_data'] if item['author']['username'] != comment['author']['username']]
        reaction_dates = [parser.parse(reaction['created_at']).replace(tzinfo=None) for reaction
                          in item['award_emoji_data'] if item['author']['username'] != reaction['user']['username']]
        reaction_dates.extend(comment_dates)
        if reaction_dates:
            return min(reaction_dates)
        return None

    @metadata
    def get_rich_item(self, item):

        rich_issue = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_issue[f] = item[f]
            else:
                rich_issue[f] = None
        # The real data
        issue = item['data']

        rich_issue['time_to_close_days'] = \
            get_time_diff_days(issue['created_at'], issue['closed_at'])

        if issue['state'] != 'closed':
            rich_issue['time_open_days'] = \
                get_time_diff_days(issue['created_at'], datetime.utcnow())
        else:
            rich_issue['time_open_days'] = rich_issue['time_to_close_days']

        rich_issue['author_username'] = issue['author']['username']
        author = issue['author']

        if author:
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
        rich_issue['id_in_repo'] = issue['web_url'].split("/")[-1]
        rich_issue['repository'] = issue['web_url'].rsplit("/", 2)[0]
        rich_issue['title'] = issue['title']
        rich_issue['title_analyzed'] = issue['title']
        rich_issue['state'] = issue['state']
        rich_issue['created_at'] = issue['created_at']
        rich_issue['updated_at'] = issue['updated_at']
        rich_issue['closed_at'] = issue['closed_at']
        rich_issue['url'] = issue['web_url']
        labels = ''
        if 'labels' in issue:
            for label in issue['labels']:
                labels += label + ";;"
        if labels != '':
            labels[:-2]
        rich_issue['labels'] = labels

        rich_issue['gitlab_repo'] = rich_issue['repository'].replace(GITLAB, '')
        rich_issue['gitlab_repo'] = re.sub('.git$', '', rich_issue['gitlab_repo'])
        rich_issue["url_id"] = issue['web_url'].replace(GITLAB, '')

        if self.prjs_map:
            rich_issue.update(self.get_item_project(rich_issue))

        if 'project' in item:
            rich_issue['project'] = item['project']

        rich_issue['time_to_first_attention'] = None
        if len(issue['notes_data']) + len(issue['award_emoji_data']) != 0:
            rich_issue['time_to_first_attention'] = \
                get_time_diff_days(issue['created_at'], self.get_time_to_first_attention(issue))

        rich_issue.update(self.get_grimoire_fields(issue['created_at'], "issue"))

        if self.sortinghat:
            item[self.get_field_date()] = rich_issue[self.get_field_date()]
            rich_issue.update(self.get_item_sh(item, self.issue_roles))

        return rich_issue
