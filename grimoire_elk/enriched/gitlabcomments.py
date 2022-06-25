# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2021 Bitergia
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
import re
import collections

from grimoirelab_toolkit.datetime import str_to_datetime, datetime_utcnow

from .utils import get_time_diff_days

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping

MAX_SIZE_BULK_ENRICHED_ITEMS = 200

GITLAB = 'https://gitlab.com/'

ISSUE_TYPE = 'issue'
MERGE_TYPE = 'merge_request'

COMMENT_TYPE = 'comment'
ISSUE_COMMENT_TYPE = 'issue_comment'
MERGE_COMMENT_TYPE = 'merge_comment'

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
               "title_analyzed": {
                    "type": "text",
                    "index": true
               },
               "body_analyzed": {
                    "type": "text",
                    "index": true
               },
               "id": {
                    "type": "keyword"
               }
            }
        }
        """

        return {"items": mapping}


class GitLabCommentsEnrich(Enrich):
    mapping = Mapping

    comment_roles = ['author']
    issue_roles = ['author', 'assignee']
    mr_roles = ['author', 'merged_by']
    roles = ['author', 'assignee', 'merged_by']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_author(self):
        return "author"

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "grimoire_creation_date"

    def get_field_unique_id(self):
        return "id"

    def add_gelk_metadata(self, eitem):
        eitem['metadata__gelk_version'] = self.gelk_version
        eitem['metadata__gelk_backend_name'] = self.__class__.__name__
        eitem['metadata__enriched_on'] = datetime_utcnow().isoformat()

    def get_identities(self, item):
        """Return the identities from an item"""

        category = item['category']

        item = item['data']
        comments_attr = None
        if category == "issue":
            identity_types = ['author', 'assignee']
            comments_attr = 'notes_data'
        elif category == "merge_request":
            identity_types = ['author', 'merged_by']
            comments_attr = 'notes_data'
        else:
            identity_types = []

        for identity in identity_types:
            if item[identity]:
                user = self.get_sh_identity(item[identity])
                if user:
                    yield user

        comments = item.get(comments_attr, [])
        for comment in comments:
            user = self.get_sh_identity(comment['author'])
            if user:
                yield user

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]
        elif identity_field:
            user = item[identity_field]

        if not user:
            return identity

        identity['username'] = user.get('username', None)
        identity['name'] = user.get('name', None)
        identity['email'] = user.get('email', None)

        return identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        return repo

    def get_time_to_first_attention(self, item):
        """Get the first date at which a comment or reaction was made to the issue by someone
        other than the user who created the issue
        """
        first_attention = None

        author = item.get('author', None)
        if author is not None and author:

            comments_dates = [str_to_datetime(comment['created_at']) for comment in item['notes_data']
                              if item['author']['username'] != comment['author']['username']]
            reaction_dates = [str_to_datetime(reaction['created_at']) for reaction in item['award_emoji_data']
                              if item['author']['username'] != reaction['user']['username']]
            reaction_dates.extend(comments_dates)

            if reaction_dates:
                first_attention = min(reaction_dates)

        return first_attention

    def get_time_to_merge_request_response(self, item):
        """Get the first date at which a review was made on the MR by someone
        other than the user who created the MR
        """
        first_attention = None

        author = item.get('author', None)
        if author is not None and author:

            review_dates = [str_to_datetime(comment['created_at']) for comment in item['notes_data']
                            if item['author']['username'] != comment['author']['username']]

            if review_dates:
                first_attention = min(review_dates)

        return first_attention

    def __get_reactions(self, item):
        item_reactions = item.get('award_emoji_data', [])
        reactions_total_count = len(item_reactions)
        if item_reactions:
            reactions_counter = collections.Counter([reaction["name"] for reaction in item_reactions])
            item_reactions = [{"type": reaction, "count": reactions_counter[reaction]} for reaction in
                              reactions_counter]

        return {"reactions": item_reactions, "reactions_total_count": reactions_total_count}

    def __get_rich_issue(self, item):
        rich_issue = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_issue)

        # the real data
        issue = item['data']

        rich_issue['time_to_close_days'] = \
            get_time_diff_days(issue['created_at'], issue['closed_at'])

        if issue['state'] != 'closed':
            rich_issue['time_open_days'] = \
                get_time_diff_days(issue['created_at'], datetime_utcnow().replace(tzinfo=None))
        else:
            rich_issue['time_open_days'] = rich_issue['time_to_close_days']

        author = issue.get('author', None)
        if author is not None and author:
            rich_issue['author_login'] = author['username']
            rich_issue['author_name'] = author['name']
            rich_issue['author_domain'] = self.get_email_domain(author['email']) if author.get('email') else None
        else:
            rich_issue['author_login'] = None
            rich_issue['author_name'] = None
            rich_issue['author_domain'] = None

        assignee = issue.get('assignee', None)
        if assignee is not None and assignee:
            rich_issue['assignee_login'] = assignee['username']
            rich_issue['assignee_name'] = assignee['name']
            rich_issue['assignee_domain'] = self.get_email_domain(assignee['email']) if assignee.get('email') else None
        else:
            rich_issue['assignee_login'] = None
            rich_issue['assignee_name'] = None
            rich_issue['assignee_domain'] = None

        rich_issue['id'] = str(issue['id'])
        rich_issue['issue_id'] = issue['id']
        rich_issue['issue_id_in_repo'] = issue['web_url'].split("/")[-1]
        rich_issue['repository'] = self.get_project_repository(rich_issue)
        rich_issue['issue_title'] = issue['title']
        rich_issue['issue_title_analyzed'] = issue['title']
        rich_issue['issue_state'] = issue['state']
        rich_issue['issue_created_at'] = issue['created_at']
        rich_issue['issue_updated_at'] = issue['updated_at']
        rich_issue['issue_closed_at'] = issue['closed_at']
        rich_issue['url'] = issue['web_url']
        rich_issue['issue_url'] = issue['web_url']

        # extract reactions and add it to enriched item
        rich_issue.update(self.__get_reactions(issue))

        rich_issue['issue_labels'] = issue['labels']
        rich_issue['item_type'] = ISSUE_TYPE

        rich_issue['gitlab_repo'] = rich_issue['repository'].replace(GITLAB, '')
        rich_issue['gitlab_repo'] = re.sub('.git$', '', rich_issue['gitlab_repo'])

        if self.prjs_map:
            rich_issue.update(self.get_item_project(rich_issue))

        # if 'project' in item:
        #     rich_issue['project'] = item['project']

        rich_issue['time_to_first_attention'] = None
        if len(issue['award_emoji_data']) + len(issue['notes_data']) != 0:
            rich_issue['time_to_first_attention'] = \
                get_time_diff_days(str_to_datetime(issue['created_at']),
                                   self.get_time_to_first_attention(issue))

        rich_issue.update(self.get_grimoire_fields(issue['created_at'], ISSUE_TYPE))

        # due to backtrack compatibility, `is_gitlabcomments_*` is replaced with `is_gitlab_*`
        rich_issue.pop('is_gitlabcomments_{}'.format(ISSUE_TYPE))
        rich_issue['is_gitlab_{}'.format(ISSUE_TYPE)] = 1

        if self.sortinghat:
            item[self.get_field_date()] = rich_issue[self.get_field_date()]
            rich_issue.update(self.get_item_sh(item, self.issue_roles))

        return rich_issue

    def __get_rich_merge(self, item):
        rich_mr = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_mr)
        # The real data
        merge_request = item['data']

        rich_mr['time_to_close_days'] = \
            get_time_diff_days(merge_request['created_at'], merge_request['closed_at'])

        if merge_request['state'] != 'closed':
            rich_mr['time_open_days'] = \
                get_time_diff_days(merge_request['created_at'], datetime_utcnow().replace(tzinfo=None))
        else:
            rich_mr['time_open_days'] = rich_mr['time_to_close_days']

        author = merge_request.get('author', None)
        if author is not None and author:
            rich_mr['author_login'] = author['username']
            rich_mr['author_name'] = author['name']
            rich_mr['author_domain'] = self.get_email_domain(author['email']) if author.get('email') else None
        else:
            rich_mr['author_login'] = None
            rich_mr['author_name'] = None
            rich_mr['author_domain'] = None

        merged_by = merge_request.get('merged_by', None)
        if merged_by is not None and merged_by:
            rich_mr['merge_author_login'] = merged_by['username']
            rich_mr['merge_author_name'] = merged_by['name']
            rich_mr['merge_author_domain'] = \
                self.get_email_domain(merged_by['email']) if merged_by.get('email') else None
        else:
            rich_mr['merge_author_login'] = None
            rich_mr['merge_author_name'] = None
            rich_mr['merge_author_domain'] = None

        rich_mr['id'] = str(merge_request['id'])
        rich_mr['merge_id'] = merge_request['id']
        rich_mr['merge_id_in_repo'] = merge_request['web_url'].split("/")[-1]
        rich_mr['repository'] = self.get_project_repository(rich_mr)
        rich_mr['merge_title'] = merge_request['title']
        rich_mr['merge_title_analyzed'] = merge_request['title']
        rich_mr['merge_state'] = merge_request['state']
        rich_mr['merge_created_at'] = merge_request['created_at']
        rich_mr['merge_updated_at'] = merge_request['updated_at']
        rich_mr['merge_status'] = merge_request['merge_status']
        rich_mr['merge_merged_at'] = merge_request['merged_at']
        rich_mr['merge_closed_at'] = merge_request['closed_at']
        rich_mr['url'] = merge_request['web_url']
        rich_mr['merge_url'] = merge_request['web_url']

        # extract reactions and add it to enriched item
        rich_mr.update(self.__get_reactions(merge_request))

        rich_mr['merge_labels'] = merge_request['labels']
        rich_mr['item_type'] = MERGE_TYPE

        rich_mr['gitlab_repo'] = rich_mr['repository'].replace(GITLAB, '')
        rich_mr['gitlab_repo'] = re.sub('.git$', '', rich_mr['gitlab_repo'])

        # GMD code development metrics
        rich_mr['code_merge_duration'] = get_time_diff_days(merge_request['created_at'],
                                                            merge_request['merged_at'])
        rich_mr['num_versions'] = len(merge_request['versions_data'])
        rich_mr['num_merge_comments'] = len(merge_request['notes_data'])

        rich_mr['time_to_merge_request_response'] = None
        if merge_request['notes_data'] != 0:
            min_review_date = self.get_time_to_merge_request_response(merge_request)
            rich_mr['time_to_merge_request_response'] = \
                get_time_diff_days(str_to_datetime(merge_request['created_at']), min_review_date)

        if self.prjs_map:
            rich_mr.update(self.get_item_project(rich_mr))

        # if 'project' in item:
        #     rich_mr['project'] = item['project']

        rich_mr.update(self.get_grimoire_fields(merge_request['created_at'], MERGE_TYPE))

        # due to backtrack compatibility, `is_gitlabcomments_*` is replaced with `is_gitlab_*`
        rich_mr.pop('is_gitlabcomments_{}'.format(MERGE_TYPE))
        rich_mr['is_gitlab_{}'.format(MERGE_TYPE)] = 1

        if self.sortinghat:
            item[self.get_field_date()] = rich_mr[self.get_field_date()]
            rich_mr.update(self.get_item_sh(item, self.mr_roles))

        return rich_mr

    @metadata
    def get_rich_item(self, item):
        rich_item = {}

        if item['category'] == 'issue':
            rich_item = self.__get_rich_issue(item)
        elif item['category'] == 'merge_request':
            rich_item = self.__get_rich_merge(item)
        else:
            logger.error("[gitlabcomments] rich item not defined for "
                         "GitLab Comments category {}".format(item['category']))

        self.add_repository_labels(rich_item)
        self.add_metadata_filter_raw(rich_item)

        return rich_item

    def get_rich_issue_comments(self, comments, eitem):
        ecomments = []

        for comment in comments:
            ecomment = {}

            self.copy_raw_fields(self.RAW_FIELDS_COPY, eitem, ecomment)

            # Copy data from the enriched issue
            ecomment['issue_labels'] = eitem['issue_labels']
            ecomment['issue_id'] = eitem['issue_id']
            ecomment['issue_id_in_repo'] = eitem['issue_id_in_repo']
            ecomment['issue_url'] = eitem['issue_url']
            ecomment['issue_title'] = eitem['issue_title']
            ecomment['issue_state'] = eitem['issue_state']
            ecomment['issue_created_at'] = eitem['issue_created_at']
            ecomment['issue_updated_at'] = eitem['issue_updated_at']
            ecomment['issue_closed_at'] = eitem['issue_closed_at']
            ecomment['gitlab_repo'] = eitem['gitlab_repo']
            ecomment['repository'] = eitem['repository']
            ecomment['item_type'] = COMMENT_TYPE
            ecomment['sub_type'] = ISSUE_COMMENT_TYPE

            # Copy data from the raw comment
            ecomment['body'] = comment['body'][:self.KEYWORD_MAX_LENGTH]
            ecomment['body_analyzed'] = comment['body']
            ecomment['author_login'] = comment['author'].get('username', None)

            ecomment['url'] = '{}#note_{}'.format(eitem['issue_url'], comment['id'])

            # extract reactions and add it to enriched item
            ecomment.update(self.__get_reactions(comment))

            ecomment['comment_updated_at'] = comment['updated_at']

            # Add id info to allow to coexistence of items of different types in the same index
            ecomment['id'] = '{}_issue_comment_{}'.format(eitem['id'], comment['id'])
            ecomment.update(self.get_grimoire_fields(comment['updated_at'], ISSUE_COMMENT_TYPE))

            # due to backtrack compatibility, `is_gitlabcomments_*` is replaced with `is_gitlab_*`
            ecomment.pop('is_gitlabcomments_{}'.format(ISSUE_COMMENT_TYPE))
            ecomment['is_gitlab_{}'.format(ISSUE_COMMENT_TYPE)] = 1
            ecomment['is_gitlab_comment'] = 1

            if self.sortinghat:
                ecomment.update(self.get_item_sh(comment, self.comment_roles, 'updated_at'))

            if self.prjs_map:
                ecomment.update(self.get_item_project(ecomment))

            if 'project' in eitem:
                ecomment['project'] = eitem['project']

            self.add_repository_labels(ecomment)
            self.add_metadata_filter_raw(ecomment)
            self.add_gelk_metadata(ecomment)

            ecomments.append(ecomment)

        return ecomments

    def get_rich_merge_reviews(self, comments, eitem):
        ecomments = []

        for comment in comments:
            ecomment = {}

            self.copy_raw_fields(self.RAW_FIELDS_COPY, eitem, ecomment)

            # Copy data from the enriched merge request
            ecomment['merge_labels'] = eitem['merge_labels']
            ecomment['merge_id'] = eitem['merge_id']
            ecomment['merge_id_in_repo'] = eitem['merge_id_in_repo']
            ecomment['merge_title'] = eitem['merge_title']
            ecomment['merge_url'] = eitem['merge_url']
            ecomment['merge_state'] = eitem['merge_state']
            ecomment['merge_created_at'] = eitem['merge_created_at']
            ecomment['merge_updated_at'] = eitem['merge_updated_at']
            ecomment['merge_merged_at'] = eitem['merge_merged_at']
            ecomment['merge_closed_at'] = eitem['merge_closed_at']
            ecomment['merge_status'] = eitem['merge_status']
            ecomment['gitlab_repo'] = eitem['gitlab_repo']
            ecomment['repository'] = eitem['repository']
            ecomment['item_type'] = COMMENT_TYPE
            ecomment['sub_type'] = MERGE_COMMENT_TYPE

            # Copy data from the raw comment
            ecomment['body'] = comment['body'][:self.KEYWORD_MAX_LENGTH]
            ecomment['body_analyzed'] = comment['body']
            ecomment['author_login'] = comment['author'].get('username', None)

            ecomment['url'] = '{}#note_{}'.format(eitem['merge_url'], comment['id'])

            # extract reactions and add it to enriched item
            ecomment.update(self.__get_reactions(comment))

            ecomment['comment_updated_at'] = comment['updated_at']

            # Add id info to allow to coexistence of items of different types in the same index
            ecomment['id'] = '{}_merge_comment_{}'.format(eitem['id'], comment['id'])
            ecomment.update(self.get_grimoire_fields(comment['updated_at'], MERGE_COMMENT_TYPE))

            # due to backtrack compatibility, `is_gitlabcomments_*` is replaced with `is_gitlab_*`
            ecomment.pop('is_gitlabcomments_{}'.format(MERGE_COMMENT_TYPE))
            ecomment['is_gitlab_{}'.format(MERGE_COMMENT_TYPE)] = 1
            ecomment['is_gitlab_comment'] = 1

            if self.sortinghat:
                ecomment.update(self.get_item_sh(comment, self.comment_roles, 'updated_at'))

            if self.prjs_map:
                ecomment.update(self.get_item_project(ecomment))

            if 'project' in eitem:
                ecomment['project'] = eitem['project']

            self.add_repository_labels(ecomment)
            self.add_metadata_filter_raw(ecomment)
            self.add_gelk_metadata(ecomment)

            ecomments.append(ecomment)

        return ecomments

    def enrich_issue(self, item, eitem):
        eitems = []

        comments = item['data'].get('notes_data', [])
        if comments:
            rich_item_comments = self.get_rich_issue_comments(comments, eitem)
            eitems.extend(rich_item_comments)

        return eitems

    def enrich_merge(self, item, eitem):
        eitems = []

        comments = item['data'].get('notes_data', [])
        if comments:
            rich_item_comments = self.get_rich_merge_reviews(comments, eitem)
            eitems.extend(rich_item_comments)

        return eitems

    def enrich_items(self, ocean_backend):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        for item in ocean_backend.fetch():
            eitems = []

            eitem = self.get_rich_item(item)
            items_to_enrich.append(eitem)

            if item['category'] == ISSUE_TYPE:
                eitems = self.enrich_issue(item, eitem)
            elif item['category'] == MERGE_TYPE:
                eitems = self.enrich_merge(item, eitem)

            items_to_enrich.extend(eitems)

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
            logger.error("%s/%s missing items for GitLab Comments", str(missing), str(num_items))

        logger.info("%s items inserted for GitLab Comments", str(num_items))

        return num_items
