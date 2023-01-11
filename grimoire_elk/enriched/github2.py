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

from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)

from .utils import get_time_diff_days

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping


MAX_SIZE_BULK_ENRICHED_ITEMS = 200
GEOLOCATION_INDEX = '/github/'
GITHUB = 'https://github.com/'
ISSUE_TYPE = 'issue'
PULL_TYPE = 'pull_request'
COMMENT_TYPE = 'comment'
ISSUE_COMMENT_TYPE = 'issue_comment'
REVIEW_COMMENT_TYPE = 'review_comment'
REPOSITORY_TYPE = 'repository'

USER_NOT_AVAILABLE = {'organizations': []}
DELETED_USER_LOGIN = 'ghost'
DELETED_USER_NAME = 'Deleted user'

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
               "merge_author_geolocation": {
                   "type": "geo_point"
               },
               "assignee_geolocation": {
                   "type": "geo_point"
               },
               "issue_state": {
                   "type": "keyword"
               },
               "pull_state": {
                   "type": "keyword"
               },
               "user_geolocation": {
                   "type": "geo_point"
               },
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


class GitHubEnrich2(Enrich):

    mapping = Mapping

    comment_roles = ['user_data']
    issue_roles = ['assignee_data', 'user_data']
    pr_roles = ['merged_by_data', 'user_data']
    roles = ['assignee_data', 'merged_by_data', 'user_data']

    def __init__(self, db_sortinghat=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', db_path=None,
                 db_port=None, db_ssl=False):
        super().__init__(db_sortinghat=db_sortinghat, json_projects_map=json_projects_map,
                         db_user=db_user, db_password=db_password, db_host=db_host,
                         db_port=db_port, db_path=db_path, db_ssl=db_ssl)

        self.studies = []
        self.studies.append(self.enrich_geolocation)
        self.studies.append(self.enrich_feelings)
        self.studies.append(self.enrich_extra_data)
        self.studies.append(self.enrich_demography)

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_author(self):
        return "user_data"

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "grimoire_creation_date"

    def get_identities(self, item):
        """Return the identities from an item"""

        category = item['category']
        item = item['data']
        comments_attr = None
        if category == "issue":
            identity_types = ['user', 'assignee']
            comments_attr = 'comments_data'
        elif category == "pull_request":
            identity_types = ['user', 'merged_by']
            comments_attr = 'review_comments_data'
        else:
            identity_types = []

        for identity in identity_types:
            identity_attr = identity + "_data"
            if item[identity] and identity_attr in item:
                # In user_data we have the full user data
                user = self.get_sh_identity(item[identity_attr])
                if user:
                    yield user

        comments = item.get(comments_attr, [])
        for comment in comments:
            user = self.get_sh_identity(comment['user_data'])
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

        identity['name'] = user.get('name', user.get('login', None))
        identity['email'] = user.get('email', None)
        identity['username'] = user.get('username', user.get('login', None))

        return identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        return repo

    def get_time_to_first_attention(self, item):
        """Get the first date at which a comment or reaction was made to the issue by someone
        other than the user who created the issue
        """
        dates = []
        deleted_user_login = {'login': DELETED_USER_LOGIN}

        for comment in item['comments_data']:
            # Add deleted (ghost) user
            if not comment['user']:
                comment['user'] = deleted_user_login

            # skip comments of the issue creator
            if item['user']['login'] == comment['user']['login']:
                continue

            dates.append(str_to_datetime(comment['created_at']))

        for reaction in item['reactions_data']:
            # Add deleted (ghost) user
            if not reaction['user']:
                reaction['user'] = deleted_user_login

            # skip reactions of the issue creator
            if item['user']['login'] == reaction['user']['login']:
                continue

            dates.append(str_to_datetime(reaction['created_at']))

        if dates:
            return min(dates)

        return None

    def get_time_to_merge_request_response(self, item):
        """Get the first date at which a review was made on the PR by someone
        other than the user who created the PR
        """
        review_dates = []
        for comment in item['review_comments_data']:
            # Add deleted (ghost) user
            if not comment['user']:
                comment['user'] = {'login': DELETED_USER_LOGIN}

            # skip comments of the pull request creator
            if item['user']['login'] == comment['user']['login']:
                continue

            review_dates.append(str_to_datetime(comment['created_at']))

        if review_dates:
            return min(review_dates)

        return None

    def get_field_unique_id(self):
        return "id"

    def add_gelk_metadata(self, eitem):
        eitem['metadata__gelk_version'] = self.gelk_version
        eitem['metadata__gelk_backend_name'] = self.__class__.__name__
        eitem['metadata__enriched_on'] = datetime_utcnow().isoformat()

    @metadata
    def get_rich_item(self, item):

        rich_item = {}
        if item['category'] == 'issue':
            rich_item = self.__get_rich_issue(item)
        elif item['category'] == 'pull_request':
            rich_item = self.__get_rich_pull(item)
        elif item['category'] == 'repository':
            rich_item = self.__get_rich_repo(item)
        else:
            logger.error("[github] rich item not defined for GitHub category {}".format(
                         item['category']))

        self.add_repository_labels(rich_item)
        self.add_metadata_filter_raw(rich_item)
        return rich_item

    def enrich_issue(self, item, eitem):
        eitems = []

        comments = item['data'].get('comments_data', [])
        if comments:
            rich_item_comments = self.get_rich_issue_comments(comments, eitem)
            eitems.extend(rich_item_comments)

        return eitems

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
            ecomment['issue_pull_request'] = eitem['issue_pull_request']
            ecomment['github_repo'] = eitem['github_repo']
            ecomment['repository'] = eitem['repository']
            ecomment['item_type'] = COMMENT_TYPE
            ecomment['sub_type'] = ISSUE_COMMENT_TYPE

            # Copy data from the raw comment
            ecomment['body'] = comment['body'][:self.KEYWORD_MAX_LENGTH]
            ecomment['body_analyzed'] = comment['body']
            ecomment['url'] = comment['html_url']

            # extract reactions and add it to enriched item
            ecomment.update(self.__get_reactions(comment))

            ecomment['comment_updated_at'] = comment['updated_at']

            # Add id info to allow to coexistence of items of different types in the same index
            ecomment['id'] = '{}_issue_comment_{}'.format(eitem['id'], comment['id'])
            ecomment.update(self.get_grimoire_fields(comment['updated_at'], ISSUE_COMMENT_TYPE))
            # due to backtrack compatibility, `is_github2_*` is replaced with `is_github_*`
            ecomment.pop('is_github2_{}'.format(ISSUE_COMMENT_TYPE))
            ecomment['is_github_{}'.format(ISSUE_COMMENT_TYPE)] = 1
            ecomment['is_github_comment'] = 1

            # Add user_login
            user_data = comment.get('user_data', None)
            if not user_data:
                user_data = {
                    'login': DELETED_USER_LOGIN,
                    'name': DELETED_USER_NAME
                }
                comment['user_data'] = user_data
            ecomment['user_login'] = user_data['login']

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

    def enrich_pulls(self, item, eitem):
        eitems = []

        comments = item['data'].get('review_comments_data', [])
        reviews = item['data'].get('reviews_data', [])
        if comments:
            rich_item_comments = self.get_rich_pull_reviews(comments, eitem)
            eitems.extend(rich_item_comments)
        if reviews:
            rich_item_reviews = self.get_rich_pull_reviews(reviews, eitem)
            eitems.extend(rich_item_reviews)

        return eitems

    def get_rich_pull_reviews(self, comments, eitem):
        ecomments = []

        for comment in comments:
            # If the comment comes from a review is "Approve" or "Change requests"
            # there is a "submitted_at" instead of "updated_at"
            if 'updated_at' not in comment:
                comment['updated_at'] = comment['submitted_at']

            ecomment = {}

            self.copy_raw_fields(self.RAW_FIELDS_COPY, eitem, ecomment)

            # Review state
            ecomment['review_state'] = comment.get('state', '')

            # Copy data from the enriched pull
            ecomment['pull_labels'] = eitem['pull_labels']
            ecomment['pull_id'] = eitem['pull_id']
            ecomment['pull_id_in_repo'] = eitem['pull_id_in_repo']
            ecomment['issue_id_in_repo'] = eitem['issue_id_in_repo']
            ecomment['issue_title'] = eitem['issue_title']
            ecomment['issue_url'] = eitem['issue_url']
            ecomment['pull_url'] = eitem['pull_url']
            ecomment['pull_state'] = eitem['pull_state']
            ecomment['pull_created_at'] = eitem['pull_created_at']
            ecomment['pull_updated_at'] = eitem['pull_updated_at']
            ecomment['pull_merged_at'] = eitem['pull_merged_at']
            ecomment['pull_closed_at'] = eitem['pull_closed_at']
            ecomment['pull_merged'] = eitem['pull_merged']
            ecomment['pull_state'] = eitem['pull_state']
            ecomment['github_repo'] = eitem['github_repo']
            ecomment['repository'] = eitem['repository']
            ecomment['item_type'] = COMMENT_TYPE
            ecomment['sub_type'] = REVIEW_COMMENT_TYPE

            # Copy data from the raw comment
            ecomment['body'] = comment['body'][:self.KEYWORD_MAX_LENGTH]
            ecomment['body_analyzed'] = comment['body']
            ecomment['url'] = comment['html_url']

            # extract reactions and add it to enriched item
            ecomment.update(self.__get_reactions(comment))

            ecomment['comment_updated_at'] = comment['updated_at']
            ecomment['comment_created_at'] = comment.get('created_at', comment['updated_at'])

            # Add id info to allow to coexistence of items of different types in the same index
            ecomment['id'] = '{}_review_comment_{}'.format(eitem['id'], comment['id'])
            ecomment.update(self.get_grimoire_fields(comment['updated_at'], REVIEW_COMMENT_TYPE))
            # due to backtrack compatibility, `is_github2_*` is replaced with `is_github_*`
            ecomment.pop('is_github2_{}'.format(REVIEW_COMMENT_TYPE))
            ecomment['is_github_{}'.format(REVIEW_COMMENT_TYPE)] = 1
            ecomment['is_github_comment'] = 1

            # Add user_login
            user_data = comment.get('user_data', None)
            if not user_data:
                user_data = {
                    'login': DELETED_USER_LOGIN,
                    'name': DELETED_USER_NAME
                }
                comment['user_data'] = user_data
            ecomment['user_login'] = user_data['login']

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
            elif item['category'] == PULL_TYPE:
                eitems = self.enrich_pulls(item, eitem)

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
            logger.error("%s/%s missing items for GitHub", str(missing), str(num_items))
        else:
            logger.info("%s items inserted for GitHub", str(num_items))

        return num_items

    def __get_reactions(self, item):
        reactions = {}

        item_reactions = item.get('reactions', {})
        # remove reactions url
        item_reactions.pop('url', None)
        for reaction in item_reactions:
            if reaction == '-1':
                reaction_name = 'thumb_down'
            elif reaction == '+1':
                reaction_name = 'thumb_up'
            else:
                reaction_name = reaction

            reactions['reaction_{}'.format(reaction_name)] = item_reactions[reaction]

        return reactions

    def __get_rich_pull(self, item):
        rich_pr = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_pr)
        # The real data
        pull_request = item['data']

        rich_pr['time_to_close_days'] = \
            get_time_diff_days(pull_request['created_at'], pull_request['closed_at'])

        if pull_request['state'] != 'closed':
            rich_pr['time_open_days'] = \
                get_time_diff_days(pull_request['created_at'], datetime_utcnow().replace(tzinfo=None))
        else:
            rich_pr['time_open_days'] = rich_pr['time_to_close_days']

        rich_pr['user_login'] = pull_request['user']['login']

        user = pull_request.get('user_data', None)
        if user is not None and user:
            rich_pr['user_name'] = user['name']
            rich_pr['author_name'] = user['name']
            rich_pr["user_domain"] = self.get_email_domain(user['email']) if user['email'] else None
            rich_pr['user_org'] = user['company']
            rich_pr['user_location'] = user['location']
            rich_pr['user_geolocation'] = None
        else:
            rich_pr['user_name'] = None
            rich_pr["user_domain"] = None
            rich_pr['user_org'] = None
            rich_pr['user_location'] = None
            rich_pr['user_geolocation'] = None
            rich_pr['author_name'] = None

        merged_by = pull_request.get('merged_by_data', None)
        if merged_by and merged_by != USER_NOT_AVAILABLE:
            rich_pr['merge_author_login'] = merged_by['login']
            rich_pr['merge_author_name'] = merged_by['name']
            rich_pr["merge_author_domain"] = self.get_email_domain(merged_by['email']) if merged_by['email'] else None
            rich_pr['merge_author_org'] = merged_by['company']
            rich_pr['merge_author_location'] = merged_by['location']
            rich_pr['merge_author_geolocation'] = None
        else:
            rich_pr['merge_author_name'] = None
            rich_pr['merge_author_login'] = None
            rich_pr["merge_author_domain"] = None
            rich_pr['merge_author_org'] = None
            rich_pr['merge_author_location'] = None
            rich_pr['merge_author_geolocation'] = None

        rich_pr['id'] = pull_request['id']
        rich_pr['pull_id'] = pull_request['id']
        rich_pr['pull_id_in_repo'] = pull_request['html_url'].split("/")[-1]
        rich_pr['issue_id_in_repo'] = pull_request['html_url'].split("/")[-1]
        rich_pr['repository'] = self.get_project_repository(rich_pr)
        rich_pr['issue_title'] = pull_request['title']
        rich_pr['issue_title_analyzed'] = pull_request['title']
        rich_pr['pull_state'] = pull_request['state']
        rich_pr['pull_created_at'] = pull_request['created_at']
        rich_pr['pull_updated_at'] = pull_request['updated_at']
        rich_pr['pull_merged'] = pull_request['merged']
        rich_pr['pull_merged_at'] = pull_request['merged_at']
        rich_pr['pull_closed_at'] = pull_request['closed_at']
        rich_pr['url'] = pull_request['html_url']
        rich_pr['pull_url'] = pull_request['html_url']
        rich_pr['issue_url'] = pull_request['html_url']

        # extract reactions and add it to enriched item
        rich_pr.update(self.__get_reactions(pull_request))

        labels = []
        [labels.append(label['name']) for label in pull_request['labels'] if 'labels' in pull_request]
        rich_pr['pull_labels'] = labels

        rich_pr['item_type'] = PULL_TYPE

        rich_pr['github_repo'] = rich_pr['repository'].replace(GITHUB, '')
        rich_pr['github_repo'] = re.sub('.git$', '', rich_pr['github_repo'])

        # GMD code development metrics
        rich_pr['forks'] = pull_request['base']['repo']['forks_count']
        rich_pr['code_merge_duration'] = get_time_diff_days(pull_request['created_at'],
                                                            pull_request['merged_at'])
        rich_pr['num_review_comments'] = pull_request['review_comments']

        rich_pr['time_to_merge_request_response'] = None
        if pull_request['review_comments'] != 0:
            min_review_date = self.get_time_to_merge_request_response(pull_request)
            rich_pr['time_to_merge_request_response'] = \
                get_time_diff_days(str_to_datetime(pull_request['created_at']), min_review_date)

        if self.prjs_map:
            rich_pr.update(self.get_item_project(rich_pr))

        if 'project' in item:
            rich_pr['project'] = item['project']

        rich_pr.update(self.get_grimoire_fields(pull_request['created_at'], PULL_TYPE))
        # due to backtrack compatibility, `is_github2_*` is replaced with `is_github_*`
        rich_pr.pop('is_github2_{}'.format(PULL_TYPE))
        rich_pr['is_github_{}'.format(PULL_TYPE)] = 1

        if self.sortinghat:
            item[self.get_field_date()] = rich_pr[self.get_field_date()]
            rich_pr.update(self.get_item_sh(item, self.pr_roles))

        return rich_pr

    def __get_rich_issue(self, item):
        rich_issue = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_issue)
        # The real data
        issue = item['data']

        rich_issue['time_to_close_days'] = \
            get_time_diff_days(issue['created_at'], issue['closed_at'])

        if issue['state'] != 'closed':
            rich_issue['time_open_days'] = \
                get_time_diff_days(issue['created_at'], datetime_utcnow().replace(tzinfo=None))
        else:
            rich_issue['time_open_days'] = rich_issue['time_to_close_days']

        rich_issue['user_login'] = issue['user']['login']

        user = issue.get('user_data', None)
        if user is not None and user:
            rich_issue['user_name'] = user['name']
            rich_issue['author_name'] = user['name']
            rich_issue["user_domain"] = self.get_email_domain(user['email']) if user['email'] else None
            rich_issue['user_org'] = user['company']
            rich_issue['user_location'] = user['location']
            rich_issue['user_geolocation'] = None
        else:
            rich_issue['user_name'] = None
            rich_issue["user_domain"] = None
            rich_issue['user_org'] = None
            rich_issue['user_location'] = None
            rich_issue['user_geolocation'] = None
            rich_issue['author_name'] = None

        assignee = issue.get('assignee_data', None)
        if assignee and assignee != USER_NOT_AVAILABLE:
            rich_issue['assignee_login'] = assignee['login']
            rich_issue['assignee_name'] = assignee['name']
            rich_issue["assignee_domain"] = self.get_email_domain(assignee['email']) if assignee['email'] else None
            rich_issue['assignee_org'] = assignee['company']
            rich_issue['assignee_location'] = assignee['location']
            rich_issue['assignee_geolocation'] = None
        else:
            rich_issue['assignee_name'] = None
            rich_issue['assignee_login'] = None
            rich_issue["assignee_domain"] = None
            rich_issue['assignee_org'] = None
            rich_issue['assignee_location'] = None
            rich_issue['assignee_geolocation'] = None

        rich_issue['id'] = issue['id']
        rich_issue['issue_id'] = issue['id']
        rich_issue['issue_id_in_repo'] = issue['html_url'].split("/")[-1]
        rich_issue['repository'] = self.get_project_repository(rich_issue)
        rich_issue['issue_title'] = issue['title']
        rich_issue['issue_title_analyzed'] = issue['title']
        rich_issue['issue_state'] = issue['state']
        rich_issue['issue_created_at'] = issue['created_at']
        rich_issue['issue_updated_at'] = issue['updated_at']
        rich_issue['issue_closed_at'] = issue['closed_at']
        rich_issue['url'] = issue['html_url']
        rich_issue['issue_url'] = issue['html_url']

        # extract reactions and add it to enriched item
        rich_issue.update(self.__get_reactions(issue))

        labels = []
        [labels.append(label['name']) for label in issue['labels'] if 'labels' in issue]
        rich_issue['issue_labels'] = labels

        rich_issue['item_type'] = ISSUE_TYPE
        rich_issue['issue_pull_request'] = True
        if 'head' not in issue.keys() and 'pull_request' not in issue.keys():
            rich_issue['issue_pull_request'] = False

        rich_issue['github_repo'] = rich_issue['repository'].replace(GITHUB, '')
        rich_issue['github_repo'] = re.sub('.git$', '', rich_issue['github_repo'])

        if self.prjs_map:
            rich_issue.update(self.get_item_project(rich_issue))

        if 'project' in item:
            rich_issue['project'] = item['project']

        rich_issue['time_to_first_attention'] = None
        if issue['comments'] + issue['reactions']['total_count'] != 0:
            rich_issue['time_to_first_attention'] = \
                get_time_diff_days(str_to_datetime(issue['created_at']),
                                   self.get_time_to_first_attention(issue))

        rich_issue.update(self.get_grimoire_fields(issue['created_at'], ISSUE_TYPE))
        # due to backtrack compatibility, `is_github2_*` is replaced with `is_github_*`
        rich_issue.pop('is_github2_{}'.format(ISSUE_TYPE))
        rich_issue['is_github_{}'.format(ISSUE_TYPE)] = 1

        if self.sortinghat:
            item[self.get_field_date()] = rich_issue[self.get_field_date()]
            rich_issue.update(self.get_item_sh(item, self.issue_roles))

        return rich_issue

    def __get_rich_repo(self, item):
        rich_repo = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_repo)

        repo = item['data']

        rich_repo['id'] = str(repo['fetched_on'])
        rich_repo['forks_count'] = repo['forks_count']
        rich_repo['subscribers_count'] = repo['subscribers_count']
        rich_repo['stargazers_count'] = repo['stargazers_count']
        rich_repo['fetched_on'] = repo['fetched_on']
        rich_repo['url'] = repo['html_url']

        if self.prjs_map:
            rich_repo.update(self.get_item_project(rich_repo))

        rich_repo.update(self.get_grimoire_fields(item['metadata__updated_on'], REPOSITORY_TYPE))
        # due to backtrack compatibility, `is_github2_*` is replaced with `is_github_*`
        rich_repo.pop('is_github2_{}'.format(REPOSITORY_TYPE))
        rich_repo['is_github_{}'.format(REPOSITORY_TYPE)] = 1

        return rich_repo
