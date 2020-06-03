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
#   Ria Gupta <ria18405@iiitd.ac.in>
#

import logging

from .enrich import Enrich, metadata
from .scmsenrich import ScmsEnrich
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
               "id": {
                    "type": "keyword"
               }
            }
        }
        """

        return {"items": mapping}


class ScmsGitHubEnrich(ScmsEnrich):

    mapping = Mapping


    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []

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

        identity['name'] = user.get('login', None)
        identity['username'] = user.get('username', None)
        identity['email'] = user.get('email', None)

        return identity


    def get_field_unique_id(self):
        return "id"


    @metadata
    def get_rich_item(self, item):

        rich_item = {}
        if item['category'] == 'issue':
            rich_item = self.__get_rich_issue(item)
        elif item['category'] == 'pull_request':
            rich_item = self.__get_rich_pull(item)
        else:
            logger.error("[github] rich item not defined for GitHub category {}".format(
                         item['category']))

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

            # Add id info to allow to coexistence of items of different types in the same index
            ecomment['id'] = '{}_issue_comment_{}'.format(eitem['id'], comment['id'])
            # Copy data from the raw comment
            ecomment['context']=eitem['issue_title']
            ecomment['body'] = comment['body'][:self.KEYWORD_MAX_LENGTH]
            ecomment.update(self.get_grimoire_fields(comment['updated_at'], ISSUE_COMMENT_TYPE))

            ecomments.append(ecomment)

        return ecomments

    def enrich_pulls(self, item, eitem):
        eitems = []

        comments = item['data'].get('review_comments_data', [])
        if comments:
            rich_item_comments = self.get_rich_pull_reviews(comments, eitem)
            eitems.extend(rich_item_comments)

        return eitems

    def get_rich_pull_reviews(self, comments, eitem):
        ecomments = []

        for comment in comments:
            ecomment = {}

            self.copy_raw_fields(self.RAW_FIELDS_COPY, eitem, ecomment)

            # Add id info to allow to coexistence of items of different types in the same index
            ecomment['id'] = '{}_review_comment_{}'.format(eitem['id'], comment['id'])
            # Copy data from the raw comment
            ecomment['context'] = eitem['pr_title']
            ecomment['body'] = comment['body'][:self.KEYWORD_MAX_LENGTH]
            ecomment.update(self.get_grimoire_fields(comment['updated_at'], REVIEW_COMMENT_TYPE))
            ecomments.append(ecomment)

        return ecomments

    def enrich_items(self, ocean_backend):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        for item in ocean_backend.fetch():
            eitems = []

            eitem = self.get_rich_item(item)
            # items_to_enrich.append(eitem)
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


    def __get_rich_pull(self, item):
        rich_pr = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_pr)
        # The real data
        pull_request = item['data']
        rich_pr['id'] = pull_request['id']
        rich_pr['pr_title'] = pull_request['title']
        rich_pr['item_type'] = PULL_TYPE
        rich_pr.update(self.get_grimoire_fields(pull_request['created_at'], PULL_TYPE))

        return rich_pr

    def __get_rich_issue(self, item):
        rich_issue = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_issue)
        # The real data
        issue = item['data']
        rich_issue['id'] = issue['id']
        rich_issue['issue_title'] = issue['title']
        rich_issue['item_type'] = ISSUE_TYPE
        rich_issue.update(self.get_grimoire_fields(issue['created_at'], ISSUE_TYPE))

        return rich_issue
