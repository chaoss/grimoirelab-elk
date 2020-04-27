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
#   Animesh Kumar <animuz111@gmail.com>
#

import logging
from datetime import datetime
from grimoirelab_toolkit.datetime import (unixtime_to_datetime,
                                          datetime_utcnow)
from .utils import get_time_diff_days
from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping

logger = logging.getLogger(__name__)

MAX_SIZE_BULK_ENRICHED_ITEMS = 200
ISSUE_TYPE = 'issue'
COMMENT_TYPE = 'comment'
ISSUE_COMMENT_TYPE = 'issue_comment'


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


class PagureEnrich(Enrich):
    mapping = Mapping

    comment_roles = ['user']
    issue_roles = ['user', 'assignee']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_author(self):
        return "user"

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """

        return "grimoire_creation_date"

    def get_field_unique_id(self):
        return "id"

    def get_identities(self, item):
        """ Return the identities from an item """

        item = item['data']
        for identity in self.issue_roles:
            if item[identity]:
                user = self.get_sh_identity(item[identity])
                if user:
                    yield user

        # Comments
        if 'comments' in item:
            for comment in item['comments']:
                if 'user' in comment:
                    user = comment.get('user', None)
                    identity = self.get_sh_identity(user)
                    yield identity

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]
        elif identity_field:
            user = item[identity_field]

        if not user:
            return identity

        identity['email'] = None
        identity['name'] = user.get('fullname', None)
        identity['username'] = user.get('name', None)
        return identity

    def add_gelk_metadata(self, eitem):
        eitem['metadata__gelk_version'] = self.gelk_version
        eitem['metadata__gelk_backend_name'] = self.__class__.__name__
        eitem['metadata__enriched_on'] = datetime_utcnow().isoformat()

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        return repo

    def get_time_to_first_attention(self, item):
        """Get the first date at which a comment was made to the issue by someone
        other than the user who created the issue
        """
        comment_dates = [unixtime_to_datetime(float(comment['date_created'])).isoformat() for comment
                         in item['comments'] if item['user']['name'] != comment['user']['name']]
        if comment_dates:
            return min(comment_dates)
        return None

    def __get_reactions(self, item):
        reactions = {}

        item_reactions = item.get('reactions', {})
        for reaction in item_reactions:
            reactions['reaction_{}'.format(reaction)] = item_reactions[reaction]

        return reactions

    def get_rich_issue_comments(self, comments, eitem):
        ecomments = []

        for comment in comments:
            ecomment = {}

            self.copy_raw_fields(self.RAW_FIELDS_COPY, eitem, ecomment)

            # Copy data from the enriched issue
            ecomment['issue_id'] = eitem['id']
            ecomment['issue_title'] = eitem['title']
            ecomment['issue_status'] = eitem['status']
            ecomment['issue_created_at'] = eitem['created_at']
            ecomment['issue_updated_at'] = eitem['updated_at']
            ecomment['issue_closed_at'] = eitem['closed_at']
            ecomment['issue_pull_requests'] = eitem['related_prs']
            ecomment['item_type'] = COMMENT_TYPE

            # Copy data from the raw comment
            ecomment['body'] = comment['comment'][:self.KEYWORD_MAX_LENGTH]
            ecomment['body_analyzed'] = comment['comment']

            # extract reactions and add it to enriched item
            ecomment.update(self.__get_reactions(comment))

            if 'edited_on' in comment and comment['edited_on']:
                ecomment['comment_updated_at'] = unixtime_to_datetime(float(comment['edited_on'])).isoformat()
                editor = comment['editor']
                ecomment['comment_updated_by_name'] = editor.get('fullname', None)
                ecomment['comment_updated_by_username'] = editor.get('name', None)

            ecomment['comment_created_at'] = unixtime_to_datetime(float(comment['date_created'])).isoformat()
            ecomment['comment_changed_at'] = \
                ecomment['comment_updated_at'] if 'comment_updated_at' in ecomment else ecomment['comment_created_at']
            ecomment['notification'] = comment['notification']
            ecomment['parent'] = comment['parent']

            author = comment.get('user', None)
            if author:
                ecomment['comment_author_username'] = author.get('name', None)
                ecomment['comment_author_name'] = author.get('fullname', None)

            # Add id info to allow to coexistence of items of different types in the same index
            ecomment['id'] = '{}_issue_comment_{}'.format(eitem['id'], comment['id'])
            ecomment.update(self.get_grimoire_fields(ecomment['comment_changed_at'], ISSUE_COMMENT_TYPE))

            if self.sortinghat:
                comment['changed_at'] = ecomment['comment_changed_at']
                ecomment.update(self.get_item_sh(comment, self.comment_roles, 'changed_at'))

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

        comments = item['data'].get('comments', [])
        if comments:
            rich_item_comments = self.get_rich_issue_comments(comments, eitem)
            eitems.extend(rich_item_comments)

        return eitems

    def enrich_items(self, ocean_backend):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        for item in ocean_backend.fetch():
            eitem = self.get_rich_item(item)

            if item['category'] == ISSUE_TYPE:
                items_to_enrich.append(eitem)
                eitems = self.enrich_issue(item, eitem)
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
            logger.error("%s/%s missing items for Pagure", str(missing), str(num_items))

        logger.info("%s/%s items inserted for Pagure", str(ins_items), str(num_items))

        return num_items

    @metadata
    def get_rich_item(self, item):

        rich_item = {}
        if item['category'] == 'issue':
            rich_item = self.__get_rich_issue(item)
            self.add_repository_labels(rich_item)
            self.add_metadata_filter_raw(rich_item)
        else:
            logger.error("[pagure] rich item not defined for Pagure category {}".format(item['category']))

        return rich_item

    def __get_rich_issue(self, item):

        rich_issue = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_issue)

        # The real data
        issue = item['data']

        rich_issue['author_username'] = None
        rich_issue['author_name'] = None
        if 'user' in issue and issue['user']:
            author = issue['user']
            rich_issue['author_username'] = author.get('name', None)
            rich_issue['author_name'] = author.get('fullname', None)

        rich_issue['assignee_username'] = None
        rich_issue['assignee_name'] = None
        if 'assignee' in issue and issue['assignee']:
            assignee = issue['assignee']
            rich_issue['assignee_username'] = assignee.get('name', None)
            rich_issue['assignee_name'] = assignee.get('fullname', None)

        if 'closed_by' in issue and issue['closed_by']:
            closed_by = issue['closed_by']
            rich_issue['closed_by_username'] = closed_by.get('name', None)
            rich_issue['closed_by_name'] = closed_by.get('fullname', None)

        if 'closed_at' in issue and issue['closed_at']:
            rich_issue['closed_at'] = unixtime_to_datetime(float(issue['closed_at'])).isoformat()
        else:
            rich_issue['closed_at'] = None

        rich_issue['id'] = issue['id']
        rich_issue['title'] = issue['title']
        rich_issue['title_analyzed'] = issue['title']
        rich_issue['content'] = issue['content']
        rich_issue['status'] = issue['status']
        rich_issue['close_status'] = issue['close_status']
        rich_issue['num_comments'] = len(issue['comments'])
        rich_issue['created_at'] = unixtime_to_datetime(float(issue['date_created'])).isoformat()
        rich_issue['updated_at'] = unixtime_to_datetime(float(issue['last_updated'])).isoformat()
        rich_issue['blocks'] = issue['blocks']
        rich_issue['private'] = issue['private']
        rich_issue['priority'] = issue['priority']
        rich_issue['milestone'] = issue['milestone']
        rich_issue['related_prs'] = issue['related_prs']
        rich_issue['tags'] = issue['tags']
        rich_issue['custom_fields'] = issue['custom_fields']
        rich_issue['item_type'] = ISSUE_TYPE

        rich_issue['time_to_close_days'] = \
            get_time_diff_days(rich_issue['created_at'], rich_issue['closed_at'])

        if issue['status'] != 'Closed':
            rich_issue['time_open_days'] = \
                get_time_diff_days(rich_issue['created_at'], datetime.utcnow())
        else:
            rich_issue['time_open_days'] = rich_issue['time_to_close_days']

        if self.prjs_map:
            rich_issue.update(self.get_item_project(rich_issue))

        rich_issue['time_to_first_attention'] = None
        if len(issue['comments']) != 0:
            rich_issue['time_to_first_attention'] = \
                get_time_diff_days(rich_issue['created_at'], self.get_time_to_first_attention(issue))

        rich_issue.update(self.get_grimoire_fields(rich_issue['updated_at'], "issue"))

        if self.sortinghat:
            item[self.get_field_date()] = rich_issue[self.get_field_date()]
            rich_issue.update(self.get_item_sh(item, self.issue_roles))

        return rich_issue
