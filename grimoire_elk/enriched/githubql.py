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
#   Valerio Cosentino <valcos@bitergia.com>
#

import logging
import re

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping

GITHUB = 'https://github.com/'
LABEL_EVENTS = ['LabeledEvent', 'UnlabeledEvent']
PROJECT_EVENTS = ['AddedToProjectEvent', 'MovedColumnsInProjectEvent', 'RemovedFromProjectEvent']
REFERENCE_EVENTS = ['CrossReferencedEvent']
CLOSED_EVENTS = ['ClosedEvent']

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
               "issue_state": {
                   "type": "keyword"
               },
               "title_analyzed": {
                 "type": "text",
                 "index": true
               }
            }
        }
        """

        return {"items": mapping}


class GitHubQLEnrich(Enrich):

    mapping = Mapping

    event_roles = ['actor']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_author(self):
        return "actor"

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "grimoire_creation_date"

    def get_identities(self, item):
        """Return the identities from an item"""

        item = item['data']
        identity_attr = "actor"
        if item[identity_attr] and identity_attr in item:
            user = self.get_sh_identity(item[identity_attr])
            if user:
                yield user

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]

        if not user:
            return identity

        identity['username'] = user['login']
        identity['email'] = None
        identity['name'] = None

        return identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        return repo

    @metadata
    def get_rich_item(self, item):

        rich_item = self.__get_rich_event(item)

        self.add_repository_labels(rich_item)
        self.add_metadata_filter_raw(rich_item)

        return rich_item

    def __get_rich_event(self, item):
        rich_event = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_event)

        event = item['data']
        issue = item['data']['issue']
        actor = item['data']['actor']

        rich_event['event_type'] = event['eventType']
        rich_event['created_at'] = event['createdAt']
        rich_event['actor_username'] = actor['login'] if actor else None
        rich_event['repository'] = self.get_project_repository(rich_event)
        rich_event['pull_request'] = True
        rich_event['item_type'] = 'pull request'
        if 'head' not in issue.keys() and 'pull_request' not in issue.keys():
            rich_event['pull_request'] = False
            rich_event['item_type'] = 'issue'

        rich_event['issue_id'] = issue['id']
        rich_event['issue_id_in_repo'] = issue['html_url'].split("/")[-1]
        rich_event['title'] = issue['title']
        rich_event['title_analyzed'] = issue['title']
        rich_event['issue_state'] = issue['state']
        rich_event['issue_created_at'] = issue['created_at']
        rich_event['issue_updated_at'] = issue['updated_at']
        rich_event['issue_closed_at'] = issue['closed_at']
        rich_event['issue_url'] = issue['html_url']
        labels = []
        [labels.append(label['name']) for label in issue['labels'] if 'labels' in issue]
        rich_event['issue_labels'] = labels

        rich_event['github_repo'] = rich_event['repository'].replace(GITHUB, '')
        rich_event['github_repo'] = re.sub('.git$', '', rich_event['github_repo'])
        rich_event["issue_url_id"] = rich_event['github_repo'] + "/issues/" + rich_event['issue_id_in_repo']

        if rich_event['event_type'] in LABEL_EVENTS:
            label = event['label']
            rich_event['label'] = label['name']
            rich_event['label_description'] = label['description']
            rich_event['label_is_default'] = label['isDefault']
            rich_event['label_created_at'] = label['createdAt']
            rich_event['label_updated_at'] = label['updatedAt']
        elif rich_event['event_type'] in CLOSED_EVENTS:
            closer = event['closer']
            rich_event['label'] = rich_event['issue_labels']
            if closer and closer['type'] == 'PullRequest':
                rich_event['closer_event_url'] = event['url']
                rich_event['closer_type'] = closer['type']
                rich_event['closer_number'] = closer['number']
                rich_event['closer_url'] = closer['url']
                rich_event['closer_repo'] = '/'.join(closer['url'].replace(GITHUB, '').split('/')[:-2])
                rich_event['closer_created_at'] = closer['createdAt']
                rich_event['closer_updated_at'] = closer['updatedAt']
                rich_event['closer_closed_at'] = closer['closedAt']
                rich_event['closer_closed'] = closer['closed']
                rich_event['closer_merged'] = closer.get('merged', None)
        elif rich_event['event_type'] in REFERENCE_EVENTS:
            source = event['source']
            rich_event['reference_cross_repo'] = event['isCrossRepository']
            rich_event['reference_will_close_target'] = event['willCloseTarget']
            rich_event['reference_event_url'] = event['url']
            rich_event['reference_source_type'] = source['type']
            rich_event['reference_source_number'] = source['number']
            rich_event['reference_source_url'] = source['url']
            rich_event['reference_source_repo'] = '/'.join(source['url'].replace(GITHUB, '').split('/')[:-2])
            rich_event['reference_source_created_at'] = source['createdAt']
            rich_event['reference_source_updated_at'] = source['updatedAt']
            rich_event['reference_source_closed_at'] = source['closedAt']
            rich_event['reference_source_closed'] = source['closed']
            rich_event['reference_source_merged'] = source.get('merged', None)
        elif rich_event['event_type'] in PROJECT_EVENTS:
            project = event['project']
            rich_event['board_column'] = event['projectColumnName']
            rich_event['board_name'] = project['name']
            rich_event['board_url'] = project['url']
            rich_event['board_created_at'] = project['createdAt']
            rich_event['board_updated_at'] = project['updatedAt']
            rich_event['board_closed_at'] = project['closedAt']
            rich_event['board_state'] = project['state'].lower()

            # only for events of type MovedColumnsInProjectEvent
            if 'previousProjectColumnName' in event:
                rich_event['board_previous_column'] = event['previousProjectColumnName']
        else:
            logger.warning("[github] event {} not processed".format(rich_event['event_type']))

        if self.prjs_map:
            rich_event.update(self.get_item_project(rich_event))

        rich_event.update(self.get_grimoire_fields(event['createdAt'], "issue"))
        item[self.get_field_date()] = rich_event[self.get_field_date()]
        rich_event.update(self.get_item_sh(item, self.event_roles))

        # Copy SH actor info to author equivalent attributes
        rich_event['author_id'] = rich_event.get('actor_id', None)
        rich_event['author_uuid'] = rich_event.get('actor_uuid', None)
        rich_event['author_name'] = rich_event.get('actor_name', None)
        rich_event['author_user_name'] = rich_event.get('actor_user_name', None)
        rich_event['author_domain'] = rich_event.get('actor_domain', None)
        rich_event['author_gender'] = rich_event.get('actor_gender', None)
        rich_event['author_gender_acc'] = rich_event.get('actor_gender_acc', None)
        rich_event['author_org_name'] = rich_event.get('actor_org_name', None)
        rich_event['author_bot'] = rich_event.get('actor_bot', None)
        rich_event['author_multi_org_names'] = rich_event.get('actor_multi_org_names', None)

        return rich_event
