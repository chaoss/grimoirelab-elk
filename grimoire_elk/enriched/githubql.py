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

from elasticsearch import Elasticsearch as ES, RequestsHttpConnection

from .enrich import Enrich, metadata
from .utils import anonymize_url, get_time_diff_days
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

    event_roles = ['actor', 'reporter']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = [self.enrich_duration_analysis]

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_author(self):
        return "actor"

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "grimoire_creation_date"

    def get_identities(self, item):
        """Return the identities from an item"""

        event = item['data']
        event_actor = event.get("actor", None)
        if event_actor:
            identity = self.get_sh_identity(event_actor)
            if identity:
                yield identity

        issue = event['issue']
        issue_reporter = issue.get("user", None)
        if issue_reporter:
            identity = self.get_sh_identity(issue_reporter)
            if identity:
                yield identity

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if isinstance(item, dict) and 'data' in item:
            if identity_field == 'actor':
                user = item['data'][identity_field]
            elif identity_field == 'reporter':
                user = item['data']['issue']['user']

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

        # move the issue reporter to level of actor. This is needed to
        # allow `get_item_sh` adding SortingHat identities
        reporter = issue['user']
        item['data']['reporter'] = reporter

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

    def enrich_duration_analysis(self, ocean_backend, enrich_backend, start_event_type, target_attr,
                                 fltr_event_types, fltr_attr=None, page_size=200):
        """The purpose of this study is to calculate the duration between two GitHub events. It requires
        a start event type (e.g., UnlabeledEvent or MovedColumnsInProjectEvent), which is used to
        retrieve for each issue all events of that type. For each issue event obtained, the first
        previous event of one of the types defined at `fltr_event_types` is returned, and used to
        calculate the duration (in days) between the two events. Optionally, an additional filter
        can be defined to retain the events that share a given property (e.g., a specific label,
        the name of project board). Finally, the duration and the previous event uuid are added to
        the start event via the attributes `duration_from_previous_event` and `previous_event_uuid`.

        This study is executed in a incremental way, thus only the start events that don't
        include the attribute `duration_from_previous_event` are retrieved and processed.

        The examples below show how to activate the study by modifying the setup.cfg. The first example
        calculates the duration between Unlabeled and Labeled events per label. The second example
        calculates the duration between the MovedColumnsInProject and AddedToProject events per
        column in each board

        ```
        [githubql]
        ...
        studies = [enrich_duration_analysis:label, enrich_duration_analysis:kanban]

        [enrich_duration_analysis:kanban]
        start_event_type = MovedColumnsInProjectEvent
        fltr_attr = board_name
        target_attr = board_column
        fltr_event_types = [MovedColumnsInProjectEvent, AddedToProjectEvent]

        [enrich_duration_analysis:label]
        start_event_type = UnlabeledEvent
        target_attr = label
        fltr_attr = label
        fltr_event_types = [LabeledEvent]
        ```

        :param ocean_backend: backend from which to read the raw items
        :param enrich_backend:  backend from which to read the enriched items
        :param start_event_type: the type of the start event (e.g., UnlabeledEvent)
        :param target_attr: the attribute returned from the events (e.g., label)
        :param fltr_event_types: a list of event types to select the previous events (e.g., LabeledEvent)
        :param fltr_attr: an optional attribute to filter in the events with a given property (e.g., label)
        :param page_size: number of events without `duration_from_previous_event` per page
        """
        data_source = enrich_backend.__class__.__name__.split("Enrich")[0].lower()
        log_prefix = "[{}] Duration analysis".format(data_source)
        logger.info("{} starting study {}".format(log_prefix, anonymize_url(self.elastic.index_url)))

        es_in = ES([enrich_backend.elastic_url], retry_on_timeout=True, timeout=100,
                   verify_certs=self.elastic.requests.verify, connection_class=RequestsHttpConnection)
        in_index = enrich_backend.elastic.index

        # get all start events that don't have the attribute `duration_from_previous_event`
        query_start_event_type = {
            "query": {
                "bool": {
                    "filter": {
                        "term": {
                            "event_type": start_event_type
                        }
                    },
                    "must_not": {
                        "exists": {
                            "field": "duration_from_previous_event"
                        }
                    }
                }
            },
            "_source": [
                "uuid", "issue_url_id", "grimoire_creation_date", target_attr
            ],
            "sort": [
                {
                    "grimoire_creation_date": {
                        "order": "asc"
                    }
                }
            ],
            "size": page_size
        }

        if fltr_attr:
            query_start_event_type['_source'].append(fltr_attr)

        start_event_types = es_in.search(index=in_index, body=query_start_event_type, scroll='5m')

        sid = start_event_types['_scroll_id']
        scroll_size = len(start_event_types['hits']['hits'])

        while scroll_size > 0:

            # for each event, retrieve the previous event included in `fltr_event_types`
            for start_event in start_event_types['hits']['hits']:
                start_event = start_event['_source']
                start_uuid = start_event['uuid']
                start_issue_url_id = start_event['issue_url_id']
                start_date_event = start_event['grimoire_creation_date']

                query_previous_events = {
                    "size": 1,
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "term": {
                                        "issue_url_id": start_issue_url_id
                                    }
                                },
                                {
                                    "terms": {
                                        "event_type": fltr_event_types
                                    }
                                },
                                {
                                    "range": {
                                        "grimoire_creation_date": {
                                            "lt": start_date_event
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    "_source": [
                        "uuid", "grimoire_creation_date", target_attr
                    ],
                    "sort": [
                        {
                            "grimoire_creation_date": {
                                "order": "desc"
                            }
                        }
                    ]
                }

                if fltr_attr:
                    _fltr = {
                        "term": {
                            fltr_attr: start_event[fltr_attr]
                        }
                    }

                    query_previous_events['query']['bool']['filter'].append(_fltr)
                    query_start_event_type['_source'].append(fltr_attr)

                previous_events = es_in.search(index=in_index, body=query_previous_events)['hits']['hits']
                if not previous_events:
                    continue

                previous_event = previous_events[0]['_source']
                previous_event_date = previous_event['grimoire_creation_date']
                previous_event_uuid = previous_event['uuid']
                duration = get_time_diff_days(previous_event_date, start_date_event)

                painless_code = "ctx._source.duration_from_previous_event=params.duration;" \
                                "ctx._source.previous_event_uuid=params.uuid"

                add_previous_event_query = {
                    "script": {
                        "source": painless_code,
                        "lang": "painless",
                        "params": {
                            "duration": duration,
                            "uuid": previous_event_uuid
                        }
                    },
                    "query": {
                        "bool": {
                            "filter": {
                                "term": {
                                    "uuid": start_uuid
                                }
                            }
                        }
                    }
                }
                r = es_in.update_by_query(index=in_index, body=add_previous_event_query, conflicts='proceed')
                if r['failures']:
                    logger.error("{} Error while executing study {}".format(log_prefix,
                                                                            anonymize_url(self.elastic.index_url)))
                    logger.error(str(r['failures'][0]))
                    return

            start_event_types = es_in.scroll(scroll_id=sid, scroll='2m')
            # update the scroll ID
            sid = start_event_types['_scroll_id']
            # get the number of results that returned in the last scroll
            scroll_size = len(start_event_types['hits']['hits'])

        logger.info("{} ending study {}".format(log_prefix, anonymize_url(self.elastic.index_url)))
