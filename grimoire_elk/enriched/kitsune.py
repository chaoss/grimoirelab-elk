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
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import json
import logging

from .enrich import Enrich, metadata
from .utils import get_time_diff_days, anonymize_url
from ..elastic_mapping import Mapping as BaseMapping
from grimoirelab_toolkit.datetime import str_to_datetime


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
                "content_analyzed": {
                  "type": "text",
                  "index": true
                },
                "tags_analyzed": {
                  "type": "text",
                  "index": true
                }
           }
        } """

        return {"items": mapping}


class KitsuneEnrich(Enrich):

    mappping = Mapping

    def get_field_author(self):
        return "creator"

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]
        elif identity_field in item:
            # for answers
            user = item[identity_field]

        identity['username'] = user['username']
        identity['email'] = None
        identity['name'] = user['username']
        if user['display_name']:
            identity['name'] = user['display_name']

        return identity

    def get_identities(self, item):
        """ Return the identities from an item """

        item = item['data']

        for identity in ['creator']:
            # Todo: questions has also involved and solved_by
            if identity in item and item[identity]:
                user = self.get_sh_identity(item[identity])
                yield user
            if 'answers_data' in item:
                for answer in item['answers_data']:
                    user = self.get_sh_identity(answer[identity])
                    yield user

    @metadata
    def get_rich_item(self, item, kind='question'):
        eitem = {}

        # Fields common in questions and answers
        common_fields = ["product", "topic", "locale", "is_spam", "title"]

        if kind == 'question':
            eitem['type'] = kind
            for f in self.RAW_FIELDS_COPY + ["offset"]:
                if f in item:
                    eitem[f] = item[f]
                else:
                    eitem[f] = None
            # The real data
            question = item['data']

            # data fields to copy
            copy_fields = ["content", "num_answers", "solution"]
            copy_fields += common_fields
            for f in copy_fields:
                if f in question:
                    eitem[f] = question[f]
                else:
                    eitem[f] = None
            eitem["content_analyzed"] = question['content']

            # Fields which names are translated
            map_fields = {"id": "question_id",
                          "num_votes": "score"
                          }
            for fn in map_fields:
                eitem[map_fields[fn]] = question[fn]

            tags = ''
            for tag in question['tags']:
                tags += tag['slug'] + ","
            tags = tags[0:-1]  # remove last ,
            eitem["tags"] = tags
            eitem["tags_analyzed"] = tags

            # Enrich dates
            eitem["creation_date"] = str_to_datetime(question["created"]).isoformat()
            eitem["last_activity_date"] = str_to_datetime(question["updated"]).isoformat()

            eitem['lifetime_days'] = \
                get_time_diff_days(question['created'], question['updated'])

            eitem.update(self.get_grimoire_fields(question['created'], "question"))

            eitem['author'] = question['creator']['username']
            if question['creator']['display_name']:
                eitem['author'] = question['creator']['display_name']

            if self.sortinghat:
                eitem.update(self.get_item_sh(item))

            if self.prjs_map:
                eitem.update(self.get_item_project(eitem))

            self.add_repository_labels(eitem)
            self.add_metadata_filter_raw(eitem)

        elif kind == 'answer':
            answer = item
            eitem['type'] = kind

            # data fields to copy
            copy_fields = ["content", "solution"]
            copy_fields += common_fields
            for f in copy_fields:
                if f in answer:
                    eitem[f] = answer[f]
                else:
                    eitem[f] = None
            eitem["content_analyzed"] = answer['content']

            # Fields which names are translated
            map_fields = {
                "id": "answer_id",
                "question": "question_id",
                "num_helpful_votes": "score",
                "num_unhelpful_votes": "unhelpful_answer"
            }
            for fn in map_fields:
                eitem[map_fields[fn]] = answer[fn]

            eitem["helpful_answer"] = answer['num_helpful_votes']

            # Enrich dates
            eitem["creation_date"] = str_to_datetime(answer["created"]).isoformat()
            eitem["last_activity_date"] = str_to_datetime(answer["updated"]).isoformat()

            eitem['lifetime_days'] = \
                get_time_diff_days(answer['created'], answer['updated'])

            eitem.update(self.get_grimoire_fields(answer['created'], "answer"))

            eitem['author'] = answer['creator']['username']
            if answer['creator']['display_name']:
                eitem['author'] = answer['creator']['display_name']

            if self.sortinghat:
                # date field must be the same than in question to share code
                answer[self.get_field_date()] = answer['updated']
                eitem[self.get_field_date()] = answer[self.get_field_date()]
                eitem.update(self.get_item_sh(answer))

            if self.prjs_map:
                eitem.update(self.get_item_project(eitem))

        return eitem

    def enrich_items(self, ocean_backend):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""
        total = 0

        url = self.elastic.get_bulk_url()

        logger.debug("[kitsune] Adding items to {} (in {} packs)".format(anonymize_url(url), max_items))

        items = ocean_backend.fetch()
        for item in items:
            if current >= max_items:
                total += self.elastic.safe_put_bulk(url, bulk_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (item[self.get_field_unique_id()])
            bulk_json += data_json + "\n"  # Bulk document
            current += 1
            # Time to enrich also de answers
            if 'answers_data' in item['data']:
                for answer in item['data']['answers_data']:
                    # Add question title in answers
                    answer['title'] = item['data']['title']
                    answer['solution'] = 0
                    if answer['id'] == item['data']['solution']:
                        answer['solution'] = 1
                    rich_answer = self.get_rich_item(answer, kind='answer')
                    data_json = json.dumps(rich_answer)
                    bulk_json += '{"index" : {"_id" : "%s_%i" } }\n' % \
                        (item[self.get_field_unique_id()],
                         rich_answer['answer_id'])
                    bulk_json += data_json + "\n"  # Bulk document
                    current += 1

        if current > 0:
            total += self.elastic.safe_put_bulk(url, bulk_json)

        return total
