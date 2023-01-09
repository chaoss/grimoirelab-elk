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
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import logging

from grimoirelab_toolkit.datetime import unixtime_to_datetime

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping


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
                }
           }
        } """

        return {"items": mapping}


class StackExchangeEnrich(Enrich):

    mapping = Mapping

    def get_field_unique_id(self):
        return "item_id"

    def get_field_author(self):
        return "owner"

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]
        elif identity_field in item:
            # for answers
            user = item[identity_field]

        if 'display_name' not in user:
            user['display_name'] = ''

        identity['username'] = user['display_name']
        identity['email'] = None
        identity['name'] = user['display_name']

        return identity

    def get_identities(self, item):
        """ Return the identities from an item """

        item = item['data']

        for identity in ['owner']:
            if identity in item and item[identity]:
                user = self.get_sh_identity(item[identity])
                yield user
            if 'answers' in item:
                for answer in item['answers']:
                    user = self.get_sh_identity(answer[identity])
                    yield user

    @metadata
    def get_rich_item(self, item, kind='question', question_tags=None):
        eitem = {}

        # Fields common in questions and answers
        common_fields = ["title", "comment_count", "question_id",
                         "delete_vote_count", "up_vote_count",
                         "down_vote_count", "favorite_count", "view_count",
                         "last_activity_date", "link", "score", "tags"]

        if kind == 'question':
            self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)
            # The real data
            question = item['data']

            eitem["item_id"] = question['question_id']
            eitem["type"] = 'question'
            eitem["author"] = None
            if 'owner' in question and question['owner']['user_type'] == "does_not_exist":
                logger.warning("[stackexchange] question without owner: {}".format(question['question_id']))
            else:
                eitem["author"] = question['owner']['display_name']
                eitem["author_link"] = None
                if 'link' in question['owner']:
                    eitem["author_link"] = question['owner']['link']
                eitem["reputation"] = None
                if 'reputation' in question['owner']:
                    eitem["author_reputation"] = question['owner']['reputation']

            # data fields to copy
            copy_fields = common_fields + ['answer_count']
            for f in copy_fields:
                if f in question:
                    eitem[f] = question[f]
                else:
                    eitem[f] = None

            eitem["question_tags"] = question['tags']
            # eitem["question_tags_custom_analyzed"] = question['tags']

            # Fields which names are translated
            map_fields = {"title": "question_title"}
            for fn in map_fields:
                eitem[map_fields[fn]] = question[fn]
            eitem['title_analyzed'] = question['title']

            eitem['question_has_accepted_answer'] = 0
            eitem['question_accepted_answer_id'] = None

            if question['answer_count'] >= 1 and 'answers' not in question:
                logger.warning("[stackexchange] Missing answers for question {}".format(question['question_id']))
            elif question['answer_count'] >= 1 and 'answers' in question:
                answers_id = [p['answer_id'] for p in question['answers']
                              if 'is_accepted' in p and p['is_accepted']]
                eitem['question_accepted_answer_id'] = answers_id[0] if answers_id else None
                eitem['question_has_accepted_answer'] = 1 if eitem['question_accepted_answer_id'] else 0

            creation_date = unixtime_to_datetime(question["creation_date"]).isoformat()
            eitem['creation_date'] = creation_date
            eitem.update(self.get_grimoire_fields(creation_date, "question"))

            if self.sortinghat:
                eitem.update(self.get_item_sh(item))

            if self.prjs_map:
                eitem.update(self.get_item_project(eitem))

            self.add_repository_labels(eitem)
            self.add_metadata_filter_raw(eitem)

        elif kind == 'answer':
            answer = item

            eitem["type"] = 'answer'
            eitem["item_id"] = answer['answer_id']
            eitem["author"] = None
            if 'owner' in answer and answer['owner']['user_type'] == "does_not_exist":
                logger.warning("[stackexchange] answer without owner: {}".format(answer['question_id']))
            else:
                eitem["author"] = answer['owner']['display_name']
                eitem["author_link"] = None
                if 'link' in answer['owner']:
                    eitem["author_link"] = answer['owner']['link']
                eitem["reputation"] = None
                if 'reputation' in answer['owner']:
                    eitem["author_reputation"] = answer['owner']['reputation']

            # data fields to copy
            copy_fields = common_fields + ["origin", "tag", "creation_date", "is_accepted", "answer_id"]
            for f in copy_fields:
                if f in answer:
                    eitem[f] = answer[f]
                else:
                    eitem[f] = None

            eitem['is_accepted_answer'] = 1 if answer['is_accepted'] else 0
            eitem['answer_status'] = "accepted" if answer['is_accepted'] else "not_accepted"

            eitem["question_tags"] = question_tags
            if 'tags' in answer:
                eitem["answer_tags"] = answer['tags']

            # Fields which names are translated
            map_fields = {"title": "question_title"
                          }
            for fn in map_fields:
                eitem[map_fields[fn]] = answer[fn]

            creation_date = unixtime_to_datetime(answer["creation_date"]).isoformat()
            eitem['creation_date'] = creation_date
            eitem.update(self.get_grimoire_fields(creation_date, "answer"))

            if self.sortinghat:
                # date field must be the same than in question to share code
                answer[self.get_field_date()] = eitem['creation_date']
                eitem[self.get_field_date()] = eitem['creation_date']
                eitem.update(self.get_item_sh(answer))

            if self.prjs_map:
                eitem.update(self.get_item_project(eitem))

        return eitem

    def enrich_items(self, ocean_backend):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        items = ocean_backend.fetch()
        for item in items:

            answers_tags = []

            if 'answers' in item['data']:
                for answer in item['data']['answers']:
                    # Copy mandatory raw fields
                    answer['origin'] = item['origin']
                    answer['tag'] = item['tag']

                    rich_answer = self.get_rich_item(answer,
                                                     kind='answer',
                                                     question_tags=item['data']['tags'])
                    if 'answer_tags' in rich_answer:
                        answers_tags.extend(rich_answer['answer_tags'])
                    items_to_enrich.append(rich_answer)

            rich_question = self.get_rich_item(item)
            rich_question['answers_tags'] = list(set(answers_tags))
            rich_question['thread_tags'] = rich_question['answers_tags'] + rich_question['question_tags']
            items_to_enrich.append(rich_question)

            if len(items_to_enrich) < self.elastic.max_items_bulk:
                continue

            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())
            items_to_enrich = []

        if len(items_to_enrich) > 0:
            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())

        if num_items != ins_items:
            missing = num_items - ins_items
            logger.error("[stackexchange] {}/{} missing items".format(missing, num_items))
        else:
            logger.info("[stackexchange] {} items inserted".format(num_items))

        return num_items
