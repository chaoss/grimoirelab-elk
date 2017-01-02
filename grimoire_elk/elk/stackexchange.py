#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
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
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import json
import logging

from datetime import datetime

from .enrich import Enrich, metadata

from .utils import unixtime_to_datetime

class StackExchangeEnrich(Enrich):

    def get_field_unique_id(self):
        return "question_id"

    def get_field_author(self):
        return "owner"

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "title_analyzed": {
                  "type": "string",
                  "index":"analyzed"
                },
                "question_tags_analyzed": {
                    "type": "string",
                    "index":"analyzed"
                },
                "answer_tags_analyzed": {
                    "type": "string",
                    "index":"analyzed"
                },
                "question_tags_custom_analyzed" : {
                  "type" : "string",
                  "analyzer" : "comma"
                }
           }
        } """

        # For ES 5
        # "question_tags_custom_analyzed_5" : {
        #   "type" : "string",
        #   "analyzer" : "comma",
        #   "fielddata" : "true" }


        return {"items":mapping}

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item
        if 'data' in item and type(item) == dict:
            user = item['data'][identity_field]
        elif identity_field in item:
            # for answers
            user = item[identity_field]

        identity['username'] = user['display_name']
        identity['email'] = None
        identity['name'] = user['display_name']

        return identity

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        item = item['data']

        for identity in ['owner']:
            if identity in item and item[identity]:
                user = self.get_sh_identity(item[identity])
                identities.append(user)
            if 'answers' in item:
                for answer in item['answers']:
                    user = self.get_sh_identity(answer[identity])
                    identities.append(user)
        return identities

    @metadata
    def get_rich_item(self, item, kind='question', question_tags = None):
        eitem = {}

        # Fields common in questions and answers
        common_fields = ["title", "comment_count", "question_id",
                         "delete_vote_count", "up_vote_count",
                         "down_vote_count","favorite_count", "view_count",
                         "last_activity_date", "link", "score", "tags"]

        if kind == 'question':
            # metadata fields to copy, only in question (perceval item)
            copy_fields = ["metadata__updated_on","metadata__timestamp","ocean-unique-id","origin"]
            for f in copy_fields:
                if f in item:
                    eitem[f] = item[f]
                else:
                    eitem[f] = None
            # The real data
            question = item['data']

            eitem["type"] = 'question'
            eitem["author"] = question['owner']['display_name']
            eitem["author_link"] = None
            if 'link' in question['owner']:
                eitem["author_link"] = question['owner']['link']
            if 'reputation' in question['owner']:
                eitem["author_reputation"] = question['owner']['reputation']

            # data fields to copy
            copy_fields = common_fields + ['answer_count']
            for f in copy_fields:
                if f in question:
                    eitem[f] = question[f]
                else:
                    eitem[f] = None

            eitem["question_tags"] = ",".join(question['tags'])
            eitem["question_tags_analyzed"] = ",".join(question['tags'])
            eitem["question_tags_custom_analyzed"] = ",".join(question['tags'])

            # Fields which names are translated
            map_fields = {"title": "question_title"
                          }
            for fn in map_fields:
                eitem[map_fields[fn]] = question[fn]

            creation_date = unixtime_to_datetime(question["creation_date"]).isoformat()
            eitem['creation_date'] = creation_date
            eitem.update(self.get_grimoire_fields(creation_date, "question"))

            if self.sortinghat:
                eitem.update(self.get_item_sh(item))

        elif kind == 'answer':
            answer = item

            eitem["type"] = 'answer'
            eitem["author"] = answer['owner']['display_name']
            eitem["author_link"] = None
            if 'link' in answer['owner']:
                eitem["author_link"] = answer['owner']['link']
            if 'reputation' in answer['owner']:
                eitem["author_reputation"] = answer['owner']['reputation']

            # data fields to copy
            copy_fields = common_fields + ["creation_date", "is_accepted", "answer_id"]
            for f in copy_fields:
                if f in answer:
                    eitem[f] = answer[f]
                else:
                    eitem[f] = None

            eitem["question_tags"] = question_tags
            eitem["question_tags_analyzed"] = question_tags
            if 'tags' in answer:
                eitem["answer_tags"] = ",".join(answer['tags'])
                eitem["answer_tags_analyzed"] = ",".join(answer['tags'])

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

        return eitem

    def enrich_items(self, items):
        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        url = self.elastic.index_url+'/items/_bulk'

        logging.debug("Adding items to %s (in %i packs)", url, max_items)

        for item in items:
            if current >= max_items:
                self.requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (rich_item[self.get_field_unique_id()])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
            total += 1
            # Time to enrich also de answers
            if 'answers' in item['data']:
                for answer in item['data']['answers']:
                    rich_answer = self.get_rich_item(answer, kind='answer', question_tags=rich_item['question_tags'])
                    data_json = json.dumps(rich_answer)
                    bulk_json += '{"index" : {"_id" : "%i_%i" } }\n' % \
                        (rich_answer[self.get_field_unique_id()],
                         rich_answer['answer_id'])
                    bulk_json += data_json +"\n"  # Bulk document
                    current += 1
                    total += 1

        self.requests.put(url, data = bulk_json)

        return total
