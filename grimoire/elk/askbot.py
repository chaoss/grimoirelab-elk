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

from dateutil import parser

from .utils import get_time_diff_days, unixtime_to_datetime

from grimoire.elk.enrich import Enrich, metadata

class AskbotEnrich(Enrich):

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        # question
        user = self.get_sh_identity(item, self.get_field_author())
        identities.append(user)

        # answers
        if 'answers' in item['data']:
            for answer in item['data']['answers']:
                user = self.get_sh_identity(answer['answered_by'])
                identities.append(user)
        return identities

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
            user = item['data'][identity_field]
        identity['username'] = user['username']
        identity['email'] = None
        identity['name'] = user['username']
        return identity

    def get_field_author(self):
        return 'author'

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "author_badges": {
                  "type": "string",
                  "index":"analyzed"
                },
                "summary": {
                  "type": "string",
                  "index":"analyzed"
                },
                "question_tags": {
                    "type": "string",
                    "index":"analyzed",
                    "analyzer" : "comma"
                },
                "question_answer_ids": {
                    "type": "string",
                    "index":"analyzed",
                    "analyzer" : "comma"
                }
           }
        } """
        return {"items":mapping}

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        copy_fields = ["metadata__updated_on", "metadata__timestamp",
                       "ocean-unique-id", "origin"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None

        question = item['data']

        # Fields that are the same in item and eitem
        copy_fields = ["id", "url", "title", "summary", "score"]
        for f in copy_fields:
            if f in question:
                eitem[f] = question[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"title": "question_title",
                      "answer_count": "question_answer_count",
                      "view_count": "question_view_count",
                      "answer_ids": "question_answer_ids"}
        for fn in map_fields:
            if fn in question:
                eitem[map_fields[fn]] = question[fn]
            else:
                eitem[map_fields[fn]] = None

        # First answer time
        added_at = unixtime_to_datetime(float(question["added_at"]))
        eitem['time_from_question'] = None
        if 'answers' in question:
            # answers ordered by time
            first_answer_time = unixtime_to_datetime(float(question['answers'][0]["added_at"]))
            eitem['time_from_question'] = get_time_diff_days(added_at, first_answer_time)

        eitem['author_user_name'] = question['author']['username']
        eitem['author_id'] = question['author']['id']
        eitem['author_badges'] = question['author']['badges']
        eitem['author_reputation'] = question['author']['reputation']

        eitem['question_last_activity_at'] = unixtime_to_datetime(float(question['last_activity_at'])).isoformat()
        eitem['question_last_activity_by_id'] = question['last_activity_by']['id']
        eitem['question_last_activity_by_username'] = question['last_activity_by']['username']
        # Analyzed
        eitem['question_tags'] = ",".join([tag for tag in question['tags']])
        eitem['question_answer_ids'] = ",".join([str(aid) for aid in question['answer_ids']])

        eitem['comment_count'] = 0
        if 'answers' in question:
            eitem['comment_count'] = sum([len (a['comments']) if 'comments' in a else 0 for a in question['answers']])

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))
        eitem["type"] = "question"
        eitem.update(self.get_grimoire_fields(added_at.isoformat(), eitem["type"]))

        return eitem
