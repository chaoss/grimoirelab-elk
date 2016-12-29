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

from .enrich import Enrich, metadata

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
                # avoid "answered_by" : "This post is a wiki" corner case
                if type(answer['answered_by']) is dict:
                    user = self.get_sh_identity(answer['answered_by'])
                    identities.append(user)
                if 'comments' in answer:
                    for comment in answer['comments']:
                        identities.append(self.get_sh_identity(comment))
        return identities

    def get_sh_identity(self, item, identity_field=None):
        identity = {key: None for key in ['username', 'name', 'email']}

        user = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
            user = item['data'][identity_field]
        elif 'author' in item:
            user = item['author']

        if user is None:
            return identity

        if 'username' in user:
            identity['username'] = user['username']
        elif 'user_display_name' in user:
            identity['username'] = user['user_display_name']
        identity['name'] = identity['username']
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
        eitem['time_to_reply'] = None
        if 'answers' in question:
            # answers ordered by time
            first_answer_time = unixtime_to_datetime(float(question['answers'][0]["added_at"]))
            eitem['time_to_reply'] = get_time_diff_days(added_at, first_answer_time)

        if question['author'] and type(question['author']) is dict:
            eitem['author_user_name'] = question['author']['username']
            eitem['author_id'] = question['author']['id']
            eitem['author_badges'] = question['author']['badges']
            eitem['author_reputation'] = int(question['author']['reputation'])

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

    def get_field_unique_id_comment(self):
        return "id"

    def get_field_unique_id_answer(self):
        return "id"

    def get_rich_comment(self, item, answer, comment):
        ecomment = self.get_rich_item(item)  # reuse all fields from item
        ecomment['id'] = comment['id']
        ecomment['url'] = item['data']['url']+"/?answer="
        ecomment['url'] += answer['id']+'#post-id-'+answer['id']
        if 'author' in comment:
            ecomment['author_user_name'] = comment['author']['username']
            ecomment['author_id'] = comment['author']['id']
        if 'summary' in comment:
            ecomment['summary'] = comment['summary']
        ecomment['score'] = comment['score']

        dfield = 'added_at'
        if 'comment_added_at' in comment:
            dfield = 'comment_added_at'

        if self.sortinghat:
            if dfield == 'added_at':
                comment['added_at_date'] = unixtime_to_datetime(float(comment[dfield])).isoformat()
            else:
                comment['added_at_date'] = comment[dfield]
            ecomment.update(self.get_item_sh(comment, date_field="added_at_date"))

        if dfield == 'added_at':
            comment_at = unixtime_to_datetime(float(comment[dfield]))
        else:
            comment_at = parser.parse(comment[dfield])

        added_at = unixtime_to_datetime(float(item['data']["added_at"]))
        ecomment['time_from_question'] = get_time_diff_days(added_at, comment_at)
        ecomment['type'] = 'comment'
        ecomment.update(self.get_grimoire_fields(comment_at.isoformat(), ecomment['type']))

        # Clean items fields not valid in comments
        for f in ['is_askbot_question', 'author_reputation', 'author_badges', 'is_correct', 'comment_count']:
            if f in ecomment:
                ecomment.pop(f)

        return ecomment

    def get_rich_answer(self, item, answer):
        eanswer = self.get_rich_item(item)  # reuse all fields from item
        eanswer['id'] = answer['id']
        eanswer['url'] = item['data']['url']+"/?answer="
        eanswer['url'] += answer['id']+'#post-id-'+answer['id']
        if type(answer['answered_by']) is dict:
            eanswer['author_user_name'] = answer['answered_by']['username']
            eanswer['author_id'] = answer['answered_by']['id']
            eanswer['author_badges'] = answer['answered_by']['badges']
            eanswer['author_reputation'] = int(answer['answered_by']['reputation'])
        eanswer['summary'] = answer['summary']
        eanswer['score'] = answer['score']
        if 'is_correct' in answer:
            eanswer['is_correct'] = 1

        if self.sortinghat:
            answer['added_at_date'] = unixtime_to_datetime(float(answer["added_at"])).isoformat()
            eanswer.update(self.get_item_sh(answer, date_field="added_at_date"))

        answer_at = unixtime_to_datetime(float(answer["added_at"]))
        added_at = unixtime_to_datetime(float(item['data']["added_at"]))
        eanswer['time_from_question'] = get_time_diff_days(added_at, answer_at)
        eanswer['type'] = 'answer'
        eanswer.update(self.get_grimoire_fields(answer_at.isoformat(), eanswer['type']))

        # Clean items fields not valid in comments
        eanswer.pop('is_askbot_question')

        return eanswer

    def get_rich_item_answers_comments(self, item):
        answers_enrich = []
        comments_enrich = []

        if 'answers' not in item['data']:
            return (answers_enrich, comments_enrich)

        for answer in item['data']['answers']:
            eanswer = self.get_rich_answer(item, answer)
            if not answers_enrich:
                eanswer['first_answer'] = 1
            answers_enrich.append(eanswer)

            # And now time to process the comments
            if 'comments' in answer:
                for comment in answer['comments']:
                    ecomment = self.get_rich_comment(item, answer, comment)
                    comments_enrich.append(ecomment)

        return (answers_enrich, comments_enrich)

    def enrich_items(self, items):
        nitems = super(AskbotEnrich, self).enrich_items(items)
        logging.info("Total questions enriched: %i", nitems)

        # And now for each item we want also the answers (tops)
        nanswers = 0
        ncomments = 0
        rich_item_answers = []
        rich_item_comments = []

        for item in items:
            (answers, comments) = self.get_rich_item_answers_comments(item)
            rich_item_answers += answers
            rich_item_comments += comments

        if rich_item_answers:
            nanswers = self.elastic.bulk_upload(rich_item_answers,
                                                self.get_field_unique_id_answer())
        if rich_item_comments:
            ncomments += self.elastic.bulk_upload(rich_item_comments,
                                                  self.get_field_unique_id_comment())
        logging.info("Total answers enriched: %i", nanswers)
        logging.info("Total comments enriched: %i", ncomments)
