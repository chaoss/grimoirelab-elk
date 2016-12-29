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

from .utils import get_time_diff_days

from .enrich import Enrich, metadata

class DiscourseEnrich(Enrich):

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        # All identities are in the post stream
        # The first post is the question. Next replies

        posts = item['data']['post_stream']['posts']

        for post in posts:
            user = self.get_sh_identity(post)
            identities.append(user)
        return identities

    def get_sh_identity(self, post, identity_field=None):
        identity = {}

        if identity_field in post:
            post = post[identity_field]

        identity['username'] = post['username']
        identity['email'] = None
        identity['name'] = post['display_username']
        return identity

    def get_field_author(self):
        return 'author'

    def get_users_data(self, post):
        """ Adapt the data to be used with standard SH enrich API """
        poster = {}
        poster[self.get_field_author()] = post
        return poster

    def get_rich_item_answers(self, item):
        answers_enrich = []
        nanswers = 0

        for answer in item['data']['post_stream']['posts']:
            eanswer = self.get_rich_item(item)  # reuse all fields from item
            eanswer['id'] = answer['id']
            eanswer['url'] = eanswer['origin'] + "/t/" + answer['topic_slug']
            eanswer['url'] += "/" + str(answer['topic_id']) + "/" + str(answer['post_number'])
            eanswer['type'] = 'answer'
            eanswer.update(self.get_grimoire_fields(answer['created_at'], eanswer['type']))
            eanswer.pop('is_discourse_question')
            eanswer['display_username'] = answer['display_username']
            eanswer['username'] = answer['username']
            eanswer['author_id'] = answer['user_id']
            eanswer['author_trust_level'] = answer['trust_level']
            eanswer['author_url'] = eanswer['origin'] + "/users/" + str(eanswer['author_id'])
            eanswer['reads'] = answer['reads']
            eanswer['score'] = answer['score']
            eanswer['reply_count'] = answer['reply_count']
            eanswer['time_from_question'] = None
            post_time = answer['created_at']
            item_time = item['data']['created_at']
            eanswer['first_reply_time'] = get_time_diff_days(item_time, post_time)
            answers_enrich.append(eanswer)

            if self.sortinghat:
                eanswer.update(self.get_item_sh(answer, date_field="created_at"))

            nanswers += 1
            eanswer['first_answer'] = 0
            if nanswers == 1:
                eanswer['first_answer'] = 1

        return answers_enrich

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

        topic = item['data']

        # Fields that are the same in item and eitem
        copy_fields = ["id"]
        for f in copy_fields:
            if f in topic:
                eitem[f] = topic[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"like_count": "question_like_count",
                      "posts_count": "question_posts_count",
                      "participant_count": "question_participants",
                      "pinned_at": "question_pinned_at",
                      "pinned_globally": "question_pinned_globally",
                      "pinned_until": "question_pinned_until",
                      "pinned": "question_pinned",
                      "fancy_title": "question_title",
                      "views": "question_views",
                      "reply_count": "question_replies"
                      }
        for fn in map_fields:
            if fn in topic:
                eitem[map_fields[fn]] = topic[fn]
            else:
                eitem[map_fields[fn]] = None

        # The first post is the first published, and it is the question
        first_post = topic['post_stream']['posts'][0]
        eitem['url'] = eitem['origin'] + "/t/" + first_post['topic_slug']
        eitem['url'] += "/" + str(first_post['topic_id']) + "/" + str(first_post['post_number'])
        eitem['display_username'] = first_post['display_username']
        eitem['username'] = first_post['username']
        eitem['author_id'] = first_post['user_id']
        eitem['author_trust_level'] = first_post['trust_level']
        eitem['author_url'] = eitem['origin'] + "/users/" + str(eitem['author_id'])
        eitem['reads'] = first_post['reads']
        eitem['score'] = first_post['score']
        eitem['reply_count'] = first_post['reply_count']

        # First reply time
        eitem['time_from_question'] = None
        firt_post_time = None
        if len(topic['post_stream']['posts'])>1:
            firt_post_time = first_post['created_at']
            second_post_time = topic['post_stream']['posts'][1]['created_at']
            eitem['first_reply_time'] = get_time_diff_days(firt_post_time, second_post_time)

        if self.sortinghat:
            eitem.update(self.get_item_sh(first_post, date_field="created_at"))

        eitem['type'] = 'question'
        eitem.update(self.get_grimoire_fields(topic["created_at"], eitem['type']))

        return eitem

    def get_field_unique_id_answer(self):
        return "id"

    def enrich_items(self, items):
        nitems = super(DiscourseEnrich, self).enrich_items(items)
        logging.info("Total questions enriched: %i", nitems)

        # And now for each item we want also the answers (tops)
        nanswers = 0
        rich_item_answers = []

        for item in items:
            rich_item_answers += self.get_rich_item_answers(item)

        if rich_item_answers:
            nanswers += self.elastic.bulk_upload(rich_item_answers,
                                                 self.get_field_unique_id_answer())
        logging.info("Total answers enriched: %i", nanswers)
