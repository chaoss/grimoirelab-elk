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

from grimoire.elk.enrich import Enrich, metadata

class DiscourseEnrich(Enrich):

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        participants = item['data']['details']['participants']

        for identity in participants:
            user = self.get_sh_identity(identity)
            identities.append(user)
        return identities

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
            user = item['data']['details'][identity_field]
        identity['username'] = user['username']
        identity['email'] = None
        identity['name'] = user['username']
        return identity

    def get_field_author(self):
        return 'created_by'

    def get_users_data(self, item):
        """ If user fields are inside the global item dict """
        if 'data' in item:
            users_data = item['data']['details']
        else:
            # the item is directly the data (kitsune answer)
            users_data = item

        return users_data

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
        map_fields = {"question_like_count": "like_count",
                      "question_posts_count": "posts_count",
                      "question_participants": "participant_count",
                      "question_pinned_at": "pinned_at",
                      "question_pinned_globally": "pinned_globally",
                      "question_pinned_until": "pinned_until",
                      "question_pinned": "pinned",
                      "question_title": "fancy_title",
                      "question_views": "views",
                      "question_replies": "reply_count"
                      }
        for fn in map_fields:
            if fn in topic:
                eitem[map_fields[fn]] = topic[fn]
            else:
                eitem[map_fields[fn]] = None

        if 'question_replies' in eitem:
            eitem['question_replies'] -= 1  # index enrich spec

        # The first post is the first published
        posts = topic['post_stream']['posts'][0]
        eitem['url'] = eitem['origin'] + "/t/" + posts['topic_slug']
        eitem['url'] += "/" + str(posts['topic_id']) + "/" + str(posts['post_number'])
        eitem['author_user_name'] = posts['display_username']
        eitem['author_id'] = posts['user_id']
        eitem['reads'] = posts['reads']
        eitem['reply_count'] = posts['reply_count']

        # First reply time
        eitem['time_from_question'] = None
        firt_post_time = None
        if len(topic['post_stream']['posts'])>1:
            firt_post_time = topic['post_stream']['posts'][0]['created_at']
            second_post_time = topic['post_stream']['posts'][1]['created_at']
            eitem['first_reply_time'] = get_time_diff_days(firt_post_time, second_post_time)

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        eitem['type'] = 'question'
        eitem.update(self.get_grimoire_fields(topic["created_at"], eitem['type']))

        return eitem
