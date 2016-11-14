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
            user = item['data']['details']['participants'][identity_field]
        identity['username'] = user['username']
        identity['email'] = None
        identity['name'] = user['username']
        return identity

    def get_field_author(self):
        return 'created_by'

    @metadata
    def get_rich_item(self, item):
        eitem = {}
        eitem["metadata__updated_on"] = item["metadata__updated_on"]
        eitem["ocean-unique-id"] = item["ocean-unique-id"]
        topic = item['data']

        # Fields that are the same in item and eitem
        copy_fields = ["created_at", "like_count", "reply_count", "word_count", "posts_count",
                       "id", "participant_count", "views", "pinned",
                       "created_at", "last_posted_at", "title"]
        for f in copy_fields:
            if f in topic:
                eitem[f] = topic[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"slug": "topic_slug"}
        for fn in map_fields:
            if fn in topic:
                eitem[map_fields[fn]] = topic[fn]
            else:
                eitem[map_fields[fn]] = None

        eitem['username'] = topic['details']['created_by']['username']

        # First post has the topic contents
        eitem['description'] = topic['post_stream']['posts'][0]['cooked']

        # Topic URL
        eitem['topic_url'] = item["origin"] + '/t/' + str(topic['id'])

        # First reply time
        eitem['first_reply_time'] = None
        firt_post_time = None
        if len(topic['post_stream']['posts'])>1:
            firt_post_time = topic['post_stream']['posts'][0]['created_at']
            second_post_time = topic['post_stream']['posts'][1]['created_at']
            eitem['first_reply_time'] = get_time_diff_days(firt_post_time, second_post_time)

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        eitem.update(self.get_grimoire_fields(firt_post_time, "topic"))

        return eitem
