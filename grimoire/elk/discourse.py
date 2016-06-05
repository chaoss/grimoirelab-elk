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

from .utils import get_time_diff_days


from grimoire.elk.enrich import Enrich

class DiscourseEnrich(Enrich):

    def __init__(self, discourse, sortinghat=True, db_projects_map = None):
        super().__init__(sortinghat, db_projects_map)
        self.elastic = None
        self.perceval_backend = discourse
        self.index_Discourse = "discourse"

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_date(self):
        return "metadata__updated_on"

    def get_field_unique_id(self):
        return "ocean-unique-id"

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        return identities

    def get_sh_identity(self, discourse_user):
        identity = {}
        return identity

    def get_rich_item(self, item):
        eitem = {}
        eitem["metadata__updated_on"] = item["metadata__updated_on"]
        eitem["ocean-unique-id"] = item["ocean-unique-id"]
        topic = item['data']

        # Fields that are the same in item and eitem
        copy_fields = ["like_count", "reply_count", "word_count", "posts_count",
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

        # First reply time
        eitem['first_reply_time'] = None
        if len(topic['post_stream']['posts'])>1:
            firt_post_time = topic['post_stream']['posts'][0]['created_at']
            second_post_time = topic['post_stream']['posts'][1]['created_at']
            eitem['first_reply_time'] = get_time_diff_days(firt_post_time, second_post_time)
        return eitem

    def enrich_items(self, items):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        url = self.elastic.index_url+'/items/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))

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
        self.requests.put(url, data = bulk_json)
