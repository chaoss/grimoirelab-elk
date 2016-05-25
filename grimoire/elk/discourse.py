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
import requests

from dateutil import parser

from grimoire.elk.enrich import Enrich

from sortinghat import api

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
        post = item['data']
        # Fields that are the same in item and eitem
        copy_fields = ["topic_slug","display_username","_category_id","avg_time",
                       "score","reads","id","topic_id","cooked"]
        for f in copy_fields:
            if f in post:
                eitem[f] = post[f]
            else:
                eitem[f] = None
        return eitem

    def enrich_items(self, items):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        url = self.elastic.index_url+'/items/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))

        for item in items:
            if current >= max_items:
                requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (rich_item[self.get_field_unique_id()])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        requests.put(url, data = bulk_json)
