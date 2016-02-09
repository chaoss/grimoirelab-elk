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

class StackExchangeEnrich(Enrich):

    def __init__(self, stackexchange):
        super().__init__()
        self.elastic = None
        self.perceval_backend = stackexchange
        self.index_stackexchange = "stackexchange"

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_date(self):
        return "__metadata__updated_on"

    def get_field_unique_id(self):
        return "question_id"

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
               "owner": {
                "display_name": {
                  "type": "string",
                  "index":"not_analyzed"
                  }
               }
               }
        } """

        return {"items":mapping}


    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        return identities

    def get_sh_identity(self, stackexchange_user):
        identity = {}
        return identity

    def get_rich_item(self, item):
        eitem = {}
        # Fields that are the same in item and eitem
        copy_fields = ["title","question_id","link","view_count",
                       "answer_count","comment_count"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {}
        for fn in map_fields:
            eitem[map_fields[fn]] = commit[fn]
        # Enrich dates
        eitem["question_date"] = parser.parse(item["__metadata__updated_on"]).isoformat()
        # people
        eitem["question_owner"] = item["owner"]["display_name"]
        # eitem["owner_link"] = item["owner"]["link"]
        eitem["tags"] = ",".join(item["tags"])
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
