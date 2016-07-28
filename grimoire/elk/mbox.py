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
import email.utils

from grimoire.elk.enrich import Enrich

from sortinghat import api

class MBoxEnrich(Enrich):

    def __init__(self, mbox, db_sortinghat=None, db_projects_map = None):
        super().__init__(db_sortinghat, db_projects_map)
        self.elastic = None
        self.perceval_backend = mbox
        self.index_mbox = "mbox"

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_date(self):
        return "metadata__updated_on"

    def get_field_unique_id(self):
        return "ocean-unique-id"

    def get_fields_uuid(self):
        return ["from_uuid"]

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                 "Subject_analyzed": {
                   "type": "string",
                   "index":"analyzed"
                 }
           }
        } """

        return {"items":mapping}

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        item = item['data']
        for identity in ['From']:
            if identity in item and item[identity]:
                user = self.get_sh_identity(item[identity])
                identities.append(user)
        return identities

    def get_sh_identity(self, from_data):
        # "From": "hwalsh at wikiledia.net (Heat Walsh)"

        identity = {}

        # First desofuscate the email
        EMAIL_OBFUSCATION_PATTERNS = [' at ', '_at_', ' en ']
        for pattern in EMAIL_OBFUSCATION_PATTERNS:
            if from_data.find(pattern) != -1:
                from_data = from_data.replace(pattern, '@')

        from_ = email.utils.parseaddr(from_data)

        identity['username'] = None  # email does not have username
        identity['email'] = from_[1]
        identity['name'] = from_[0]
        if not identity['name']:
            identity['name'] = identity['email'].split('@')[0]
        return identity


    def get_item_project(self, item):
        """ Get project mapping enrichment field """
        # "origin": "dltk-commits"
        # /mnt/mailman_archives/dltk-dev.mbox/dltk-dev.mbox
        ds_name = "mls"  # data source name in projects map
        mls_list = item['origin']
        # Eclipse specific yet
        path = "/mnt/mailman_archives/"
        path += mls_list+".mbox/"+mls_list+".mbox"

        try:
            project = (self.prjs_map[ds_name][path])
        except KeyError:
            # logging.warning("Project not found for list %s" % (mls_list))
            project = None
        return {"project": project}

    def get_rich_item(self, item):
        eitem = {}

        # metadata fields to copy
        copy_fields = ["metadata__updated_on","metadata__timestamp","ocean-unique-id","origin"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        message = item['data']

        # Fields that are the same in message and eitem
        copy_fields = ["Date","From","Subject","Message-ID"]
        for f in copy_fields:
            if f in message:
                eitem[f] = message[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"Subject": "Subject_analyzed"
                      }
        for fn in map_fields:
            if fn in message:
                eitem[map_fields[fn]] = message[fn]
            else:
                eitem[map_fields[fn]] = None

        # Enrich dates
        eitem["email_date"] = parser.parse(item["metadata__updated_on"]).isoformat()
        eitem["list"] = item["origin"]

        # Root message
        if 'In-Reply-To' in message:
            eitem["root"] = False
        else:
            eitem["root"] = True

        # Size of the message
        try:
            eitem["size"] = len(message['body']['plain'])
        except:
            eitem["size"] = None

        # Time zone
        try:
            message_date = parser.parse(message['Date'])
            eitem["tz"]  = int(message_date.strftime("%z")[0:3])
        except:
            eitem["tz"]  = None

        if self.sortinghat:
            eitem.update(self.get_item_sh(item,"From"))

        if self.prjs_map:
            eitem.update(self.get_item_project(item))

        eitem.update(self.get_grimoire_fields(message['Date'], "message"))

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
        try:
            self.requests.put(url, data = bulk_json)
        except UnicodeEncodeError:
            # Related to body.encode('iso-8859-1'). mbox data
            logging.error("Encoding error ... converting bulk to iso-8859-1")
            bulk_json = bulk_json.encode('iso-8859-1','ignore')
            self.requests.put(url, data=bulk_json)
