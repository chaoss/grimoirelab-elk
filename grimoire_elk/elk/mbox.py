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

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping
from .mbox_study_kip import kafka_kip, MAX_LINES_FOR_VOTE


logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        if es_major != '2':
            fielddata = ', "fielddata": true'
        else:
            fielddata = ''

        if es_major != '2':
            mapping = """
            {
                "properties": {
                     "Subject_analyzed": {
                       "type": "string"
                     },
                     "body": {
                       "type": "string"
                     }
               }
            } """
        else:
            mapping = """
            {
                "properties": {
                     "Subject_analyzed": {
                      "type": "string",
                      "index": "analyzed"
                       %s
                     },
                     "body": {
                      "type": "string",
                      "index": "analyzed"
                     }
               }
            } """ % fielddata

        return {"items": mapping}


class MBoxEnrich(Enrich):

    mapping = Mapping

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = [self.kafka_kip]

    def get_field_author(self):
        return "From"

    def get_fields_uuid(self):
        return ["from_uuid"]

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        item = item['data']
        for identity in ['From']:
            if identity in item and item[identity]:
                user = self.get_sh_identity(item[identity])
                identities.append(user)
        return identities

    def get_sh_identity(self, item, identity_field=None):
        # "From": "hwalsh at wikiledia.net (Heat Walsh)"

        identity = {}

        from_data = item
        if 'data' in item and type(item) == dict:
            from_data = item['data'][identity_field]

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

    def get_project_repository(self, eitem):
        mls_list = eitem['origin']
        # This should be configurable
        mboxes_dir = '/home/bitergia/mboxes/'
        repo = mls_list + " " + mboxes_dir
        repo += mls_list + ".mbox/" + mls_list + ".mbox"
        return repo

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        message = item['data']

        # Fields that are the same in message and eitem
        copy_fields = ["Date", "From", "Subject", "Message-ID"]
        for f in copy_fields:
            if f in message:
                eitem[f] = message[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {
            "Subject": "Subject_analyzed"
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

        # Part of the body is needed in studies like kafka_kip
        eitem["body_extract"] = ""
        # Size of the message
        eitem["size"] = None
        if 'plain' in message['body']:
            eitem["body_extract"] = "\n".join(message['body']['plain'].split("\n")[:MAX_LINES_FOR_VOTE])
            eitem["size"] = len(message['body']['plain'])

        # Time zone
        try:
            message_date = parser.parse(message['Date'])
            eitem["tz"] = int(message_date.strftime("%z")[0:3])
        except Exception:
            eitem["tz"] = None

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(message['Date'], "message"))

        return eitem

    def enrich_items(self, ocean_backend):
        # items is a generator but we need to reuse it so we store all items
        # from the generator in a list
        # items = list(items)

        # Use standard method and if fails, use the old one with Unicode control
        total = 0
        try:
            total = super(MBoxEnrich, self).enrich_items(ocean_backend)
        except UnicodeEncodeError:
            total = self.enrich_items_old(ocean_backend.fetch())

        return total

    def enrich_items_old(self, items):
        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        url = self.elastic.index_url + '/items/_bulk'

        logger.debug("Adding items to %s (in %i packs)" % (url, max_items))

        for item in items:
            if current >= max_items:
                self.requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (rich_item[self.get_field_unique_id()])
            bulk_json += data_json + "\n"  # Bulk document
            current += 1
            total += 1
        try:
            self.requests.put(url, data=bulk_json)
        except UnicodeEncodeError:
            # Related to body.encode('iso-8859-1'). mbox data
            logger.error("Encoding error ... converting bulk to iso-8859-1")
            bulk_json = bulk_json.encode('iso-8859-1', 'ignore')
            self.requests.put(url, data=bulk_json)

        return total

    def kafka_kip(self, enrich_backend, no_incremental=False):
        # KIP study is not incremental
        kafka_kip(self)
