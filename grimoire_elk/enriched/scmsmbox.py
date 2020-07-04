# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Ria Gupta <ria18405@iiitd.ac.in>
#
import json
import logging

from requests.structures import CaseInsensitiveDict
import email.utils
from .enrich import metadata
from .scmsenrich import ScmsEnrich

logger = logging.getLogger(__name__)


class ScmsMboxEnrich(ScmsEnrich):

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []
        self.studies.append(self.enrich_extra_data)

    def get_field_author(self):
        return "From"

    def get_identities(self, item):
        """ Return the identities from an item """

        item = item['data']
        for identity in ['From']:
            if identity in item and item[identity]:
                user = self.get_sh_identity(item[identity])
                yield user

    def get_sh_identity(self, item, identity_field=None):
        # "From": "hwalsh at wikiledia.net (Heat Walsh)"

        identity = {f: None for f in ['email', 'name', 'username']}

        from_data = item
        if 'data' in item and type(item) == dict:
            from_data = item['data'][identity_field]

        # First desofuscate the email
        EMAIL_OBFUSCATION_PATTERNS = [' at ', '_at_', ' en ']
        for pattern in EMAIL_OBFUSCATION_PATTERNS:
            if not from_data:
                return identity
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
        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)
        # The real data
        message = CaseInsensitiveDict(item['data'])

        # Fields which names are translated
        map_fields = {
            "Subject": "context",
            "uuid": "id"
        }
        for fn in map_fields:
            if fn in message:
                eitem[map_fields[fn]] = message[fn]
            else:
                eitem[map_fields[fn]] = None

        if 'uuid' in item:
            eitem['id'] = item['uuid']

        if 'plain' in message['body']:
            eitem['body'] = " ".join(message['body']['plain'].split("\n")[:])

        eitem.update(self.get_grimoire_fields(message['Date'], "message"))
        eitem['data_source'] = 'Mbox'
        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        return eitem

    def enrich_items(self, ocean_backend):
        # items is a generator but we need to reuse it so we store all items
        # from the generator in a list
        # items = list(items)

        # Use standard method and if fails, use the old one with Unicode control
        total = 0
        try:
            total = super(ScmsMboxEnrich, self).enrich_items(ocean_backend)
        except UnicodeEncodeError:
            total = self.enrich_items_old(ocean_backend.fetch())

        return total

    def enrich_items_old(self, items):
        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        url = self.elastic.get_bulk_url()

        logger.debug("[mbox] Adding items to {} (in {} packs)".format(self.elastic.anonymize_url(url), max_items))

        for item in items:
            if current >= max_items:
                total += self.elastic.safe_put_bulk(url, bulk_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (rich_item[self.get_field_unique_id()])
            bulk_json += data_json + "\n"  # Bulk document
            current += 1

        if current == 0:
            return total

        try:
            total += self.elastic.safe_put_bulk(url, bulk_json)
        except UnicodeEncodeError:
            # Related to body.encode('iso-8859-1'). mbox data
            logger.error("[mbox] Encoding error ... converting bulk to iso-8859-1")
            bulk_json = bulk_json.encode('iso-8859-1', 'ignore')

            total += self.elastic.safe_put_bulk(url, bulk_json)

        return total
