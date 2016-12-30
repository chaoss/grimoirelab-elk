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

from .enrich import Enrich, metadata

class MediaWikiEnrich(Enrich):

    def get_field_unique_id_review(self):
        return "revision_revid"

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "title_analyzed": {
                  "type": "string",
                  "index":"analyzed"
                  }
           }
        } """

        return {"items":mapping}


    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        revisions = item['data']['revisions']

        for revision in revisions:
            user = self.get_sh_identity(revision)
            identities.append(user)
        return identities

    def get_field_author(self):
        return 'user'

    def get_sh_identity(self, item, identity_field=None):

        revision = item

        if 'data' in item and type(item) == dict:
            revision = item['data']['revisions'][0]

        identity = {}
        identity['username'] = None
        identity['email'] = None
        identity['name'] = None

        if  identity_field in revision:
            identity['username'] = revision[identity_field]
            identity['name'] = revision[identity_field]
        return identity

    def get_review_sh(self, revision, item):
        """ Add sorting hat enrichment fields for the author of the revision """

        identity  = self.get_sh_identity(revision)
        update =  parser.parse(item[self.get_field_date()])
        erevision = self.get_item_sh_fields(identity, update)

        return erevision

    def get_rich_item_reviews(self, item):
        erevisions = []
        eitem = {}

        # All revisions include basic page info
        eitem = self.get_rich_item(item)

        # Revisions
        for rev in item["data"]["revisions"]:
            erevision = {}
            # Metadata needed for enrichment
            copy_fields_item = ["origin","metadata__updated_on","metadata__timestamp"]
            for f in copy_fields_item:
                if f in eitem:
                    erevision[f] = eitem[f]
                else:
                    erevision[f] = None
            # Metadata related to the page according to the enrichment specification
            copy_fields_item = ["origin","metadata__updated_on","metadata__timestamp","pageid","title"]
            for f in copy_fields_item:
                if f in eitem:
                    erevision["page_"+f] = eitem[f]
                else:
                    erevision["page_"+f] = None
            # Copy fields from the review
            copy_fields = ["revid","user","parentid","timestamp","comment"]
            for f in copy_fields:
                if f in rev:
                    erevision["revision_"+f] = rev[f]
                else:
                    erevision["revision_"+f] = None
            if self.sortinghat:
                erevision.update(self.get_review_sh(rev, item))

            # And now some calculated fields
            erevision["url"] = erevision["page_origin"] + "/" + erevision["page_title"]
            erevision["iscreated"] = 0
            erevision["creation_date"] = None
            erevision["isrevision"] = 0
            if rev['parentid'] == 0:
                erevision["iscreated"] = 1
                erevision["creation_date"] = rev['timestamp']
            else:
                erevision["isrevision"] = 1
            erevision["page_last_edited_date"] = eitem['last_edited_date']

            erevisions.append(erevision)

        return erevisions

    @metadata
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
        page = item['data']

        # data fields to copy
        copy_fields = ["pageid","title"]
        for f in copy_fields:
            if f in page:
                eitem[f] = page[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"title": "title_analyzed"}
        for fn in map_fields:
            eitem[map_fields[fn]] = page[fn]

        # Enrich dates
        eitem["update_date"] = parser.parse(item["metadata__updated_on"]).isoformat()
        # Revisions
        eitem["last_edited_date"] = None
        eitem["nrevisions"] = len(page["revisions"])
        if len(page["revisions"])>0:
            eitem["first_editor"] = page["revisions"][0]["user"]
            eitem["last_edited_date"] = page["revisions"][-1]["timestamp"]

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        return eitem

    def enrich_items(self, items):
        if True:
            # Hack: by default we use events in MediaWiki
            return self.enrich_events(items)
        else:
            super(MediaWikiEnrich, self).enrich_items(items)


    def enrich_events(self, items):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""
        total = 0

        url = self.elastic.index_url+'/items/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))

        for item in items:
            rich_item_reviews = self.get_rich_item_reviews(item)
            for enrich_review in rich_item_reviews:
                if current >= max_items:
                    self.requests.put(url, data=bulk_json)
                    bulk_json = ""
                    current = 0
                data_json = json.dumps(enrich_review)
                bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                    (enrich_review[self.get_field_unique_id_review()])
                bulk_json += data_json +"\n"  # Bulk document
                current += 1
                total += 1
        self.requests.put(url, data = bulk_json)

        return total
