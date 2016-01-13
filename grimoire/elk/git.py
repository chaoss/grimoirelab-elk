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

class GitEnrich(Enrich):

    def __init__(self, git):
        super().__init__()
        self.elastic = None
        self.perceval_backend = git
        self.index_git = "git"

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_date(self):
        return "CommitDate"

    def get_field_unique_id(self):
        return "hash"

    def get_fields_uuid(self):
        return ["author_uuid", "committer_uuid"]

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
               "author_name": {
                  "type": "string",
                  "index":"not_analyzed"
               },
               "org_name": {
                  "type": "string",
                  "index":"not_analyzed"
               }
            }
        } """

        return {"items":mapping}


    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        for identity in ['Author', 'Commit']:
            if item[identity]:
                user = self.get_sh_identity(item[identity])
                identities.append(user)
        return identities

    def get_sh_identity(self, git_user):
        # John Smith <john.smith@bitergia.com>
        identity = {}
        name = git_user.split("<")[0]
        email = git_user.split("<")[1][:-1]
        identity['username'] = None  # git does not have username
        identity['email'] = email
        identity['name'] = name
        return identity


    def get_rich_commit(self, commit):
        eitem = {}
        # Fields that are the same in item and eitem
        copy_fields = ["message"]
        for f in copy_fields:
            eitem[f] = commit[f]
        # Fields which names are translated
        map_fields = {"commit": "hash"}
        for fn in map_fields:
            eitem[map_fields[fn]] = commit[fn]
        # Enrich dates
        author_date = parser.parse(commit["AuthorDate"])
        commit_date = parser.parse(commit["CommitDate"])
        eitem["author_date"] = author_date.replace(tzinfo=None).isoformat()
        eitem["commit_date"] = commit_date.replace(tzinfo=None).isoformat()
        eitem["utc_author"] = (author_date-author_date.utcoffset()).replace(tzinfo=None).isoformat()
        eitem["utc_commit"] = (commit_date-commit_date.utcoffset()).replace(tzinfo=None).isoformat()
        eitem["tz"]  = int(commit_date.strftime("%z")[0:3])
        # Enrich SH
        identity  = self.get_sh_identity(commit["Author"])
        eitem["author_name"] = identity['name']
        eitem["author_uuid"] = self.get_uuid(identity, self.get_connector_name())
        enrollments = api.enrollments(self.sh_db, uuid=eitem["author_uuid"])
        # TODO: get the org_name for the current commit time
        if len(enrollments) > 0:
            eitem["org_name"] = enrollments[0].organization.name
        else:
            eitem["org_name"] = None
        # bot
        u = api.unique_identities(self.sh_db, eitem["author_uuid"])[0]
        if u.profile:
            eitem["bot"] = u.profile.is_bot
        else:
            eitem["bot"] = 0  # By default, identities are not bots
        # Other enrichment
        eitem["repo_name"] = self.perceval_backend.unique_id

        return eitem

    def enrich_items(self, commits):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        url = self.elastic.index_url+'/items/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))

        for commit in commits:
            if current >= max_items:
                requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            rich_commit = self.get_rich_commit(commit)
            data_json = json.dumps(rich_commit)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (rich_commit[self.get_field_unique_id()])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        requests.put(url, data = bulk_json)
