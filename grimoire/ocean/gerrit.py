#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Gerrit Ocean feeder
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

'''Gerrit Ocean feeder'''

from grimoire.ocean.elastic import ElasticOcean

class GerritOcean(ElasticOcean):

    def get_field_date(self):
        return "lastUpdated_date"

    def get_identities(self, item):
        ''' Return the identities from an item '''

        def add_identity(identities, user):
            identity = {}
            for field in ['name', 'email', 'username']:
                identity[field] = None
            if 'name' in user: identity['name'] = user['name']
            if 'email' in user: identity['email'] = user['email']
            if 'username' in user: identity['username'] = user['username']
            identities.append(identity)

        identities = []

        # Changeset owner
        user = item['owner']
        add_identity(identities, user)

        # Patchset uploader and author
        if 'patchSets' in item:
            for patchset in item['patchSets']:
                user = patchset['uploader']
                add_identity(identities, user)
                user = patchset['author']
                add_identity(identities, user)
                if 'approvals' in patchset:
                    # Approvals by
                    for approval in patchset['approvals']:
                        user = approval['by']
                        add_identity(identities, user)
        # Comments reviewers
        if 'comments' in item:
            for comment in item['comments']:
                user = comment['reviewer']
                add_identity(identities, user)

        return identities


    def get_elastic_mappings(self):

        mapping = '''
        {
            "properties": {
               "project": {
                  "type": "string",
                  "index":"not_analyzed"
               }
            }
        }
        '''

        return {"items":mapping}

    def get_id(self):
        ''' Return gerrit unique identifier '''
        return self.repository

    def get_field_unique_id(self):
        return "id"


# We need to enrich data with it
#         entry_lastUpdated = \
#             datetime.fromtimestamp(entry['lastUpdated'])
#         entry['lastUpdated_date'] = entry_lastUpdated.isoformat()

    



