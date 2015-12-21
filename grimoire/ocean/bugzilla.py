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

class BugzillaOcean(ElasticOcean):

    def get_identities(self, item):
        ''' Return the identities from an item '''

        def add_identity(identities, user):
            identity = {}
            for field in ['name', 'email', 'username']:
                identity[field] = None
            if 'name' in user: identity['name'] = user['name']
            if 'email' in user: identity['email'] = user['email']
            if 'username' in user: identity['username'] = user['username']
            if 'assigned_to_name' in user:
                    identity['name'] = user['assigned_to_name']
                    identity['username'] = user['assigned_to']
            elif 'assigned_to' in user: identity['username'] = user['assigned_to']
            if 'changed_by' in user: identity['name'] = user['changed_by']
            if 'who' in user: identity['username'] = user['who']
            if 'reporter' in user:
                    identity['name'] = user['reporter_name']
                    identity['username'] = user['reporter']
            identities.append(identity)

        identities = []

        if 'changes' in item:
            for change in item['changes']:
                add_identity(identities, change)
        if 'long_desc' in item:
            for comment in item['long_desc']:
                add_identity(identities, comment)
        if 'assigned_to_name' in item:
            add_identity(identities, {'assigned_to_name': item['assigned_to_name'],
                                      'assigned_to':item['assigned_to']})
        elif 'assigned_to' in item:
            add_identity(identities, {'assigned_to': item['assigned_to']})
        if 'reporter_name' in item:
            add_identity(identities, {'reporter_name': item['reporter_name'],
                                      'reporter':item['reporter']})
        if 'reporter_name' in item:
            add_identity(identities, {'reporter_name': item['reporter_name'],
                                      'reporter':item['reporter']})

        return identities


    def get_field_date(self):
        field = None
        if self.perceval_backend.detail == "list":
            field = 'changeddate_date'
        else:
            field = 'delta_ts_date'

        return field


    def get_last_update_from_es(self):
        ''' Find in JSON storage the last update date '''

        last_update = self.elastic.get_last_date(self.get_field_date())
            # Format date so it can be used as URL param in bugzilla
        if last_update is not None:
            last_update = last_update.replace("T", " ")

        return last_update




