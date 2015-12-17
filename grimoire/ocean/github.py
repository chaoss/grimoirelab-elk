#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# GitHub Ocean feeder
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

'''GitHub Ocean feeder'''

import requests

from grimoire.ocean.elastic import ElasticOcean

class GitHubOcean(ElasticOcean):

    users = {}  # cache with GitHub users

    def __init__(self, perceval_backend, cache = False,
                 incremental = True, **nouse):
        # Read user data from cache in ES and call parent __init__
        if len(GitHubOcean.users.keys()) == 0:
            # If the cache is not loaded yet, load it
            GitHubOcean.users = {}
        super(GitHubOcean, self).__init__(perceval_backend, cache,
                                          incremental, **nouse)

    def get_field_date(self):
        return "updated_at"

    def get_identities(self, item):
        ''' Return the identities from an item '''
        identities = []

        for identity in ['user', 'assignee']:
            if item[identity]:
                user = self.get_user(item[identity]['login'])
                identities.append({
                                   "name":user['name'],
                                   "email":user['email'],
                                   "username":user['login'],
                                   "id":user['id'],
                                   "location":user['location'],
                                   "company":user['company'],
                                   "orgs": user['orgs']
                                   })
        return identities

    def get_user(self, login):
        if login not in GitHubOcean.users:
            user = self.perceval_backend.client.get_user(login)
            GitHubOcean.users[login] = user
            orgs = self.perceval_backend.client.get_user_orgs(login)
            GitHubOcean.users[login]['orgs'] = orgs
        return GitHubOcean.users[login]

    def drop_item(self, item):
        ''' Drop items not to be inserted in Elastic '''
        drop = False
        if not 'head' in item.keys() and not 'pull_request' in item.keys():
            drop = True
        return drop
