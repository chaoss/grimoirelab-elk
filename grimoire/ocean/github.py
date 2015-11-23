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

from perceval.backends.github import GitHub
from grimoire.ocean.elastic import ElasticOcean

class GitHubOcean(ElasticOcean):

    users = {}

    def get_field_date(self):
        return "updated_at"


    def getUser(self, url, login):
        if login not in GitHub.users:

            url = url + "/users/" + self.login

            r = requests.get(url, verify=False,
                             headers={'Authorization':'token ' + self.auth_token})
            user = r.json()

            GitHub.users[self.login] = user

            # Get the public organizations also
            url += "/orgs"
            r = requests.get(url, verify=False,
                             headers={'Authorization':'token ' + self.auth_token})
            orgs = r.json()

            GitHub.users[self.login]['orgs'] = orgs

    def drop_item(self, item):
        ''' Drop items not to be inserted in Elastic '''
        drop = False
        if not 'head' in item.keys() and not 'pull_request' in item.keys():
            drop = True
        return drop


