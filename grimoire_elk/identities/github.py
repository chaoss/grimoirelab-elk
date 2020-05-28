# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2020 Bitergia
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
#   Nitish Gupta <imnitish.ng@gmail.com>
#

from grimoire_elk.identities.identities import Identities


class GitHubIdentities(Identities):

    @classmethod
    def anonymize_item(cls, item):
        """Remove or hash the fields that contain personal information"""

        category = item['category']

        item = item['data']
        comments_attr = None
        if category == "issue":
            identity_types = ['user', 'assignee']
            comments_attr = 'comments_data'
        elif category == "pull_request":
            identity_types = ['user', 'merged_by']
            comments_attr = 'review_comments_data'
        else:
            identity_types = []

        for identity in identity_types:
            if identity not in item:
                continue
            if not item[identity]:
                continue

            identity_attr = identity + "_data"

            item[identity] = {
                'login': cls._hash(item[identity]['login'])
            }

            item[identity_attr] = {
                'name': cls._hash(item[identity_attr]['login']),
                'login': cls._hash(item[identity_attr]['login']),
                'email': None,
                'company': None,
                'location': None,
            }

        comments = item.get(comments_attr, [])
        for comment in comments:
            if 'user' in comment and comment['user']:
                comment['user'] = {
                    'login': cls._hash(comment['user']['login'])
                }
            comment['user_data'] = {
                'name': cls._hash(comment['user_data']['login']),
                'login': cls._hash(comment['user_data']['login']),
                'email': None,
                'company': None,
                'location': None,
            }
            for reaction in comment['reactions_data']:
                reaction['user'] = {
                    'login': cls._hash(reaction['user']['login'])
                }

    def get_field_author(self):
        return "user_data"

    def get_identities(self, item):
        """Return the identities from an item"""

        category = item['category']
        item = item['data']

        if category == "issue":
            identity_types = ['user', 'assignee']
        elif category == "pull_request":
            identity_types = ['user', 'merged_by']
        else:
            identity_types = []

        for identity in identity_types:
            identity_attr = identity + "_data"
            if item[identity] and identity_attr in item:
                # In user_data we have the full user data
                user = self.get_sh_identity(item[identity_attr])
                if user:
                    yield user

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]

        if not user:
            return identity

        identity['username'] = user['login']
        identity['email'] = None
        identity['name'] = None
        if 'email' in user:
            identity['email'] = user['email']
        if 'name' in user:
            identity['name'] = user['name']
        return identity
