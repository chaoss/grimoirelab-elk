# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2023 Bitergia
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
#   Jose Javier Merchante Picazo <jjmerchante@cauldron.io>
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
