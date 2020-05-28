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

class AskbotIdentities(Identities):

    @classmethod
    def anonymize_item(cls, item):
        """Remove or hash the fields that contain personal information"""
        category = item['category']

        item = item['data']
        if category == "question":
            identity_types = ['author', 'last_activity_by']
            comments_attr = 'comments'
            answers_attr = 'answers'

        for identity in identity_types:
            if identity not in item:
                continue
            if not item[identity]:
                continue

            item[identity] = {
                'username': cls._hash(item[identity]['username'])
            }

        comments = item.get(comments_attr, [])
        for comment in comments:
            if 'user_display_name' in comment and comment['user_display_name']:
                comment['user_display_name'] = {
                    'user_display_name': cls._hash(comment['user_display_name'])
                }

        answers = item.get(answers_attr, [])
        for answer in answers:
            if 'answered_by' in answer and answer['answered_by']:
                comment['username'] = {
                    'username': cls._hash(comment['username'])
                }

    def get_identities(self, item):
        """ Return the identities from an item """

        # question
        user = self.get_sh_identity(item, self.get_field_author())
        yield user

        # answers
        if 'answers' in item['data']:
            for answer in item['data']['answers']:
                # avoid "answered_by" : "This post is a wiki" corner case
                if type(answer['answered_by']) is dict:
                    user = self.get_sh_identity(answer['answered_by'])
                    yield user
                if 'comments' in answer:
                    for comment in answer['comments']:
                        commenter = self.get_sh_identity(comment)
                        yield commenter

    def get_sh_identity(self, item, identity_field=None):
        identity = {key: None for key in ['username', 'name', 'email']}

        user = item  # by default a specific user dict is expected
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]
        elif 'author' in item:
            user = item['author']

        if user is None:
            return identity

        if 'username' in user:
            identity['username'] = user['username']
        elif 'user_display_name' in user:
            identity['username'] = user['user_display_name']
        elif 'answered_by' in user:
            if 'username' in user['answered_by']:
                identity['username'] = user['answered_by']['username']
            else:
                # "answered_by" : "This post is a wiki"
                identity['username'] = user['answered_by']
        identity['name'] = identity['username']

        return identity

    def get_field_author(self):
        return 'author'
