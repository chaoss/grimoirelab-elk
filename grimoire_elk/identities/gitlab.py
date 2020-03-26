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
#   Jose Javier Merchante Picazo <jjmerchante@gcauldron.io>
#

from grimoire_elk.identities.identities import Identities


class GitlabIdentities(Identities):
    @classmethod
    def anonymize_item(cls, item):
        """Remove or hash the fields that contain personal information"""

        category = item['category']

        item = item['data']

        if category == "issue":
            identity_types = ['author', 'assignee']
        elif category == "merge_request":
            identity_types = ['author', 'merged_by']
        else:
            identity_types = []

        for identity in identity_types:
            if identity not in item:
                continue
            if not item[identity]:
                continue

            item[identity] = {
                'username': cls._hash(item[identity]['username']),
                'name': cls._hash(item[identity]['name']),
                'email': None,
                'organization': None,
                'location': None
            }
