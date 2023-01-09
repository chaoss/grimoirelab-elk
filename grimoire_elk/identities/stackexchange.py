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


class StackExchangeIdentities(Identities):
    @classmethod
    def anonymize_item(cls, item):
        """Remove or hash the fields that contain personal information

        Comments are removed because can cause complexity, there could be many
        and are not used in the enrichment process
        """

        item = item['data']

        item['comments'] = []
        if 'owner' in item and item['owner']:
            cls._sanitize_owner(item['owner'])

        if 'answers' in item and item['answers']:
            for answer in item['answers']:
                if 'owner' in answer and answer['owner']:
                    cls._sanitize_owner(answer['owner'])
                answer['comments'] = []

    @classmethod
    def _sanitize_owner(cls, owner):
        """Remove links and hash personal information"""
        if 'display_name' in owner:
            owner['display_name'] = cls._hash(owner['display_name'])
        if 'user_id' in owner:
            owner['user_id'] = cls._hash(str(owner['user_id']))
        owner['profile_image'] = ''
        owner['link'] = ''
