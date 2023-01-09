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


class MeetupIdentities(Identities):
    @classmethod
    def anonymize_item(cls, item):
        """Remove or hash the fields that contain personal information"""

        item = item['data']

        if 'event_hosts' in item:
            for i, host in enumerate(item['event_hosts']):
                item['event_hosts'][i] = {
                    'id': host['id'],
                    'name': cls._hash(host['name']),
                }

        if 'rsvps' in item:
            for rsvp in item['rsvps']:
                rsvp['member'] = {
                    'id': rsvp['member']['id'],
                    'name': cls._hash(rsvp['member']['name']),
                    'event_context': {'host': rsvp['member']['event_context']['host']}
                }

        if 'comments' in item:
            for comment in item['comments']:
                comment['member'] = {
                    'id': comment['member']['id'],
                    'name': cls._hash(comment['member']['name'])
                }
