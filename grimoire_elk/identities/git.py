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


class GitIdentities(Identities):

    @staticmethod
    def _get_identity(git_user):
        identity = {}
        fields = git_user.split("<")
        identity['name'] = fields[0].strip()
        try:
            email = fields[1][:-1]
            identity['domain'] = email.split("@")[1]
        except IndexError:
            identity['domain'] = 'unknown'

        return identity

    @classmethod
    def anonymize_item(cls, item):
        """Remove or hash the fields that contain personal information"""

        item = item['data']

        if item['Author']:
            author = cls._get_identity(item['Author'])
            item['Author'] = "{} <xxxxxx@{}>".format(cls._hash(author['name']), author['domain'])
        if item['Commit']:
            commit = cls._get_identity(item['Commit'])
            item['Commit'] = "{} <xxxxxx@{}>".format(cls._hash(commit['name']), commit['domain'])
