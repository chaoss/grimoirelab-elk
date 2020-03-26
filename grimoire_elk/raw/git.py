# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import hashlib

from .elastic import ElasticOcean
from ..elastic_mapping import Mapping as BaseMapping


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        Ensure data.message is string, since it can be very large

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = '''
         {
            "dynamic":true,
            "properties": {
                "data": {
                    "properties": {
                        "message": {
                            "type": "text",
                            "index": true
                        }
                    }
                }
            }
        }
        '''

        return {"items": mapping}


class GitOcean(ElasticOcean):
    """Git Ocean feeder"""

    mapping = Mapping

    @classmethod
    def get_perceval_params_from_url(cls, url):
        params = []
        tokens = url.split(' ', 1)  # Just split the URL not the filter
        url = tokens[0]
        params.append(url)

        return params

    def _hash(self, name):
        sha1 = hashlib.sha1(name.encode('UTF-8', errors="surrogateescape"))
        return sha1.hexdigest()

    def _get_identity(self, git_user):
        identity = {}
        fields = git_user.split("<")
        identity['name'] = fields[0].strip()
        try:
            email = fields[1][:-1]
            identity['domain'] = email.split("@")[1]
        except IndexError:
            identity['domain'] = 'unknown'

        return identity

    def _anonymize_item(self, item):
        """ Remove or hash the fields that contain personal information """
        item = item['data']

        if item['Author']:
            author = self._get_identity(item['Author'])
            item['Author'] = "{} <xxxxxx@{}>".format(self._hash(author['name']), author['domain'])
        if item['Commit']:
            commit = self._get_identity(item['Commit'])
            item['Commit'] = "{} <xxxxxx@{}>".format(self._hash(commit['name']), commit['domain'])
