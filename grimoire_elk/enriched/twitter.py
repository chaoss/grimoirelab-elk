# -*- coding: utf-8 -*-
#
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

import logging

from dateutil import parser
from requests.structures import CaseInsensitiveDict

from .enrich import Enrich, metadata, DEFAULT_PROJECT
from ..elastic_mapping import Mapping as BaseMapping


logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
                "text_analyzed": {
                  "type": "text"
                  },
                  "geolocation": {
                     "type": "geo_point"
                  }
           }
        } """

        return {"items": mapping}


class TwitterEnrich(Enrich):

    mapping = Mapping

    def get_field_author(self):
        return "user"

    def get_sh_identity(self, item, identity_field=None):
        identity = {}
        identity['username'] = None
        identity['email'] = None
        identity['name'] = None

        if identity_field is None:
            identity_field = self.get_field_author()

        tweet = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
            tweet = item['data']

        if identity_field in tweet:
            identity['username'] = tweet[identity_field]['screen_name']
            identity['name'] = tweet[identity_field]['name']
        return identity

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        item = item['data']

        user = self.get_sh_identity(item)
        identities.append(user)

        return identities

    def get_item_project(self, eitem):
        """ Get project mapping enrichment field.

        Twitter mappings is pretty special so it needs a special
        implementacion.
        """

        project = None
        eitem_project = {}
        ds_name = self.get_connector_name()  # data source name in projects map

        if ds_name not in self.prjs_map:
            return eitem_project

        for tag in eitem['hashtags_analyzed']:
            # lcanas: hashtag provided in projects.json file should not be case sensitive T6876
            tags2project = CaseInsensitiveDict(self.prjs_map[ds_name])
            if tag in tags2project:
                project = tags2project[tag]
                break

        if project is None:
            project = DEFAULT_PROJECT

        eitem_project = {"project": project}

        eitem_project.update(self.add_project_levels(project))

        return eitem_project

    @metadata
    def get_rich_item(self, item):
        eitem = {}
        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None

        # The real data
        tweet = item['data']

        # data fields to copy
        copy_fields = ["id", "id_str", "lang", "place", "retweet_count",
                       "text", "in_reply_to_user_id_str",
                       "in_reply_to_screen_name"]
        for f in copy_fields:
            if f in tweet:
                eitem[f] = tweet[f]
            else:
                eitem[f] = None

        # Date fields
        eitem["created_at"] = parser.parse(tweet["created_at"]).isoformat()

        # data fields to copy from user
        copy_fields = ["created_at", "description", "followers_count",
                       "friends_count", "id_str", "lang", "location", "name",
                       "url", "utc_offset", "verified"]
        for f in copy_fields:
            if f in tweet['user']:
                eitem["user_" + f] = tweet['user'][f]
            else:
                eitem["user_" + f] = None

        if "text" in tweet:
            eitem["text_analyzed"] = tweet["text"]

        eitem['hashtags_analyzed'] = []
        for tag in tweet['entities']['hashtags']:
            eitem['hashtags_analyzed'].append(tag['text'])

        eitem['retweeted'] = 0
        if tweet['retweeted']:
            eitem['retweeted'] = 1

        eitem['url'] = "http://twitter.com/" + tweet['user']['screen_name']
        eitem['url'] += "/status/" + tweet['id_str']
        eitem['user_url_twitter'] = "http://twitter.com/" + tweet['user']['screen_name']

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "tweet"))

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        return eitem
