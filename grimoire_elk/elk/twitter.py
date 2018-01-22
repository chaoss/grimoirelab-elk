#!/usr/bin/python3
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

        if es_major != '2':
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
        else:
            mapping = """
            {
                "properties": {
                    "text_analyzed": {
                      "type": "string",
                      "index": "analyzed"
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

    def get_field_date(self):
        return "created_at"

    def get_field_unique_id(self):
        return "id_str"

    def get_sh_identity(self, item, identity_field=None):
        identity = {}
        identity['username'] = None
        identity['email'] = None
        identity['name'] = None

        if identity_field is None:
            identity_field = self.get_field_author()

        if identity_field in item:
            identity['username'] = item[identity_field]['screen_name']
            identity['name'] = item[identity_field]['name']
        return identity

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

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
            if tag in self.prjs_map[ds_name]:
                project = self.prjs_map[ds_name][tag]
                break

        if project is None:
            project = DEFAULT_PROJECT

        eitem_project = {"project": project}

        eitem_project.update(self.add_project_levels(project))

        return eitem_project

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        # The real data
        tweet = item

        # data fields to copy
        copy_fields = ["id", "id_str", "lang", "place", "retweet_count",
                       "text", "in_reply_to_user_id_str",
                       "in_reply_to_screen_name", "metadata__timestamp"]
        for f in copy_fields:
            if f in tweet:
                eitem[f] = tweet[f]
            else:
                eitem[f] = None
        # Date fields
        eitem["created_at"] = parser.parse(tweet["created_at"]).isoformat()
        # Fields which names are translated
        map_fields = {"@timestamp": "timestamp",
                      "@version": "version"
                      }
        for f in map_fields:
            if f in tweet:
                eitem[map_fields[f]] = tweet[f]
            else:
                eitem[map_fields[f]] = None

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

        if self.sortinghat:
            eitem.update(self.get_item_sh(tweet))

        eitem.update(self.get_grimoire_fields(tweet["created_at"], "twitter"))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        return eitem
