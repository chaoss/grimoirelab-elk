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

from grimoire.elk.enrich import Enrich

class ReMoEnrich(Enrich):

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "description_analyzed": {
                  "type": "string",
                  "index":"analyzed"
                  },
                "geolocation": {
                   "type": "geo_point"
                },
                "functional_areas": {
                  "type": "string",
                  "index":"analyzed"
                  }
           }
        } """

        return {"items":mapping}

    def get_identities(self, item):
        ''' Return the identities from an item '''

        identities = []
        item = item['data']
        if 'owner' in item:
            identities.append(self.get_sh_identity(item['owner']))
        if 'user' in item:
            identities.append(self.get_sh_identity(item['user']))
        if 'mentor' in item:
            identities.append(self.get_sh_identity(item['mentor']))

        return identities

    def get_sh_identity(self, owner):
		# "owner": {
        #             "first_name": "Huda",
        #             "last_name": "Sarfraz",
        #             "_url": "https://reps.mozilla.org/api/beta/users/959/",
        #             "display_name": "huda_sarfraz"
        #          },
        identity = {'username':None, 'email':None, 'name':None}

        if not owner:
           return identity
        identity['username'] = owner["display_name"]
        identity['email'] = None
        identity['name'] = owner["first_name"]+" "+owner["last_name"]

        return identity

    def get_item_category(self, item):
        # We need to detect the category of item: activities (report), events or users
        category = None

        item = item['data']

        if 'estimated_attendance' in item:
            category = 'events'
        elif 'activity' in item:
            category = 'activities'
        elif 'first_name' in item:
            category = 'users'
        else:
            logging.error("Can not detect category in item %s", item)

        return category

    def get_rich_item(self, item):
        # We need to detect the category of item: activities (report), events or users
        eitem = {}

        eitem.update(self.__get_rich_item_common(item))

        category = self.get_item_category(item)

        if category == 'activities':
            eitem.update(self.__get_rich_item_activities(item))
        elif category == 'users':
            eitem.update(self.__get_rich_item_users(item))
        elif category == 'events':
            eitem.update(self.__get_rich_item_events(item))

        return eitem

    def __get_rich_item_common(self, item):
        # metadata fields to copy

        eitem = {}

        copy_fields = ["metadata__updated_on","metadata__timestamp","ocean-unique-id","origin","offset"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None

        return eitem


    def __get_rich_item_activities(self, item):
        eitem = {}

        # The real data
        activity = item['data']


        # data fields to copy
        copy_fields = ["initiative","location","activity","link",
                       "activity_description","remo_url",
                       "external_link","latitude","longitude", "report_date"]
        for f in copy_fields:
            if f in activity:
                eitem[f] = activity[f]
            else:
                eitem[f] = None


        eitem['user_profile_url'] = activity['user']['_url']
        eitem['user'] = activity['user']['first_name']+" "+activity['user']['last_name']

        if 'mentor' in activity and activity['mentor']:
            eitem['mentor_profile_url'] = activity['mentor']['_url']
            eitem['mentor'] = activity['mentor']['first_name']+" "+activity['mentor']['last_name']

        # geolocation
        eitem['geolocation'] = {
            "lat": eitem['latitude'],
            "lon": eitem['longitude'],
        }

        eitem['functional_areas'] = ''
        for area in eitem['functional_areas']:
            eitem['functional_areas'] += "," + area['name']

        if self.sortinghat:
            eitem.update(self.get_item_sh(item, "user"))

        return eitem

    def __get_rich_item_users(self, item):
        eitem = {}

        return eitem

    def __get_rich_item_events(self, item):
        eitem = {}

        # The real data
        event = item['data']

        # data fields to copy
        copy_fields = ["initiative","city","country",
                       "description","estimated_attendance","remo_url",
                       "external_link","lat","lon", "region","timezone",
                       "planning_pad_url","hashtag"]
        for f in copy_fields:
            if f in event:
                eitem[f] = event[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {
            "description": "description_analyzed",
            "end":"end_date",
            "start":"start_date",
            "name":"title"
        }
        for fn in map_fields:
            eitem[map_fields[fn]] = event[fn]

        eitem['owner_profile_url'] = None
        eitem['owner'] = None
        if 'owner' in event:
            eitem['owner_profile_url'] = event['owner']['_url']
            eitem['owner'] = event['owner']['display_name']

        # geolocation
        eitem['geolocation'] = {
            "lat": eitem['lat'],
            "lon": eitem['lon'],
        }

        if self.sortinghat:
            eitem.update(self.get_item_sh(item, "owner"))

        return eitem
