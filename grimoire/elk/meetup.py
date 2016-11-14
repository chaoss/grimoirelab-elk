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

from grimoire.elk.enrich import Enrich, metadata

from .utils import unixtime_to_datetime

class MeetupEnrich(Enrich):


    def get_field_author(self):
        return "author"

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
                "group_geolocation": {
                   "type": "geo_point"
                }
           }
        } """

        return {"items":mapping}

    def get_identities(self, item):
        ''' Return the identities from an item '''

        identities = []
        item = item['data']

        # Creators

        # rsvps

        return identities


    def get_sh_identity(self, item, identity_field=None):
		# "owner": {
        #             "first_name": "Huda",
        #             "last_name": "Sarfraz",
        #             "_url": "https://reps.mozilla.org/api/beta/users/959/",
        #             "display_name": "huda_sarfraz"
        #          },
        identity = {'username':None, 'email':None, 'name':None}

        if not item:
            return identity

        user = item
        if 'data' in item and type(item) == dict:
            user = item['data'][identity_field]

        identity['username'] = user["name"]
        identity['email'] = None
        identity['name'] = user["name"]

        return identity

    @metadata
    def get_rich_item(self, item):
        # We need to detect the category of item: activities (report), events or users
        eitem = {}

        copy_fields = ["metadata__updated_on", "metadata__timestamp",
                       "ocean-unique-id", "origin"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None

        event = item['data']

        # data fields to copy
        copy_fields = ["description", "duration", "how_to_find_us", "name",
                       "rsvp_limit", "visibility", "waitlist_count",
                       "yes_rsvp_count", "status"]
        for f in copy_fields:
            if f in event:
                eitem[f] = event[f]
            else:
                eitem[f] = None

        # Fields which names are translated
        map_fields = {
            "link": "url"
        }
        for fn in map_fields:
            eitem[map_fields[fn]] = event[fn]

        eitem['description_analyzed'] = eitem['description']

        eitem['author'] = event['event_hosts'][0]["name"]

        eitem['group'] = event['group']["name"]
        eitem['group_url'] = event['group']["urlname"]
        eitem['group_geolocation'] = {
            "lat": event['group']['lat'],
            "lon": event['group']['lon'],
        }

        eitem['geolocation'] = {
            "lat": event['venue']['lat'],
            "lon": event['venue']['lon'],
        }

        # Dates from timestamp
        eitem['time'] = unixtime_to_datetime(event['time']/1000).isoformat()
        eitem['updated'] = unixtime_to_datetime(event['updated']/1000).isoformat()
        eitem['created'] = unixtime_to_datetime(event['created']/1000).isoformat()

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        return eitem
