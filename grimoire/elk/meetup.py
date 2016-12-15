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

        identities = []

        # Creators
        user = self.get_sh_identity(item['event_hosts'][0])
        identities.append(user)

        # rsvps

        return identities


    def get_sh_identity(self, item, identity_field=None):
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
        copy_fields = ["id", "how_to_find_us"]
        for f in copy_fields:
            if f in event:
                eitem[f] = event[f]
            else:
                eitem[f] = None

        # Fields which names are translated
        map_fields = {
            "link": "url",
            "rsvp_limit": "rsvps_limit"
        }
        for fn in map_fields:
            if fn in event:
                eitem[map_fields[fn]] = event[fn]
            else:
                eitem[f] = None


        # event host fields: author of the event
        host = event['event_hosts'][0]
        eitem['member_photo_url'] = host['photo']['photo_link']
        eitem['member_photo_id'] = host['photo']['id']
        eitem['member_photo_type'] = host['photo']['type']
        eitem['member_is_host'] = True
        eitem['member_id'] = host['id']
        eitem['member_name'] = host['name']
        eitem['member_url'] = "https://www.meetup.com/members/" + str(host['id'])
        eitem['author'] = host["name"]

        eitem['event_url'] = event['link']

        # data fields to copy with meetup`prefix
        copy_fields = ["description", "plain_text_description",
                       "created", "name", "status",
                       "time", "updated", "utc_offset", "visibility",
                       "waitlist_count", "yes_rsvp_count", "duration",
                       "featured", "rsvpable"]
        for f in copy_fields:
            if f in event:
                eitem["meetup_"+f] = event[f]
            else:
                eitem[f] = None

        eitem['num_rsvps'] = len(event['rsvps'])
        eitem['num_comments'] = len(event['comments'])

        if 'venue' in event:
            venue = event['venue']
            copy_fields = ["id", "name", "city", "state", "zip", "country",
                           "localized_country_name", "repinned", "address_1"]
            for f in copy_fields:
                if f in event:
                    eitem["venue_"+f] = venue[f]
                else:
                    eitem[f] = None

            eitem['venue_geolocation'] = {
                "lat": event['venue']['lat'],
                "lon": event['venue']['lon'],
            }

        if 'series' in event:
            eitem['series_id'] = event['series']['id']
            eitem['series_description'] = event['series']['description']
            eitem['series_start_date'] = event['series']['start_date']


        eitem['group'] = event['group']["name"]
        eitem['group_url'] = event['group']["urlname"]
        eitem['group_geolocation'] = {
            "lat": event['group']['lat'],
            "lon": event['group']['lon'],
        }

        if 'group' in event:
            group = event['group']
            copy_fields = ["id", "created", "join_mode", "name", "url_name",
                           "who", "topics", "members"]
            for f in copy_fields:
                if f in group:
                    eitem["group_"+f] = group[f]
                else:
                    eitem[f] = None

            eitem['group_geolocation'] = {
                "lat": group['lat'],
                "lon": group['lon'],
            }


        created = unixtime_to_datetime(event['created']/1000).isoformat()
        eitem['type'] = "meetup"
        eitem.update(self.get_grimoire_fields(created, eitem['type']))

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        return eitem
