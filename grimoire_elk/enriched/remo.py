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

import logging

from .enrich import Enrich, metadata
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
                "description_analyzed": {
                  "type": "text",
                  "index": true
                },
                "geolocation": {
                    "type": "geo_point"
                }
           }
        } """

        return {"items": mapping}


class ReMoEnrich(Enrich):

    mapping = Mapping

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)
        self.author = "user"  # changes if we are generating events

    def get_field_author(self):
        return self.author

    def get_identities(self, item):
        """Return the identities from an item"""

        item = item['data']
        if 'owner' in item:
            owner = self.get_sh_identity(item['owner'])
            yield owner
        if 'user' in item:
            user = self.get_sh_identity(item['user'])
            yield user
        if 'mentor' in item:
            mentor = self.get_sh_identity(item['mentor'])
            yield mentor

    def get_sh_identity(self, item, identity_field=None):
        # "owner": {
        #             "first_name": "Huda",
        #             "last_name": "Sarfraz",
        #             "_url": "https://reps.mozilla.org/api/beta/users/959/",
        #             "display_name": "huda_sarfraz"
        #          },
        identity = {'username': None, 'email': None, 'name': None}

        if not item:
            return identity

        user = item
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]

        identity['username'] = user["display_name"]
        identity['email'] = None
        identity['name'] = user["first_name"] + " " + user["last_name"]

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
            logger.error("[remo] Can not detect category in item {}".format(item))

        return category

    @metadata
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

        for f in self.RAW_FIELDS_COPY + ["offset"]:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem

    def __get_rich_item_activities(self, item):
        eitem = {}

        # The real data
        activity = item['data']

        # data fields to copy
        copy_fields = ["activity", "activity_description", "external_link",
                       "functional_areas", "initiative", "link", "location",
                       "remo_url", "report_date"]
        for f in copy_fields:
            if f in activity:
                eitem[f] = activity[f]
            else:
                eitem[f] = None

        eitem['user_profile_url'] = activity['user']['_url']
        eitem['user'] = activity['user']['first_name'] + " " + activity['user']['last_name']

        if 'mentor' in activity and activity['mentor']:
            eitem['mentor_profile_url'] = activity['mentor']['_url']
            eitem['mentor'] = activity['mentor']['first_name'] + " " + activity['mentor']['last_name']

        # geolocation
        if -90 < int(activity['latitude']) < 90 and \
           -180 < int(activity['longitude']) < 180:
            eitem['geolocation'] = {
                "lat": activity['latitude'],
                "lon": activity['longitude'],
            }

        if self.sortinghat:
            self.author = "user"
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(activity["report_date"], "activity"))

        eitem["is_attendee"] = 0
        eitem["is_organizer"] = 0
        if eitem["activity"] == "Attended an Event":
            eitem["is_attendee"] = 1
        elif eitem["activity"] == "Organized an Event":
            eitem["is_organizer"] = 1

        return eitem

    def __get_rich_item_users(self, item):
        eitem = {}

        return eitem

    def __get_rich_item_events(self, item):
        eitem = {}

        # The real data
        event = item['data']

        # data fields to copy
        copy_fields = ["initiative", "categories", "city", "country",
                       "description", "estimated_attendance", "remo_url",
                       "external_link", "region", "timezone",
                       "planning_pad_url", "hashtag"]
        for f in copy_fields:
            if f in event:
                eitem[f] = event[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {
            "description": "description_analyzed",
            "end": "end_date",
            "start": "start_date",
            "name": "title"
        }
        for fn in map_fields:
            eitem[map_fields[fn]] = event[fn]

        eitem['owner_profile_url'] = None
        eitem['owner'] = None
        if 'owner' in event:
            eitem['owner_profile_url'] = event['owner']['_url']
            eitem['owner'] = event['owner']['display_name']

        # geolocation
        if (-90 < int(event['lat']) < 90 and
            -180 < int(event['lon']) < 180):
            eitem['geolocation'] = {
                "lat": event['lat'],
                "lon": event['lon'],
            }

        if self.sortinghat:
            self.author = 'owner'
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(event["start"], "event"))

        return eitem
