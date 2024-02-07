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
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import copy
import logging

from grimoirelab_toolkit.datetime import unixtime_to_datetime

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping


MAX_SIZE_BULK_ENRICHED_ITEMS = 200


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
                "comment": {
                    "type": "text",
                    "index": true
                },
                "venue_geolocation": {
                    "type": "geo_point"
                },
                "group_geolocation": {
                    "type": "geo_point"
                }
           }
        } """

        return {"items": mapping}


class MeetupEnrich(Enrich):

    mapping = Mapping

    def get_field_author(self):
        return "author"

    def get_identities(self, item):
        ''' Return the identities from an item '''

        item = item['data']

        # Creators
        if 'event_hosts' in item:
            user = self.get_sh_identity(item['event_hosts'][0])
            yield user

        # rsvps
        rsvps = item.get('rsvps', [])

        for rsvp in rsvps:
            user = self.get_sh_identity(rsvp['member'])
            yield user

        # Comments
        for comment in item['comments']:
            user = self.get_sh_identity(comment['member'])
            yield user

    def get_sh_identity(self, item, identity_field=None):
        identity = {'username': None, 'email': None, 'name': None}

        if not item:
            return identity

        user = item
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]

        identity['username'] = str(user["id"])
        identity['email'] = None
        identity['name'] = user["name"]

        return identity

    def get_project_repository(self, eitem):
        return eitem['tag']

    @metadata
    def get_rich_item(self, item):
        # We need to detect the category of item: activities (report), events or users
        eitem = {}

        if 'time' not in item['data']:
            logger.warning("[meetup] Not processing {}: no time field".format(item['uuid']))
            return eitem

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)

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
        if 'event_hosts' in event:
            host = event['event_hosts'][0]
            if 'photo' in host:
                eitem['member_photo_url'] = host['photo']['photo_link']
                eitem['member_photo_id'] = host['photo']['id']
                eitem['member_photo_type'] = host['photo']['type']
            eitem['member_is_host'] = True
            eitem['member_id'] = host['id']
            eitem['member_name'] = host['name']
            eitem['member_url'] = "https://www.meetup.com/members/" + str(host['id'])

        eitem['event_url'] = event['link']

        # data fields to copy with meetup`prefix
        copy_fields = ["description", "plain_text_description",
                       "name", "status", "utc_offset", "visibility",
                       "waitlist_count", "yes_rsvp_count", "duration",
                       "featured", "rsvpable"]
        copy_fields_time = ["time", "updated", "created"]

        for f in copy_fields:
            if f in event:
                eitem["meetup_" + f] = event[f]
            else:
                eitem[f] = None

        for f in copy_fields_time:
            if f in event:
                eitem["meetup_" + f] = unixtime_to_datetime(event[f] / 1000).isoformat()
            else:
                eitem[f] = None

        rsvps = event.get('rsvps', [])

        eitem['num_rsvps'] = len(rsvps)
        eitem['num_comments'] = len(event['comments'])

        try:
            if 'time' in event:
                eitem['time_date'] = unixtime_to_datetime(event['time'] / 1000).isoformat()
            else:
                logger.warning("[meetup] Time field nof found in event")
                return {}
        except ValueError:
            logger.warning("[meetup] Wrong datetime for {}: {}".format(eitem['url'], event['time']))
            # If no datetime for the enriched item, it is useless for Kibana
            return {}

        if 'venue' in event:
            venue = event['venue']
            copy_fields = ["id", "name", "city", "state", "zip", "country",
                           "localized_country_name", "repinned", "address_1"]
            for f in copy_fields:
                if f in venue:
                    eitem["venue_" + f] = venue[f]
                else:
                    eitem[f] = None

            eitem['venue_geolocation'] = None
            if 'lat' in event['venue'] and 'lon' in event['venue']:
                eitem['venue_geolocation'] = {
                    "lat": event['venue']['lat'],
                    "lon": event['venue']['lon']
                }

        if 'series' in event:
            eitem['series_id'] = event['series']['id']
            eitem['series_description'] = event['series']['description']
            eitem['series_start_date'] = event['series']['start_date']

        if 'group' in event:
            group = event['group']
            copy_fields = ["id", "created", "join_mode", "name", "urlname",
                           "who"]
            for f in copy_fields:
                if f in group:
                    eitem["group_" + f] = group[f]
                else:
                    eitem[f] = None

            eitem['group_geolocation'] = {
                "lat": group['lat'],
                "lon": group['lon'],
            }

            if eitem['group_created']:
                eitem['group_created'] = unixtime_to_datetime(eitem['group_created'] / 1000).isoformat()

            eitem['group_topics'] = []
            eitem['group_topics_keys'] = []
            if 'topics' in group:
                group_topics = [topic['name'] for topic in group['topics']]
                group_topics_keys = [topic['urlkey'] for topic in group['topics']]
                eitem['group_topics'] = group_topics
                eitem['group_topics_keys'] = group_topics_keys

        if len(rsvps) > 0:
            eitem['group_members'] = rsvps[0]['group']['members']

        created = unixtime_to_datetime(event['created'] / 1000).isoformat()
        eitem['type'] = "meetup"
        # time_date is when the meetup will take place, the needed one in this index
        # created is when the meetup entry was created and it is not the interesting date
        eitem.update(self.get_grimoire_fields(eitem['time_date'], eitem['type']))

        eitem.update(self.get_item_sh(event))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem

    def get_item_sh(self, item):
        """ Add sorting hat enrichment fields  """

        sh_fields = {}

        # Not shared common get_item_sh because it is pretty specific
        if 'member' in item:
            # comment and rsvp
            identity = self.get_sh_identity(item['member'])
        elif 'event_hosts' in item:
            # meetup event
            identity = self.get_sh_identity(item['event_hosts'][0])
        else:
            return sh_fields

        created = unixtime_to_datetime(item['created'] / 1000).isoformat()
        if self.sortinghat:
            sh_fields = self.get_item_sh_fields(identity, created)
        else:
            sh_fields = self.get_item_no_sh_fields(identity, 'author')

        return sh_fields

    def get_rich_item_comments(self, comments, eitem):
        for comment in comments:
            ecomment = copy.deepcopy(eitem)
            created = unixtime_to_datetime(comment['created'] / 1000).isoformat()
            ecomment['url'] = comment['link']
            ecomment['id'] = ecomment['id'] + '_comment_' + str(comment['id'])
            ecomment['comment'] = comment['comment']
            ecomment['like_count'] = comment['like_count']
            ecomment['type'] = 'comment'
            ecomment.update(self.get_grimoire_fields(created, ecomment['type']))
            ecomment.pop('is_meetup_meetup')
            # event host fields: author of the event
            member = comment['member']
            if 'photo' in member:
                ecomment['member_photo_url'] = member['photo']['photo_link']
                ecomment['member_photo_id'] = member['photo']['id']
                ecomment['member_photo_type'] = member['photo']['type']
            if 'event_context' in member:
                ecomment['member_is_host'] = member['event_context']['host']
            ecomment['member_id'] = member['id']
            ecomment['member_name'] = member['name']
            ecomment['member_url'] = "https://www.meetup.com/members/" + str(member['id'])

            ecomment.update(self.get_item_sh(comment))

            yield ecomment

    def get_rich_item_rsvps(self, rsvps, eitem):
        for rsvp in rsvps:
            ersvp = copy.deepcopy(eitem)
            ersvp['type'] = 'rsvp'
            created = unixtime_to_datetime(rsvp['created'] / 1000).isoformat()
            ersvp.update(self.get_grimoire_fields(created, ersvp['type']))
            ersvp.pop('is_meetup_meetup')
            # event host fields: author of the event
            member = rsvp['member']
            if 'photo' in member:
                ersvp['member_photo_url'] = member['photo']['photo_link']
                ersvp['member_photo_id'] = member['photo']['id']
                ersvp['member_photo_type'] = member['photo']['type']
            ersvp['member_is_host'] = member['event_context']['host']
            ersvp['member_id'] = member['id']
            ersvp['member_name'] = member['name']
            ersvp['member_url'] = "https://www.meetup.com/members/" + str(member['id'])

            ersvp['id'] = ersvp['id'] + '_rsvp_' + str(rsvp['event']['id']) + "_" + str(member['id'])
            ersvp['url'] = "https://www.meetup.com/members/" + str(member['id'])

            ersvp['rsvps_guests'] = rsvp['guests']
            ersvp['rsvps_updated'] = rsvp['updated']
            ersvp['rsvps_response'] = rsvp['response']

            ersvp.update(self.get_item_sh(rsvp))

            yield ersvp

    def get_field_unique_id(self):
        return "id"

    def enrich_items(self, ocean_backend):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        for item in ocean_backend.fetch():
            eitem = self.get_rich_item(item)

            if 'uuid' not in eitem:
                continue

            items_to_enrich.append(eitem)

            if 'comments' in item['data'] and 'id' in eitem:
                comments = item['data']['comments']
                rich_item_comments = self.get_rich_item_comments(comments, eitem)
                items_to_enrich.extend(rich_item_comments)

            if 'rsvps' in item['data'] and 'id' in eitem:
                rsvps = item['data']['rsvps']
                rich_item_rsvps = self.get_rich_item_rsvps(rsvps, eitem)
                items_to_enrich.extend(rich_item_rsvps)

            if len(items_to_enrich) < MAX_SIZE_BULK_ENRICHED_ITEMS:
                continue

            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())
            items_to_enrich = []

        if len(items_to_enrich) > 0:
            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())

        if num_items != ins_items:
            missing = num_items - ins_items
            logger.error("[meetup] {}/{} missing items".format(missing, num_items))
        else:
            logger.info("[meetup] {} items inserted".format(num_items))

        return num_items
