# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2020 Bitergia
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
#   Venu Vardhan Reddy Tekula <venuvardhanreddytekula8@gmail.com>
#


import logging

from .qmenrich import QMEnrich

from datetime import timezone

from perceval.backend import uuid
from grimoirelab_toolkit.datetime import unixtime_to_datetime

MAX_SIZE_BULK_ENRICHED_ITEMS = 200

logger = logging.getLogger(__name__)


class MeetupQMEnrich(QMEnrich):

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.date_items = {}

        self.studies = []

    def get_identities(self, item):
        """Return the identities from an item"""
        identities = []

        return identities

    def has_identities(self):
        """ Return whether the enriched items contains identities """

        return False

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_unique_id(self):
        return "id"

    def normalized_date(self, dt):

        processed_dt = None

        if dt and dt is not None:
            processed_dt = unixtime_to_datetime(dt / 1000).replace(hour=0, minute=0, second=0, microsecond=0,
                                                                   tzinfo=timezone.utc)
            processed_dt = processed_dt.isoformat()

        return processed_dt

    def extract_project(self, item):
        return item['search_fields']['group_name']

    def extract_meetup_metric(self, item):
        event = item['data']

        if 'project' in self.date_items.keys():

            rsvps = event['rsvps']
            for rsvp in rsvps:
                rsvp_created = self.normalized_date(rsvp['created'])

                if 'rsvp' in self.date_items['data'].keys():

                    if rsvp_created in self.date_items['data']['rsvp'].keys():
                        self.date_items['data']['rsvp'][rsvp_created] += 1
                    else:
                        self.date_items['data']['rsvp'][rsvp_created] = 1

                else:
                    rsvp = {rsvp_created: 1}
                    self.date_items['data']['rsvp'] = rsvp

            comments = event['comments']
            for comment in comments:
                comment_created = self.normalized_date(comment['created'])

                if 'comment' in self.date_items['data'].keys():

                    if comment_created in self.date_items['data']['comment'].keys():
                        self.date_items['data']['comment'][comment_created] += 1
                    else:
                        self.date_items['data']['comment'][comment_created] = 1

                else:
                    comment = {comment_created: 1}
                    self.date_items['data']['comment'] = comment

            event_date = self.normalized_date(event['time'])
            self.date_items['data']['yes_rsvp_count'][event_date] = event['yes_rsvp_count']

        else:
            project = self.extract_project(item)
            self.date_items = {
                "project": project,
                "data": {
                    'yes_rsvp_count': {}
                }
            }

    def update_metric_items(self, category):
        eitem = {
            "metric_es_compute": 'sample',
            "metric_type": 'LineChart'
        }

        edict = {}

        if category == 'rsvp':
            edict = {
                "metric_class": 'events',
                "metric_id": 'events.numberRSVPs',
                "metric_desc": 'The number of rsvps sent on a current date.',
                "metric_name": 'Number of RSVPs'
            }
        elif category == 'comment':
            edict = {
                "metric_class": 'events',
                "metric_id": 'events.numberComments',
                "metric_desc": 'The number of comments on a current date.',
                "metric_name": 'Number of Threads'
            }
        elif category == 'yes_rsvp_count':
            edict = {
                "metric_class": 'events',
                "metric_id": 'events.countYesRSVP',
                "metric_desc": 'The count of yes rsvp of an event.',
                "metric_name": 'Count of Yes RSVP'
            }

        eitem.update(edict)

        return eitem

    def get_rich_item(self, dt, category):
        edate = {}

        edate.update(self.update_metric_items(category))

        edate['project'] = self.date_items['project']
        edate['datetime'] = dt
        edate['metric_es_value'] = self.date_items['data'][category][dt]
        edate['metric_es_value_weighted'] = self.date_items['data'][category][dt]
        edate['uuid'] = uuid(edate['metric_id'], edate['project'], edate['datetime'])
        edate['id'] = '{}_{}'.format(category, edate['uuid'])

        edate.update(self.get_grimoire_fields(dt, "date"))

        return edate

    def enrich_items(self, ocean_backend):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        for item in ocean_backend.fetch():
            self.extract_meetup_metric(item)

        for category in self.date_items['data'].keys():
            for dt in self.date_items['data'][category].keys():
                eitem = self.get_rich_item(dt, category)

                items_to_enrich.append(eitem)

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
            logger.error("%s/%s missing items for Meetup QM", str(missing), str(num_items))
        else:
            logger.info("%s items inserted for Meetup QM", str(num_items))

        return num_items
