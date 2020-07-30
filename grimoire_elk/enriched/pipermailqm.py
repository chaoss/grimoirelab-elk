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
from grimoirelab_toolkit.datetime import str_to_datetime

MAX_SIZE_BULK_ENRICHED_ITEMS = 200

logger = logging.getLogger(__name__)


class PipermailQMEnrich(QMEnrich):

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.date_items = {}
        self.user_items = {}

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
            processed_dt = str_to_datetime(dt).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            processed_dt = processed_dt.isoformat()

        return processed_dt

    def extract_project(self, item):
        return item['origin'].split('/')[-2]

    def extract_mail_metric(self, item):
        email = item['data']

        email_date = self.normalized_date(email['Date'])

        sender = email['From']
        if email_date in self.user_items.keys():
            self.user_items[email_date].add(sender)
        else:
            self.user_items[email_date] = {sender}

        if 'project' in self.date_items.keys():

            if email_date in self.date_items['data']['email'].keys():
                self.date_items['data']['email'][email_date] += 1
            else:
                self.date_items['data']['email'][email_date] = 1

            if 'In-Reply-To' not in email:
                if email_date in self.date_items['data']['thread'].keys():
                    self.date_items['data']['thread'][email_date] += 1
                else:
                    self.date_items['data']['thread'][email_date] = 1
            else:
                if email_date in self.date_items['data']['reply'].keys():
                    self.date_items['data']['reply'][email_date] += 1
                else:
                    self.date_items['data']['reply'][email_date] = 1

        else:
            project = self.extract_project(item)

            email = {email_date: 1}

            thread = {}
            reply = {}

            if 'In-Reply-To' not in email:
                thread = {email_date: 1}
            else:
                reply = {email_date: 1}

            self.date_items = {
                "project": project,
                "data": {
                    "email": email,
                    "thread": thread,
                    "reply": reply,
                    "user": {}
                }
            }

    def update_metric_items(self, category):
        eitem = {
            "metric_es_compute": 'sample',
            "metric_type": 'LineChart'
        }

        edict = {}

        if category == 'email':
            edict = {
                "metric_class": 'emails',
                "metric_id": 'emails.numberEmails',
                "metric_desc": 'The number of emails sent on a current date.',
                "metric_name": 'Number of Emails'
            }
        elif category == 'thread':
            edict = {
                "metric_class": 'emails',
                "metric_id": 'emails.numberThreads',
                "metric_desc": 'The number of threads started on a current date.',
                "metric_name": 'Number of Threads'
            }
        elif category == 'reply':
            edict = {
                "metric_class": 'emails',
                "metric_id": 'emails.numberReplies',
                "metric_desc": 'The number of replies sent on a current date.',
                "metric_name": 'Number of Replies'
            }
        elif category == 'user':
            edict = {
                "metric_class": 'emails',
                "metric_id": 'emails.numberUsers',
                "metric_desc": 'The number of users sent a mail on a current date.',
                "metric_name": 'Number of Users'
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
            self.extract_mail_metric(item)

        for k, v in self.user_items.items():
            self.date_items['data']['user'][k] = len(v)

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
            logger.error("%s/%s missing items for Pipermail QM", str(missing), str(num_items))
        else:
            logger.info("%s items inserted for Pipermail QM", str(num_items))

        return num_items
