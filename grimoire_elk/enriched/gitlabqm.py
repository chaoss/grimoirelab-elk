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

from perceval.backend import uuid
from grimoirelab_toolkit.datetime import str_to_datetime

MAX_SIZE_BULK_ENRICHED_ITEMS = 200
DATE_ITEMS = {}

logger = logging.getLogger(__name__)


class GitLabQMEnrich(QMEnrich):

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
            processed_dt = str_to_datetime(dt).replace(hour=0, minute=0, second=0, microsecond=0)
            processed_dt = processed_dt.isoformat()

        return processed_dt

    def extract_project(self, item):
        return item['search_fields']['project']

    def filter_items(self, items):

        for item in items:
            project = self.extract_project(item)
            issue = item['data']

            created_at = self.normalized_date(issue['created_at'])
            closed_at = self.normalized_date(issue['closed_at'])

            if project in self.date_items.keys():

                if created_at in self.date_items[project]['opened'].keys():
                    self.date_items[project]['opened'][created_at] += 1
                else:
                    self.date_items[project]['opened'][created_at] = 1

                if closed_at and closed_at is not None:
                    if closed_at in self.date_items[project]['closed'].keys():
                        self.date_items[project]['closed'][closed_at] += 1
                    else:
                        self.date_items[project]['closed'][closed_at] = 1

            else:
                opened = {}
                closed = {}

                opened[created_at] = 1
                if closed_at and closed_at is not None:
                    closed[closed_at] = 1

                self.date_items[project] = {"opened": opened, "closed": closed}

        logger.info("filtering done")

    def add_extra_data(self):
        eitem = {
            'metric_class': "issues",
            'metric_type': "LineChart",
            'metric_es_compute': "sample",
        }

        return eitem

    def enrich_opened_items(self, project):
        edates = []

        for dt in self.date_items[project]['opened'].keys():
            edate = {}

            edate.update(self.add_extra_data())
            edate['metric_id'] = "issues.numberOpenedIssues"
            edate['metric_desc'] = "The number of issues opened on a current date."
            edate['metric_name'] = "Number of Opened Issues"
            edate['project'] = project
            edate['datetime'] = dt
            edate['metric_es_value'] = self.date_items[project]['opened'][dt]
            edate['metric_es_value_weighted'] = self.date_items[project]['opened'][dt]
            edate['uuid'] = uuid(edate['metric_id'], edate['project'], edate['datetime'])
            edate['id'] = 'opened_issue_{}'.format(edate['uuid'])
            edate.update(self.get_grimoire_fields(dt, "date"))

            edates.append(edate)

        return edates

    def enrich_closed_items(self, project):
        edates = []

        for dt in self.date_items[project]['closed'].keys():
            edate = {}

            edate.update(self.add_extra_data())
            edate['metric_id'] = "issues.numberClosedIssues"
            edate['metric_desc'] = "The number of issues closed on a current date."
            edate['metric_name'] = "Number of Closed Issues"
            edate['project'] = project
            edate['datetime'] = dt
            edate['metric_es_value'] = self.date_items[project]['closed'][dt]
            edate['metric_es_value_weighted'] = self.date_items[project]['closed'][dt]
            edate['uuid'] = uuid(edate['metric_id'], edate['project'], edate['datetime'])
            edate['id'] = 'closed_issue_{}'.format(edate['uuid'])
            edate.update(self.get_grimoire_fields(dt, "date"))

            edates.append(edate)

        return edates

    def enrich_items(self, ocean_backend):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        self.filter_items(ocean_backend.fetch())

        for project in self.date_items.keys():
            eitems = []

            rich_items = self.enrich_opened_items(project)
            eitems.extend(rich_items)

            rich_items = self.enrich_closed_items(project)
            eitems.extend(rich_items)

            items_to_enrich.extend(eitems)

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
            logger.error("%s/%s missing items for GitLab QM", str(missing), str(num_items))
        else:
            logger.info("%s items inserted for GitLab QM", str(num_items))

        return num_items
