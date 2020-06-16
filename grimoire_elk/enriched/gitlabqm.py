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

from datetime import datetime
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

        self.users = {}  # cache users

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
        dt = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ")
        return str_to_datetime(str(dt.date())).isoformat()

    def filter_items(self, items):
        for item in items:
            created_at = self.normalized_date(item['data']['created_at'])

            if created_at in DATE_ITEMS.keys():
                DATE_ITEMS[created_at] += 1
            else:
                DATE_ITEMS[created_at] = 1

    def add_project(self, item):
        return {"project": item['search_fields']['project']}

    def add_extra_data(self):
        eitem = {'metric_class': "issues",
                 'metric_type': "LineChart",
                 'metric_es_compute': "sample",
                 'metric_id': "issues.numberOpenIssues",
                 'metric_desc': "The number of issues opened on a current date.",
                 'metric_name': "Number of Open Issues"
                 }

        return eitem

    def enrich_issue(self, item):

        eitem = {}
        issue = item['data']

        dt = self.normalized_date(issue['created_at'])

        if dt in DATE_ITEMS.keys():
            eitem['category'] = "issue"
            eitem['state'] = issue['state']
            eitem['datetime'] = dt
            eitem['metric_es_value'] = DATE_ITEMS[dt]
            eitem['metric_es_value_weighted'] = DATE_ITEMS[dt]
            eitem.update(self.add_project(item))
            eitem.update(self.add_extra_data())
            eitem['uuid'] = uuid(eitem['metric_id'], eitem['project'], eitem['datetime'])
            eitem['id'] = '{}_issue_{}'.format(eitem['state'], eitem['uuid'])

            DATE_ITEMS.pop(dt)

        return eitem

    def enrich_items(self, ocean_backend):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        self.filter_items(ocean_backend.fetch())

        for item in ocean_backend.fetch():

            eitem = {}

            if item['category'] == 'issue':
                eitem = self.enrich_issue(item)

            # elif item['category'] == 'merge_request':
            #     rich_item = self.enrich_merge(item)
            # else:
            #     logger.error("[gitlab] rich item not defined for GitLab category {}".format(
            #                  item['category']))

            if eitem is not None and eitem:
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
            logger.error("%s/%s missing items for GitLab QM", str(missing), str(num_items))
        else:
            logger.info("%s items inserted for GitLab QM", str(num_items))

        return num_items
