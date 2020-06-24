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
import re

from .qmenrich import QMEnrich

from perceval.backend import uuid
from grimoirelab_toolkit.datetime import str_to_datetime

MAX_SIZE_BULK_ENRICHED_ITEMS = 200

URL_PATTERN = "^(https|git)(://|@)([^/:]+)[/:]([^/:]+)/(.+).git$"

logger = logging.getLogger(__name__)


class GitQMEnrich(QMEnrich):

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
        return re.findall(URL_PATTERN, item['origin'])[0][-1]

    def extract_commit_metric(self, item):
        commit = item['data']

        commit_date = self.normalized_date(commit['CommitDate'])
        files = commit['files']

        added = 0
        removed = 0
        actions = 0
        files_changed = []

        for file in files:
            added += int(file['added']) if file['added'].isdigit() else 0
            removed += int(file['removed']) if file['removed'].isdigit() else 0
            actions += 1 if file['action'] else 0
            files_changed.append(file['file'])

        if 'project' in self.date_items.keys():

            if commit_date in self.date_items['data']['commit'].keys():
                self.date_items['data']['commit'][commit_date] += 1
                self.date_items['data']['lines_added'][commit_date] += added
                self.date_items['data']['lines_removed'][commit_date] += removed
                self.date_items['data']['actions'][commit_date] += actions
                self.date_items['data']['files_changed'][commit_date] += len(files_changed)
            else:
                self.date_items['data']['commit'][commit_date] = 1
                self.date_items['data']['lines_added'][commit_date] = added
                self.date_items['data']['lines_removed'][commit_date] = removed
                self.date_items['data']['actions'][commit_date] = actions
                self.date_items['data']['files_changed'][commit_date] = len(files_changed)

        else:
            project = self.extract_project(item)

            committed = {commit_date: 1}
            lines_added = {commit_date: added}
            lines_removed = {commit_date: removed}
            actions = {commit_date: actions}
            files_changed = {commit_date: len(files_changed)}

            self.date_items = {
                "project": project,
                "data": {
                    "commit": committed,
                    "lines_added": lines_added,
                    "lines_removed": lines_removed,
                    "actions": actions,
                    "files_changed": files_changed
                }
            }

    def update_metric_items(self, category):
        eitem = {
            "metric_es_compute": 'sample',
            "metric_type": 'LineChart'
        }

        edict = {}

        if category == 'commit':
            edict = {
                "metric_class": 'commits',
                "metric_id": 'commits.numberCommitsCreated',
                "metric_desc": 'The number of commits created on a current date.',
                "metric_name": 'Number of Commits'
            }
        elif category == 'lines_added':
            edict = {
                "metric_class": 'commits',
                "metric_id": 'commits.numberLinesAdded',
                "metric_desc": 'The number of lines added on a current date.',
                "metric_name": 'Number of Lines Added'
            }
        elif category == 'lines_removed':
            edict = {
                "metric_class": 'commits',
                "metric_id": 'commits.numberLinesRemoved',
                "metric_desc": 'The number of lines removed created on a current date.',
                "metric_name": 'Number of Lines Removed'
            }
        elif category == 'actions':
            edict = {
                "metric_class": 'commits',
                "metric_id": 'commits.numberActions',
                "metric_desc": 'The number of actions on a current date.',
                "metric_name": 'Number of Actions'
            }
        elif category == 'files_changed':
            edict = {
                "metric_class": 'commits',
                "metric_id": 'merges.numberFilesChanged',
                "metric_desc": 'The number of files changes on a current date.',
                "metric_name": 'Number of Files Changed'
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
            self.extract_commit_metric(item)

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
            logger.error("%s/%s missing items for Git QM", str(missing), str(num_items))
        else:
            logger.info("%s items inserted for Git QM", str(num_items))

        return num_items
