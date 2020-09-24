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
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#   Quan Zhou <quan@bitergia.com>
#

import logging

from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)

from ..elastic_mapping import Mapping as BaseMapping
from .enrich import Enrich, metadata
from .utils import get_time_diff_days


logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns: dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
               "main_description_analyzed": {
                    "type": "text",
                    "index": true
               },
               "summary_analyzed": {
                    "type": "text",
                    "index": true
               }
           }
        }"""

        return {"items": mapping}


class BugzillaEnrich(Enrich):

    mapping = Mapping
    roles = ['assigned_to', 'reporter', 'qa_contact']

    def get_field_author(self):
        return "reporter"

    def get_sh_identity(self, item, identity_field=None):
        """ Return a Sorting Hat identity using bugzilla user data """

        def fill_list_identity(identity, user_list_data):
            """ Fill identity with user data in first item in list """
            identity['username'] = user_list_data[0]['__text__']
            if '@' in identity['username']:
                identity['email'] = identity['username']
            if 'name' in user_list_data[0]:
                identity['name'] = user_list_data[0]['name']
            return identity

        identity = {}
        for field in ['name', 'email', 'username']:
            # Basic fields in Sorting Hat
            identity[field] = None

        user = item  # by default a specific user dict is used
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]

        identity = fill_list_identity(identity, user)

        return identity

    def get_project_repository(self, eitem):
        return eitem['origin']

    def get_identities(self, item):
        """Return the identities from an item"""

        for rol in self.roles:
            if rol in item['data']:
                user = self.get_sh_identity(item["data"][rol])
                yield user

        if 'activity' in item["data"]:
            for event in item["data"]['activity']:
                event_user = [{"__text__": event['Who']}]
                user = self.get_sh_identity(event_user)
                yield user

        if 'long_desc' in item["data"]:
            for comment in item["data"]['long_desc']:
                user = self.get_sh_identity(comment['who'])
                yield user

    @metadata
    def get_rich_item(self, item):

        if 'bug_id' not in item['data']:
            logger.warning("[bugzilla] Dropped bug without bug_id {}".format(item))
            return None

        eitem = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)

        # The real data
        issue = item['data']

        # dizquierdo specification

        eitem['changes'] = len(item['data']['activity'])

        eitem['labels'] = item['data']['keywords']
        eitem['priority'] = item['data']['priority'][0]['__text__']
        eitem['severity'] = item['data']['bug_severity'][0]['__text__']
        eitem['op_sys'] = item['data']['op_sys'][0]['__text__']
        eitem['product'] = item['data']['product'][0]['__text__']
        eitem['component'] = item['data']['component'][0]['__text__']
        eitem['platform'] = item['data']['rep_platform'][0]['__text__']
        if '__text__' in item['data']['status_whiteboard'][0]:
            eitem['whiteboard'] = item['data']['status_whiteboard'][0]['__text__']
        if '__text__' in item['data']['resolution'][0]:
            eitem['resolution'] = item['data']['resolution'][0]['__text__']
        if 'watchers' in item['data']:
            eitem['watchers'] = item['data']['watchers'][0]['__text__']
        if 'votes' in item['data']:
            eitem['votes'] = item['data']['votes'][0]['__text__']

        if "assigned_to" in issue:
            if "name" in issue["assigned_to"][0]:
                eitem["assigned"] = issue["assigned_to"][0]["name"]

        if "reporter" in issue:
            if "name" in issue["reporter"][0]:
                eitem["reporter_name"] = issue["reporter"][0]["name"]
                eitem["author_name"] = issue["reporter"][0]["name"]

        date_ts = str_to_datetime(issue['creation_ts'][0]['__text__'])
        eitem['creation_date'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')

        eitem["bug_id"] = issue['bug_id'][0]['__text__']
        eitem["status"] = issue['bug_status'][0]['__text__']
        if "short_desc" in issue:
            if "__text__" in issue["short_desc"][0]:
                eitem["main_description"] = issue['short_desc'][0]['__text__'][:self.KEYWORD_MAX_LENGTH]
                eitem["main_description_analyzed"] = issue['short_desc'][0]['__text__']
        if "summary" in issue:
            if "__text__" in issue["summary"][0]:
                eitem["summary"] = issue['summary'][0]['__text__'][:self.KEYWORD_MAX_LENGTH]
                eitem["summary_analyzed"] = issue['summary'][0]['__text__']

        # Fix dates
        date_ts = str_to_datetime(issue['delta_ts'][0]['__text__'])
        eitem['changeddate_date'] = date_ts.isoformat()
        eitem['delta_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')

        # Add extra JSON fields used in Kibana (enriched fields)
        eitem['comments'] = 0
        eitem['url'] = None

        if 'long_desc' in issue:
            eitem['comments'] = len(issue['long_desc'])
        eitem['url'] = item['origin'] + "/show_bug.cgi?id=" + issue['bug_id'][0]['__text__']
        eitem['resolution_days'] = \
            get_time_diff_days(eitem['creation_date'], eitem['delta_ts'])
        eitem['timeopen_days'] = \
            get_time_diff_days(eitem['creation_date'], datetime_utcnow().replace(tzinfo=None))

        if self.sortinghat:
            eitem.update(self.get_item_sh(item, self.roles))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(eitem['creation_date'], "bug"))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem
