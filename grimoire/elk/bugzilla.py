#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Bugzilla to Elastic class helper
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

import json
import logging

from datetime import datetime
from time import time
from urllib.parse import urlparse

from dateutil import parser

from .enrich import Enrich

from .utils import get_time_diff_days

class BugzillaEnrich(Enrich):

    def get_field_date(self):
        return "delta_ts"

    def get_fields_uuid(self):
        return ["assigned_to_uuid", "reporter_uuid"]

    def get_field_unique_id(self):
        return "ocean-unique-id"

    @classmethod
    def get_sh_identity(cls, user):
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
        if 'reporter' in user:
            identity = fill_list_identity(identity, user['reporter'])
        if 'assigned_to' in user:
            identity = fill_list_identity(identity, user['assigned_to'])
        if 'who' in user:
            identity = fill_list_identity(identity, user['who'])
        if 'Who' in user:
            identity['username'] = user['Who']
            if '@' in identity['username']:
                identity['email'] = identity['username']
        if 'qa_contact' in user:
            identity = fill_list_identity(identity, user['qa_contact'])
        if 'changed_by' in user:
            identity['name'] = user['changed_by']

        return identity

    def get_item_sh(self, item):
        """ Add sorting hat enrichment fields """
        eitem = {}  # Item enriched

        # Sorting Hat integration: reporter and assigned_to uuids
        if 'assigned_to' in item['data']:
            identity = BugzillaEnrich.get_sh_identity({'assigned_to':item["data"]['assigned_to']})
            eitem['assigned_to_uuid'] = self.get_uuid(identity, self.get_connector_name())
            eitem['assigned_to_name'] = identity['name']
            item_date = item['data'][self.get_field_date()][0]['__text__']
            item_date_dt = parser.parse(item_date)
            item_date_utc = (item_date_dt-item_date_dt.utcoffset()).replace(tzinfo=None)
            eitem["assigned_to_org_name"] = self.get_enrollment(eitem['assigned_to_uuid'], item_date_utc)
            eitem["assigned_to_domain"] = self.get_domain(identity)
            eitem["assigned_to_bot"] = self.is_bot(eitem['assigned_to_uuid'])
        if 'reporter' in item['data']:
            identity = BugzillaEnrich.get_sh_identity({'reporter':item["data"]['reporter']})
            eitem['reporter_uuid'] = self.get_uuid(identity, self.get_connector_name())
            eitem['reporter_name'] = identity['name']
            item_date = item['data'][self.get_field_date()][0]['__text__']
            item_date_dt = parser.parse(item_date)
            item_date_utc = (item_date_dt-item_date_dt.utcoffset()).replace(tzinfo=None)
            eitem["reporter_org_name"] = self.get_enrollment(eitem['reporter_uuid'], item_date_utc)
            eitem["reporter_domain"] = self.get_domain(identity)
            eitem["reporter_bot"] = self.is_bot(eitem['reporter_uuid'])

        # Unify fields name
        eitem["author_uuid"] = eitem["reporter_uuid"]
        eitem["author_name"] = eitem["reporter_name"]
        eitem["author_org_name"] = eitem["reporter_org_name"]
        eitem["author_domain"] = eitem["reporter_domain"]

        return eitem

    def get_project_repository(self, item):
        repo = item['origin']
        product = item['data']['product'][0]['__text__']
        repo += "buglist.cgi?product="+product
        return repo

    def get_identities(self, item):
        ''' Return the identities from an item '''

        identities = []

        if 'activity' in item["data"]:
            for event in item["data"]['activity']:
                identities.append(self.get_sh_identity(event))
        if 'long_desc' in item["data"]:
            for comment in item["data"]['long_desc']:
                identities.append(self.get_sh_identity(comment))
        elif 'assigned_to' in item["data"]:
            identities.append(self.get_sh_identity({'assigned_to':
                                                    item["data"]['assigned_to']}))
        elif 'reporter' in item["data"]:
            identities.append(self.get_sh_identity({'reporter':
                                                    item["data"]['reporter']}))
        elif 'qa_contact' in item["data"]:
            identities.append(self.get_sh_identity({'qa_contact':
                                                    item['qa_contact']}))
        return identities

    def get_rich_item(self, item):

        if 'bug_id' not in item['data']:
            logging.warning("Dropped bug without bug_id %s", item)
            return None

        eitem = {}

        # metadata fields to copy
        copy_fields = ["metadata__updated_on","metadata__timestamp","ocean-unique-id","origin"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None

        # The real data
        issue = item['data']

        # dizquierdo specification

        eitem['changes'] = len(item['data']['activity'])

        eitem['labels'] = item['data']['keywords']
        eitem['priority'] = item['data']['priority'][0]['__text__']
        eitem['severity'] = item['data']['bug_severity'][0]['__text__']
        eitem['op_sys'] = item['data']['op_sys'][0]['__text__']
        eitem['product'] = item['data']['product'][0]['__text__']
        eitem['project_name'] = item['data']['product'][0]['__text__']
        eitem['component'] = item['data']['component'][0]['__text__']
        eitem['platform'] = item['data']['rep_platform'][0]['__text__']
        if '__text__' in item['data']['resolution'][0]:
            eitem['resolution'] = item['data']['resolution'][0]['__text__']
        if 'watchers' in item['data']:
            eitem['watchers'] = item['data']['watchers'][0]['__text__']
        if 'votes' in item['data']:
            eitem['votes'] = item['data']['votes'][0]['__text__']

        if "assigned_to" in issue:
            if "name" in issue["assigned_to"][0]:
                eitem["assigned"] = issue["assigned_to"][0]["name"]
            if "__text__" in issue["assigned_to"][0]:
                eitem["assignee_email"] = issue["assigned_to"][0]["__text__"]


        if "reporter" in issue:
            if "name" in issue["reporter"][0]:
                eitem["reporter_name"] = issue["reporter"][0]["name"]
                eitem["author_name"] = issue["reporter"][0]["name"]
            if "__text__" in issue["reporter"][0]:
                eitem["reporter_email"] = issue["reporter"][0]["__text__"]
                eitem["author_email"] = issue["reporter"][0]["__text__"]

        date_ts = parser.parse(issue['creation_ts'][0]['__text__'])
        eitem['creation_date'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')


        eitem["bug_id"] = issue['bug_id'][0]['__text__']
        eitem["status"]  = issue['bug_status'][0]['__text__']
        if "short_desc" in issue:
            if "__text__" in issue["short_desc"][0]:
                eitem["main_description"]  = issue['short_desc'][0]['__text__']
        if "summary" in issue:
            if "__text__" in issue["summary"][0]:
                eitem["summary"]  = issue['summary'][0]['__text__']


        # Fix dates
        date_ts = parser.parse(issue['delta_ts'][0]['__text__'])
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
            get_time_diff_days(eitem['creation_date'], datetime.utcnow())

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(item))

        eitem.update(self.get_grimoire_fields(eitem['creation_date'],"bug"))

        return eitem
