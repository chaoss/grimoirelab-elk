# -*- coding: utf-8 -*-
#
# BugzillaREST to Elastic class helper
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

from datetime import datetime
from dateutil import parser

from .enrich import Enrich, metadata, DEFAULT_PROJECT
from .utils import get_time_diff_days


logger = logging.getLogger(__name__)


class BugzillaRESTEnrich(Enrich):

    roles = ['assigned_to_detail', 'qa_contact_detail', 'creator_detail']

    def get_field_author(self):
        return 'creator_detail'

    def get_fields_uuid(self):
        return ["assigned_to_uuid", "creator_uuid"]

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        for rol in self.roles:
            if rol in item['data']:
                identities.append(self.get_sh_identity(item["data"][rol]))
        return identities

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is used
        if 'data' in item and type(item) == dict:
            user = item['data'][identity_field]

        identity['username'] = user['name'].split("@")[0]
        identity['email'] = user['email']
        identity['name'] = user['real_name']
        return identity

    def get_item_project(self, eitem):
        """ Get project mapping enrichment field.

        Bugzillarest mapping is pretty special so it needs a special
        implementacion.
        """

        project = None
        ds_name = self.get_connector_name()  # data source name in projects map

        url = eitem['origin']
        component = eitem['component'].replace(" ", "+")
        product = eitem['product'].replace(" ", "+")

        repo_comp = url + "/buglist.cgi?product=" + product + "&component=" + component
        repo_comp_prod = url + "/buglist.cgi?component=" + component + "&product=" + product
        repo_product = url + "/buglist.cgi?product=" + product

        for repo in [repo_comp, repo_comp_prod, repo_product, url]:
            if repo in self.prjs_map[ds_name]:
                project = self.prjs_map[ds_name][repo]
                break

        if project is None:
            project = DEFAULT_PROJECT

        eitem_project = {"project": project}

        eitem_project.update(self.add_project_levels(project))

        return eitem_project

    @metadata
    def get_rich_item(self, item):

        if 'id' not in item['data']:
            logger.warning("Dropped bug without bug_id %s" % (item))
            return None

        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None

        # The real data
        issue = item['data']

        if "assigned_to_detail" in issue and "real_name" in issue["assigned_to_detail"]:
            eitem["assigned_to"] = issue["assigned_to_detail"]["real_name"]

        if "creator_detail" in issue and "real_name" in issue["creator_detail"]:
            eitem["creator"] = issue["creator_detail"]["real_name"]

        eitem["id"] = issue['id']
        eitem["status"] = issue['status']
        if "summary" in issue:
            eitem["summary"] = issue['summary'][:self.KEYWORD_MAX_SIZE]
            # Share the name field with bugzilla and share the panel
            eitem["main_description"] = eitem["summary"][:self.KEYWORD_MAX_SIZE]
        # Component and product
        eitem["component"] = issue['component']
        eitem["product"] = issue['product']

        # Fix dates
        date_ts = parser.parse(issue['creation_time'])
        eitem['creation_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')
        date_ts = parser.parse(issue['last_change_time'])
        eitem['changeddate_date'] = date_ts.isoformat()
        eitem['delta_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')

        # Add extra JSON fields used in Kibana (enriched fields)
        eitem['comments'] = 0
        eitem['number_of_comments'] = 0
        eitem['time_to_last_update_days'] = None
        eitem['url'] = None

        if 'long_desc' in issue:
            eitem['number_of_comments'] = len(issue['long_desc'])
        if 'comments' in issue:
            eitem['comments'] = len(issue['comments'])
        eitem['url'] = item['origin'] + "/show_bug.cgi?id=" + str(issue['id'])
        eitem['time_to_last_update_days'] = \
            get_time_diff_days(eitem['creation_ts'], eitem['delta_ts'])
        eitem['timeopen_days'] = \
            get_time_diff_days(eitem['creation_ts'], datetime.utcnow())

        eitem['changes'] = 0
        for history in issue['history']:
            if 'changes' in history:
                eitem['changes'] += len(history['changes'])

        if self.sortinghat:
            eitem.update(self.get_item_sh(item, self.roles))
            # To reuse the name of the fields in Bugzilla and share the panel
            eitem['assigned_to_org_name'] = eitem['assigned_to_detail_org_name']
            eitem['assigned_to_uuid'] = eitem['assigned_to_detail_uuid']

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(issue['creation_time'], "bugrest"))

        return eitem
