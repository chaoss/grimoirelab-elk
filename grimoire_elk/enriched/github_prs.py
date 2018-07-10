#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# GithubPRs to Elastic class helper
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
#   Pranjal Aswani <aswani.pranjal@gmail.com>

import logging
import re

from datetime import datetime

from .utils import get_time_diff_days

from .enrich import metadata
from .github import GitHubEnrich
from ..elastic_mapping import Mapping as BaseMapping


GEOLOCATION_INDEX = '/github/'
GITHUB = 'https://github.com/'
logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        geopoints type is not created in dynamic mapping

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
               "merged_by_geolocation": {
                   "type": "geo_point"
               },
               "user_geolocation": {
                   "type": "geo_point"
               },
               "title_analyzed": {
                 "type": "text"
               }
            }
        }
        """

        return {"items": mapping}


class GitHubPRsEnrich(GitHubEnrich):

    mapping = Mapping

    # how to add 'requested_reviewers_data'?
    roles = ['merged_by_data', 'user_data']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []
        self.studies.append(self.enrich_onion)

        self.users = {}  # cache users
        self.location = {}  # cache users location
        self.location_not_found = []  # location not found in map api

    def get_fields_uuid(self):
        return ["merged_by_uuid", "user_uuid"]

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        item = item['data']

        for identity in ['user', 'merged_by']:
            if item[identity]:
                # In user_data we have the full user data
                user = self.get_sh_identity(item[identity + "_data"])
                if user:
                    identities.append(user)
        return identities

    @metadata
    def get_rich_item(self, item):
        rich_pr = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_pr[f] = item[f]
            else:
                rich_pr[f] = None
        # The real data
        pull_request = item['data']

        rich_pr['time_to_close_days'] = \
            get_time_diff_days(pull_request['created_at'], pull_request['closed_at'])

        if pull_request['state'] != 'closed':
            rich_pr['time_open_days'] = \
                get_time_diff_days(pull_request['created_at'], datetime.utcnow())
        else:
            rich_pr['time_open_days'] = rich_pr['time_to_close_days']

        rich_pr['user_login'] = pull_request['user']['login']
        user = pull_request['user_data']

        if user is not None and user:
            rich_pr['user_name'] = user['name']
            rich_pr['author_name'] = user['name']
            if user['email']:
                rich_pr["user_domain"] = self.get_email_domain(user['email'])
            rich_pr['user_org'] = user['company']
            rich_pr['user_location'] = user['location']
            rich_pr['user_geolocation'] = self.get_geo_point(user['location'])
        else:
            rich_pr['user_name'] = None
            rich_pr["user_domain"] = None
            rich_pr['user_org'] = None
            rich_pr['user_location'] = None
            rich_pr['user_geolocation'] = None
            rich_pr['author_name'] = None

        merged_by = None

        if pull_request['merged_by'] is not None:
            merged_by = pull_request['merged_by_data']
            rich_pr['merged_by_login'] = pull_request['merged_by']['login']
            rich_pr['merged_by_name'] = merged_by['name']
            if merged_by['email']:
                rich_pr["merged_by_domain"] = self.get_email_domain(merged_by['email'])
            rich_pr['merged_by_org'] = merged_by['company']
            rich_pr['merged_by_location'] = merged_by['location']
            rich_pr['merged_by_geolocation'] = \
                self.get_geo_point(merged_by['location'])
        else:
            rich_pr['merged_by_name'] = None
            rich_pr['merged_by_login'] = None
            rich_pr["merged_by_domain"] = None
            rich_pr['merged_by_org'] = None
            rich_pr['merged_by_location'] = None
            rich_pr['merged_by_geolocation'] = None

        rich_pr['id'] = pull_request['id']
        rich_pr['id_in_repo'] = pull_request['html_url'].split("/")[-1]
        rich_pr['repository'] = pull_request['html_url'].rsplit("/", 2)[0]
        rich_pr['title'] = pull_request['title']
        rich_pr['title_analyzed'] = pull_request['title']
        rich_pr['state'] = pull_request['state']
        rich_pr['created_at'] = pull_request['created_at']
        rich_pr['updated_at'] = pull_request['updated_at']
        rich_pr['closed_at'] = pull_request['closed_at']
        rich_pr['merged_at'] = pull_request['merged_at']
        rich_pr['url'] = pull_request['html_url']
        labels = ''
        if 'labels' in pull_request:
            for label in pull_request['labels']:
                labels += label['name'] + ";;"
        if labels != '':
            labels[:-2]
        rich_pr['labels'] = labels

        rich_pr['item_type'] = 'pull request'

        rich_pr['github_repo'] = rich_pr['repository'].replace(GITHUB, '')
        rich_pr['github_repo'] = re.sub('.git$', '', rich_pr['github_repo'])
        rich_pr["url_id"] = rich_pr['github_repo'] + "/pull/" + rich_pr['id_in_repo']

        rich_pr['code_merge_duration'] = \
            get_time_diff_days(rich_pr['created_at'], rich_pr['merged_at'])

        if self.prjs_map:
            rich_pr.update(self.get_item_project(rich_pr))

        if 'project' in item:
            rich_pr['project'] = item['project']

        rich_pr.update(self.get_grimoire_fields(pull_request['created_at'], "pull_request"))

        if self.sortinghat:
            item[self.get_field_date()] = rich_pr[self.get_field_date()]
            rich_pr.update(self.get_item_sh(item, self.roles))

        return rich_pr

    def enrich_items(self, items):
        total = super(GitHubEnrich, self).enrich_items(items)

        logger.debug("Updating GitHub users geolocations in Elastic")
        self.geo_locations_to_es()  # Update geolocations in Elastic

        return total

    def enrich_onion(self, ocean_backend, enrich_backend,
                     no_incremental=False,
                     in_index_iss='github_issues_onion-src',
                     in_index_prs='github_prs_onion-src',
                     out_index_iss='github_issues_onion-enriched',
                     out_index_prs='github_prs_onion-enriched',
                     data_source_iss='github-issues',
                     data_source_prs='github-prs',
                     contribs_field='uuid',
                     timeframe_field='grimoire_creation_date',
                     sort_on_field='metadata__timestamp'):

        super().enrich_onion(enrich_backend=enrich_backend,
                             in_index=in_index_iss,
                             out_index=out_index_iss,
                             data_source=data_source_iss,
                             contribs_field=contribs_field,
                             timeframe_field=timeframe_field,
                             sort_on_field=sort_on_field,
                             no_incremental=no_incremental)

        super().enrich_onion(enrich_backend=enrich_backend,
                             in_index=in_index_prs,
                             out_index=out_index_prs,
                             data_source=data_source_prs,
                             contribs_field=contribs_field,
                             timeframe_field=timeframe_field,
                             sort_on_field=sort_on_field,
                             no_incremental=no_incremental)
