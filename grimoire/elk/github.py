#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Github to Elastic class helper
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
import re

from datetime import datetime

from dateutil import parser

from .utils import get_time_diff_days

from .enrich import Enrich, metadata

GITHUB = 'https://github.com/'

class GitHubEnrich(Enrich):

    roles = ['assignee_data', 'user_data']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)
        self.users = {}  # cache users
        self.location = {}  # cache users location
        self.location_not_found = []  # location not found in map api

    def set_elastic(self, elastic):
        self.elastic = elastic
        # Recover cache data from Elastic
        self.geolocations = self.geo_locations_from_es()

    def get_field_author(self):
        return "user_data"

    def get_fields_uuid(self):
        return ["assignee_uuid", "user_uuid"]

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        item = item['data']

        for identity in ['user', 'assignee']:
            if item[identity]:
                # In user_data we have the full user data
                user = self.get_sh_identity(item[identity+"_data"])
                if user:
                    identities.append(user)
        return identities

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
            user = item['data'][identity_field]

        if not user:
            return identity

        identity['username'] = user['login']
        identity['email'] = None
        identity['name'] = None
        if 'email' in user:
            identity['email'] = user['email']
        if 'name' in user:
            identity['name'] = user['name']
        return identity

    def get_geo_point(self, location):
        geo_point = geo_code = None

        if location is None:
            return geo_point

        if location in self.geolocations:
            geo_location = self.geolocations[location]
            geo_point = {
                    "lat": geo_location['lat'],
                    "lon": geo_location['lon']
            }

        elif location in self.location_not_found:
            # Don't call the API.
            pass

        else:
            url = 'https://maps.googleapis.com/maps/api/geocode/json'
            params = {'sensor': 'false', 'address': location}
            r = self.requests.get(url, params=params)

            try:
                logging.debug("Using Maps API to find %s" % (location))
                r_json = r.json()
                geo_code = r_json['results'][0]['geometry']['location']
            except:
                if location not in self.location_not_found:
                    logging.debug("Can't find geocode for " + location)
                    self.location_not_found.append(location)

            if geo_code:
                geo_point = {
                    "lat": geo_code['lat'],
                    "lon": geo_code['lng']
                }
                self.geolocations[location] = geo_point


        return geo_point


    def get_github_cache(self, kind, _key):
        """ Get cache data for items of _type using _key as the cache dict key """

        cache = {}
        res_size = 100  # best size?
        _from = 0

        index_github = "github/" + kind

        url = self.elastic.url + "/"+index_github
        url += "/_search" + "?" + "size=%i" % res_size
        r = self.requests.get(url)
        type_items = r.json()

        if 'hits' not in type_items:
            logging.info("No github %s data in ES" % (kind))

        else:
            while len(type_items['hits']['hits']) > 0:
                for hit in type_items['hits']['hits']:
                    item = hit['_source']
                    cache[item[_key]] = item
                _from += res_size
                r = self.requests.get(url+"&from=%i" % _from)
                type_items = r.json()
                if 'hits' not in type_items:
                    break

        return cache


    def geo_locations_from_es(self):
        return self.get_github_cache("geolocations", "location")

    def geo_locations_to_es(self):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        url = self.elastic.url + "/github/geolocations/_bulk"

        logging.debug("Adding geoloc to %s (in %i packs)" % (url, max_items))


        for loc in self.geolocations:
            if current >= max_items:
                self.requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            geopoint = self.geolocations[loc]
            location = geopoint.copy()
            location["location"] = loc
            # First upload the raw issue data to ES
            data_json = json.dumps(location)
            # Don't include in URL non ascii codes
            safe_loc = str(loc.encode('ascii', 'ignore'),'ascii')
            geo_id = str("%s-%s-%s" % (location["lat"], location["lon"],
                                       safe_loc))
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (geo_id)
            bulk_json += data_json +"\n"  # Bulk document
            current += 1

        self.requests.put(url, data = bulk_json)

        logging.debug("Adding geoloc to ES Done")


    def get_elastic_mappings(self):
        """ geopoints type is not created in dynamic mapping """

        mapping = """
        {
            "properties": {
               "assignee_geolocation": {
                   "type": "geo_point"
               },
               "user_geolocation": {
                   "type": "geo_point"
               },
               "title_analyzed": {
                 "type": "string",
                 "index":"analyzed"
               }
            }
        }
        """

        return {"items":mapping}

    def get_field_unique_id(self):
        return "ocean-unique-id"

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        return repo

    @metadata
    def get_rich_item(self, item):
        rich_issue = {}

        # metadata fields to copy
        copy_fields = ["metadata__updated_on","metadata__timestamp","ocean-unique-id","origin"]
        for f in copy_fields:
            if f in item:
                rich_issue[f] = item[f]
            else:
                rich_issue[f] = None
        # The real data
        issue = item['data']

        rich_issue['time_to_close_days'] = \
            get_time_diff_days(issue['created_at'], issue['closed_at'])

        if issue['state'] != 'closed':
            rich_issue['time_open_days'] = \
                get_time_diff_days(issue['created_at'], datetime.utcnow())
        else:
            rich_issue['time_open_days'] = rich_issue['time_to_close_days']

        rich_issue['user_login'] = issue['user']['login']
        user = issue['user_data']

        if user is not None:
            rich_issue['user_name'] = user['name']
            rich_issue['author_name'] = user['name']
            rich_issue['user_email'] = user['email']
            if rich_issue['user_email']:
                rich_issue["user_domain"] = self.get_email_domain(rich_issue['user_email'])
            rich_issue['user_org'] = user['company']
            rich_issue['user_location'] = user['location']
            rich_issue['user_geolocation'] = self.get_geo_point(user['location'])
        else:
            rich_issue['user_name'] = None
            rich_issue['user_email'] = None
            rich_issue["user_domain"] = None
            rich_issue['user_org'] = None
            rich_issue['user_location'] = None
            rich_issue['user_geolocation'] = None
            rich_issue['author_name'] = None


        assignee = None

        if issue['assignee'] is not None:
            assignee = issue['assignee_data']
            rich_issue['assignee_login'] = issue['assignee']['login']
            rich_issue['assignee_name'] = assignee['name']
            rich_issue['assignee_email'] = assignee['email']
            if rich_issue['assignee_email']:
                rich_issue["assignee_domain"] = self.get_email_domain(rich_issue['assignee_email'])
            rich_issue['assignee_org'] = assignee['company']
            rich_issue['assignee_location'] = assignee['location']
            rich_issue['assignee_geolocation'] = \
                self.get_geo_point(assignee['location'])
        else:
            rich_issue['assignee_name'] = None
            rich_issue['assignee_login'] = None
            rich_issue['assignee_email'] = None
            rich_issue["assignee_domain"] = None
            rich_issue['assignee_org'] = None
            rich_issue['assignee_location'] = None
            rich_issue['assignee_geolocation'] = None

        rich_issue['id'] = issue['id']
        rich_issue['id_in_repo'] = issue['html_url'].split("/")[-1]
        rich_issue['title'] = issue['title']
        rich_issue['title_analyzed'] = issue['title']
        rich_issue['state'] = issue['state']
        rich_issue['created_at'] = issue['created_at']
        rich_issue['updated_at'] = issue['updated_at']
        rich_issue['closed_at'] = issue['closed_at']
        rich_issue['url'] = issue['html_url']
        labels = ''
        if 'labels' in issue:
            for label in issue['labels']:
                labels += label['name']+";;"
        if labels != '':
            labels[:-2]
        rich_issue['labels'] = labels
        rich_issue['repository'] = rich_issue['origin']

        rich_issue['pull_request'] = True
        rich_issue['item_type'] = 'pull request'
        if not 'head' in issue.keys() and not 'pull_request' in issue.keys():
            rich_issue['pull_request'] = False
            rich_issue['item_type'] = 'issue'

        rich_issue['github_repo'] = item['origin'].replace(GITHUB,'')
        rich_issue['github_repo'] = re.sub('.git$', '', rich_issue['github_repo'])
        rich_issue["url_id"] = rich_issue['github_repo']+"/issues/"+rich_issue['id_in_repo']

        if self.prjs_map:
            rich_issue.update(self.get_item_project(rich_issue))

        if self.sortinghat:
            rich_issue.update(self.get_item_sh(item, self.roles))

        rich_issue.update(self.get_grimoire_fields(issue['created_at'], "issue"))


        return rich_issue

    def enrich_items(self, items):
        total = super(GitHubEnrich, self).enrich_items(items)

        logging.debug("Updating GitHub users geolocations in Elastic")
        self.geo_locations_to_es() # Update geolocations in Elastic

        return total


class GitHubUser(object):
    """ Helper class to manage data from a Github user """

    users = {}  # cache with users from github

    def __init__(self, user):

        self.login = user['login']
        self.email = user['email']
        if 'company' in user:
            self.company = user['company']
        self.orgs = user['orgs']
        self.org = self._getOrg()
        self.name = user['name']
        self.location = user['location']


    def _getOrg(self):
        company = None

        if self.company:
            company = self.company

        if company is None:
            company = ''
            # Return the list of orgs
            for org in self.orgs:
                company += org['login'] +";;"
            company = company[:-2]

        return company
