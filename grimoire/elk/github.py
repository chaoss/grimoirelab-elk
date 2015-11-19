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
import requests
from perceval.utils import get_time_diff_days


class GitHubElastic(object):

    def __init__(self, elastic, github):
        self.elastic = elastic
        self.github = github
        self.index_github = "github"
        self.geolocations = self.geo_locations_from_es()


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

        else:
            url = 'https://maps.googleapis.com/maps/api/geocode/json'
            params = {'sensor': 'false', 'address': location}
            r = requests.get(url, params=params)

            try:
                print (location)
                r_json = r.json()
                geo_code = r_json['results'][0]['geometry']['location']
            except:
                logging.info("Can't find geocode for " + location)

            if geo_code:
                geo_point = {
                    "lat": geo_code['lat'],
                    "lon": geo_code['lng']
                }
                self.geolocations[location] = geo_point


        return geo_point


    def geo_locations_from_es(self):

        return self.elastic.getGitHubCache("geolocations", "location")

    def geo_locations_to_es(self):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        elastic_type = "geolocations"
        url = self.elastic.index_url+'/' + elastic_type + '/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))


        for loc in self.geolocations:
            if current >= max_items:
                requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            geopoint = self.geolocations[loc]
            location = geopoint.copy()
            location["location"] = loc
            # First upload the raw pullrequest data to ES
            data_json = json.dumps(location)
            # Don't include in URL non ascii codes
            safe_loc = str(loc.encode('ascii', 'ignore'),'ascii')
            geo_id = str("%s-%s-%s" % (location["lat"], location["lon"],
                                       safe_loc))
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (geo_id)
            bulk_json += data_json +"\n"  # Bulk document
            current += 1

        requests.put(url, data = bulk_json)

        logging.debug("Adding items to ES Done")

    def users_to_es(self):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        elastic_type = "users"  # github global users
        url = self.elastic.index_url+'/' + elastic_type + '/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))

        users = self.github.users

        for login in users:
            if current >= max_items:
                requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0
            data_json = json.dumps(users[login])
            user_id = str(users[login]["id"])
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (user_id)
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        requests.put(url, data = bulk_json)

    def users_from_es(self):

        return self.elastic.getGitHubCache("users", "login")


    @classmethod
    def get_elastic_mappings(cls):
        """ geopoints type is not created in dynamic mapping """

        elastic_mappings = {}

        _types = ['pullrequests','issues_pullrequests']


        mapping = """
            {
                "properties": {
                   "assignee_geolocation": {
                       "type": "geo_point"
                   },
                   "user_geolocation": {
                       "type": "geo_point"
                   }
                }
            }
        """

        for _type in _types:
            elastic_mappings[_type] = mapping

        return elastic_mappings

    def get_rich_pull(self, pull):
        rich_pull = {}
        rich_pull['id'] = pull['id']
        rich_pull['time_to_close_days'] = \
            get_time_diff_days(pull['created_at'], pull['closed_at'])

        user_login = pull['user']['login']

        if user_login in self.github.users:
            user = GitHubUser(self.github.users[user_login])
        else:
            logging.debug("User login %s not found in github users" % (user_login))
            user = None

        rich_pull['user_login'] = user_login

        if user is not None:
            rich_pull['user_name'] = user.name
            rich_pull['user_email'] = user.email
            rich_pull['user_org'] = user.org
            rich_pull['user_location'] = user.location
            rich_pull['user_geolocation'] = self.get_geo_point(user.location)
        else:
            rich_pull['user_name'] = None
            rich_pull['user_email'] = None
            rich_pull['user_org'] = None
            rich_pull['user_location'] = None
            rich_pull['user_geolocation'] = None

        if pull['assignee'] is not None:
            assignee_login = pull['assignee']['login']
            assignee = GitHubUser(self.github.users[assignee_login])
            rich_pull['assignee_login'] = assignee_login
            rich_pull['assignee_name'] = assignee.name
            rich_pull['assignee_email'] = assignee.email
            rich_pull['assignee_org'] = assignee.org
            rich_pull['assignee_location'] = assignee.location
            rich_pull['assignee_geolocation'] = \
                self.get_geo_point(assignee.location)
        else:
            rich_pull['assignee_name'] = None
            rich_pull['assignee_login'] = None
            rich_pull['assignee_email'] = None
            rich_pull['assignee_org'] = None
            rich_pull['assignee_location'] = None
            rich_pull['assignee_geolocation'] = None

        rich_pull['title'] = pull['title']
        rich_pull['state'] = pull['state']
        rich_pull['created_at'] = pull['created_at']
        rich_pull['updated_at'] = pull['updated_at']
        rich_pull['closed_at'] = pull['closed_at']
        rich_pull['url'] = pull['html_url']
        labels = ''
        if 'labels' in pull:
            for label in pull['labels']:
                labels += label['name']+";;"
        if labels != '':
            labels[:-2]
        rich_pull['labels'] = labels

        return rich_pull


    def pullrequests_to_es(self, pulls, _type = "issues_pullrequests"):

        logging.debug("Updating Github users in Elastic")
        self.users_to_es()  # update users in Elastic
        logging.debug("Updating geolocations in Elastic")
        self.geo_locations_to_es() # Update geolocations in Elastic

        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        elastic_type = _type
        url = self.elastic.index_url+'/' + elastic_type + '/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))

        for pull in pulls:
            if not 'head' in pull.keys() and not 'pull_request' in pull.keys():
                # And issue that it is not a PR
                continue

            if current >= max_items:
                requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            rich_pull = self.get_rich_pull(pull)
            data_json = json.dumps(rich_pull)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (rich_pull["id"])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        requests.put(url, data = bulk_json)


class GitHubUser(object):
    ''' Helper class to manage data from a Github user '''

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
