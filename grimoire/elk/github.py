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
from .utils import get_time_diff_days

from grimoire.elk.enrich import Enrich


class GitHubEnrich(Enrich):

    def __init__(self, github, sortinghat=True, db_projects_map = None):
        super().__init__(sortinghat, db_projects_map)

        self.elastic = None
        self.perceval_backend = github
        self.index_github = "github"
        self.users = {}  # cache users
        self.location = {}  # cache users location
        self.location_not_found = []  # location not found in map api

    def set_elastic(self, elastic):
        self.elastic = elastic
        # Recover cache data from Elastic
        self.geolocations = self.geo_locations_from_es()

    def get_field_date(self):
        return "updated_at"

    def get_fields_uuid(self):
        return ["assignee_uuid", "user_uuid"]

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        for identity in ['user', 'assignee']:
            if item[identity]:
                # In user_data we have the full user data
                user = self.get_sh_identity(item[identity+"_data"])
                identities.append(user)
        return identities

    def get_sh_identity(self, user):
        identity = user
        identity['username'] = identity['login']
        return identity

    def get_item_sh(self, item):
        """ Add sorting hat enrichment fields """
        eitem = {}  # Item enriched

        user = item['user_data']
        if user is not None:
            identity = self.get_sh_identity(user)
            eitem["user_uuid"] = \
                self.get_uuid(identity, self.get_connector_name())
            eitem["author_uuid"] = eitem["user_uuid"]
        else:
            eitem['user_uuid'] = None
            eitem['author_uuid'] = None

        assignee = item['assignee_data']
        if assignee:
            identity = self.get_sh_identity(assignee)
            eitem["assignee_uuid"] =  \
                self.get_uuid(identity, self.get_connector_name())
        else:
            eitem["assignee_uuid"] = None

        return eitem


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
            r = requests.get(url, params=params)

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
        r = requests.get(url)
        type_items = r.json()

        if 'hits' not in type_items:
            logging.info("No github %s data in ES" % (kind))

        else:
            while len(type_items['hits']['hits']) > 0:
                for hit in type_items['hits']['hits']:
                    item = hit['_source']
                    cache[item[_key]] = item
                _from += res_size
                r = requests.get(url+"&from=%i" % _from)
                type_items = r.json()

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
                   "assignee_name": {
                      "type": "string",
                      "index":"not_analyzed"
                   },
                   "user_name": {
                      "type": "string",
                      "index":"not_analyzed"
                   },
                   "author_name": {
                      "type": "string",
                      "index":"not_analyzed"
                   },
                   "repository": {
                      "type": "string",
                      "index":"not_analyzed"
                   }
                }
            }
        """

        return {"items":mapping}

    def get_field_unique_id(self):
        return "ocean-unique-id"

    def get_rich_pull(self, item):
        rich_pull = {}

        # metadata fields to copy
        copy_fields = ["metadata__updated_on","ocean-unique-id","origin"]
        for f in copy_fields:
            if f in item:
                rich_pull[f] = item[f]
            else:
                rich_pull[f] = None
        # The real data
        pull = item['data']

        rich_pull['time_to_close_days'] = \
            get_time_diff_days(pull['created_at'], pull['closed_at'])

        rich_pull['user_login'] = pull['user']['login']
        user = pull['user_data']

        if user is not None:
            rich_pull['user_name'] = user['name']
            rich_pull['author_name'] = user['name']
            rich_pull['user_email'] = user['email']
            if rich_pull['user_email']:
                rich_pull["user_domain"] = rich_pull['user_email'].split("@")[1]
            rich_pull['user_org'] = user['company']
            rich_pull['user_location'] = user['location']
            rich_pull['user_geolocation'] = self.get_geo_point(user['location'])
        else:
            rich_pull['user_name'] = None
            rich_pull['user_email'] = None
            rich_pull["user_domain"] = None
            rich_pull['user_org'] = None
            rich_pull['user_location'] = None
            rich_pull['user_geolocation'] = None
            rich_pull['author_name'] = None


        assignee = None

        if pull['assignee'] is not None:
            assignee = pull['assignee_data']
            rich_pull['assignee_login'] = pull['assignee']['login']
            rich_pull['assignee_name'] = assignee['name']
            rich_pull['assignee_email'] = assignee['email']
            if rich_pull['assignee_email']:
                rich_pull["assignee_domain"] = rich_pull['assignee_email'].split("@")[1]
            rich_pull['assignee_org'] = assignee['company']
            rich_pull['assignee_location'] = assignee['location']
            rich_pull['assignee_geolocation'] = \
                self.get_geo_point(assignee['location'])
        else:
            rich_pull['assignee_name'] = None
            rich_pull['assignee_login'] = None
            rich_pull['assignee_email'] = None
            rich_pull["assignee_domain"] = None
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
        rich_pull['repository'] = rich_pull['origin']

        if self.sortinghat:
            rich_pull.update(self.get_item_sh(pull))

        return rich_pull

    def enrich_items(self, pulls):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        url = self.elastic.index_url+'/items/_bulk'

        logging.debug("Adding items to %s (in %i packs)" % (url, max_items))

        for pull in pulls:
            if not 'head' in pull.keys() and not 'pull_request' in pull.keys():
                # And issue that it is not a PR
                # continue
                pass

            if current >= max_items:
                requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            rich_pull = self.get_rich_pull(pull)
            data_json = json.dumps(rich_pull)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (rich_pull[self.get_field_unique_id()])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        requests.put(url, data = bulk_json)

        logging.debug("Updating GitHub users geolocations in Elastic")
        self.geo_locations_to_es() # Update geolocations in Elastic


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
