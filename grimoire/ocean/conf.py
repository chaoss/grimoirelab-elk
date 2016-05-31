#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Ocean Configuration Manager
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

'''Ocean Configuration Manager (singleton) '''

import json
import logging
import requests

class ConfOcean(object):

    conf_index = "conf"
    conf_repos = conf_index+"/repos"
    elastic = None
    requests_session = requests.Session()

    # Support working with https insecure
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    requests_session.verify = False


    @classmethod
    def get_index(cls):
        return cls.conf_index

    @classmethod
    def set_elastic(cls, elastic):
        cls.elastic = elastic

        # Check conf index
        url = elastic.url + "/" + cls.conf_index
        r = cls.requests_session.get(url)
        if r.status_code != 200:
            cls.requests_session.post(url)
            logging.info("Creating OceanConf index " + url)


    @classmethod
    def add_repo(cls, unique_id, repo):
        ''' Add a new perceval repository with its arguments '''

        if cls.elastic is None:
            logging.error("Can't add repo to conf. Ocean elastic is not configured")
            return

        url = cls.elastic.url + "/" + cls.conf_repos + "/"
        url += cls.elastic.safe_index(unique_id)

        logging.debug("Adding repo to Ocean %s %s" % (url, repo))

        cls.requests_session.post(url, data = json.dumps(repo))

    @classmethod
    def get_repos(cls):
        ''' List of repos data in Ocean '''

        repos = []

        if cls.elastic is None:
            logging.error("Can't get repos. Ocean elastic is not configured")
            return

        # TODO: use scrolling API for getting all repos
        url = cls.elastic.url + "/" + cls.conf_repos + "/_search?size=9999"

        r = cls.requests_session.get(url).json()

        if 'hits' in r:

            repos_raw = r['hits']['hits']  # Already existing items

            [ repos.append(rep['_source']) for rep in repos_raw ]

        return repos

    @classmethod
    def get_repos_ids(cls):
        ''' Lists of repos elastic ids in Ocean '''

        repos_ids = []

        if cls.elastic is None:
            logging.error("Can't get repos. Ocean elastic is not configured")
            return

        url = cls.elastic.url + "/" + cls.conf_repos + "/_search"

        r = cls.requests_session.get(url).json()

        if 'hits' in r:
            repos_raw = r['hits']['hits']  # Already existing items
            [ repos_ids.append(rep['_id']) for rep in repos_raw ]

        return repos_ids
