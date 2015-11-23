#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# GitHub Pull Requests for Elastic Search
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

'''GitHub backend for Perseval'''


import logging
import requests

from perceval.backends.backend import Backend

class GitHub(Backend):

    name = "github"
    users = {}

    @classmethod
    def add_params(cls, cmdline_parser):
        parser = cmdline_parser

        parser.add_argument("-o", "--owner", required = True,
                            help = "github owner")
        parser.add_argument("-r", "--repository", required = True,
                            help = "github repository")
        parser.add_argument("-t", "--token", required = True,
                            help = "github access token")



    def __init__(self, owner, repository, auth_token, use_cache = False):

        self.owner = owner
        self.repository = repository
        self.auth_token = auth_token
        self.url = self._get_url()

        super(GitHub, self).__init__(use_cache)

    def _get_url(self):
        github_per_page = 30  # 100 in other items. 20 for pull requests. 30 issues
        github_api = "https://api.github.com"
        github_api_repos = github_api + "/repos"
        url_repo = github_api_repos + "/" + self.owner +"/" + self.repository

        url_issues = url_repo + "/issues"

        url_params = "?per_page=" + str(github_per_page)
        url_params += "&state=all"  # open and close pull requests
        url_params += "&sort=updated"  # sort by last updated
        url_params += "&direction=asc"  # first older pull request

        url = url_issues + url_params

        return url


    def get_id(self):

        _id = "_%s_%s" % (self.owner, self.repository)

        return _id.lower()

    def get_field_unique_id(self):
        return "id"


    def _get_items(self):
        ''' Return the real item in iterations '''

        logging.info("Get issues pulls requests from " + self.url_next)
        r = requests.get(self.url_next, verify=False,
                         headers={'Authorization':'token ' + self.auth_token})
        issues = r.json()
        self.cache.items_to_cache(issues)

        logging.debug("Rate limit: %s" %
                      (r.headers['X-RateLimit-Remaining']))

        self.url_next = None
        if 'next' in r.links:
            self.url_next = r.links['next']['url']  # Loving requests :)

        if self.last_page == 1:
            if 'last' in r.links:
                self.last_page = r.links['last']['url'].split('&page=')[1].split('&')[0]
                self.last_page = int(self.last_page)

        logging.info("Page: %i/%i" % (self.page, self.last_page))

        self.page += 1

        return issues

    def __iter__(self):
        self.last_page = self.page = 1
        self.url_next = self.url

        self.items_pool = self._get_items()

        return self

    def __next__(self):
        if len(self.items_pool) == 0:
            if self.url_next:
                self.items_pool = self._get_items()
            else:
                raise StopIteration

        return self.items_pool.pop()

