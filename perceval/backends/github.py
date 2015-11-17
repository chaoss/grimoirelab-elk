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

import json
import logging
import os
import requests

from perceval.backends.backend import Backend
from perceval.utils import get_eta, remove_last_char_from_file

class GitHub(Backend):

    _name = "github"
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
        parser.add_argument("-e", "--elastic_host",  default = "127.0.0.1",
                            help = "Host with Elastic Search" + \
                            "(default: 127.0.0.1)")
        parser.add_argument("--elastic_port",  default = "9200",
                            help = "Elastic Search port " + \
                            "(default: 9200)")


    def __init__(self, owner, repository, auth_token,
                 use_cache = False, history = False):

        self.owner = owner
        self.repository = repository
        self.auth_token = auth_token
        self.pull_requests = []  # All pull requests from github repo
        self.cache = {}  # cache for pull requests
        self.use_cache = use_cache
        self.use_history = history
        self.url = self._get_url()

        super(GitHub, self).__init__(use_cache, history)

    def _get_url(self):
        github_per_page = 30  # 100 in other items. 20 for pull requests. 30 issues
        github_api = "https://api.github.com"
        github_api_repos = github_api + "/repos"
        url_repo = github_api_repos + "/" + self.owner +"/" + self.repository

        url_pulls = url_repo + "/pulls"
        url_issues = url_repo + "/issues"

        url_params = "?per_page=" + str(github_per_page)
        url_params += "&state=all"  # open and close pull requests
        url_params += "&sort=updated"  # sort by last updated
        url_params += "&direction=asc"  # first older pull request

        # prs_count = getPullRequests(url_pulls+url_params)
        url = url_issues + url_params

        return url


    def _restore(self):
        '''Restore JSON full data from storage '''

        restore_dir = self._get_storage_dir()

        if os.path.isdir(restore_dir):
            try:
                logging.debug("Restoring data from %s" % restore_dir)
                restore_file = os.path.join(restore_dir, "pull_requests.json")
                if os.path.isfile(restore_file):
                    with open(restore_file) as f:
                        data = f.read()
                        self.issues = json.loads(data)
                logging.debug("Restore completed")
            except ValueError:
                logging.warning("Restore failed. Wrong dump files in: %s" %
                                restore_file)


    def _dump(self):
        ''' Dump JSON full data to storage '''

        dump_dir = self._get_storage_dir()

        logging.debug("Dumping data to  %s" % dump_dir)
        dump_file = os.path.join(dump_dir, "pull_requests.json")
        with open(dump_file, "w") as f:
            f.write(json.dumps(self.pull_requests))
        logging.debug("Dump completed")


    def fetch(self):
        ''' Returns an iterator for the data gathered '''

        return self.getIssuesPullRequests()

    def _get_name(self):

        return GitHub._name


    def get_id(self):

        _id = "_%s_%s" % (self.owner, self.repository)

        return _id.lower()

    def _load_cache(self):
        ''' Load all cache files in memory '''

        fname = os.path.join(self._get_storage_dir(),
                             "cache_pull_requests.json")
        with open(fname,"r") as f:
            self.cache['pull_requests'] = json.loads(f.read())


    def _clean_cache(self):
        cache_files = ["cache_pull_requests.json"]

        for name in cache_files:
            fname = os.path.join(self._get_storage_dir(), name)
            with open(fname,"w") as f:
                f.write("[")

        cache_keys = ['pull_requests']

        for _id in cache_keys:
            self.cache[_id] = []

    def _close_cache(self):
        cache_file = os.path.join(self._get_storage_dir(),
                                  "cache_pull_requests.json")

        remove_last_char_from_file(cache_file)
        with open(cache_file,"a") as f:
                f.write("]")


    def getLastUpdateFromES(self, _type):

        last_update = self.elastic.get_last_date(_type, 'updated_at')

        return last_update

    def _pull_requests_to_cache(self, pull_requests):
        ''' Append to pull request JSON cache '''

        cache_file = os.path.join(self._get_storage_dir(),
                                  "cache_pull_requests.json")

        with open(cache_file, "a") as cache:

            data_json = json.dumps(pull_requests)
            data_json = data_json[1:-1]  # remove []
            data_json += "," # join between arrays
            # We need to add the array to an already existing array
            cache.write(data_json)


    def getUser(self, url, login):

        if login not in GitHub.users:

            url = url + "/users/" + self.login

            r = requests.get(url, verify=False,
                             headers={'Authorization':'token ' + self.auth_token})
            user = r.json()

            GitHub.users[self.login] = user

            # Get the public organizations also
            url += "/orgs"
            r = requests.get(url, verify=False,
                             headers={'Authorization':'token ' + self.auth_token})
            orgs = r.json()

            GitHub.users[self.login]['orgs'] = orgs


    def getPullRequests(self, url):
        url_next = url
        prs_count = 0
        last_page = None
        page = 1

        url_next += "&page="+str(page)

        while url_next:
            logging.info("Get issues pulls requests from " + url_next)
            r = requests.get(url_next, verify=False,
                             headers={'Authorization':'token ' +
                                      self.auth_token})
            pulls = r.json()
            self.pull_requests += pulls
            self._dump()
            self._pull_requests_to_cache (pulls)
            prs_count += len(pulls)

            logging.info(r.headers['X-RateLimit-Remaining'])

            url_next = None
            if 'next' in r.links:
                url_next = r.links['next']['url']  # Loving requests :)

            if not last_page:
                last_page = r.links['last']['url'].split('&page=')[1].split('&')[0]

            logging.info("Page: %i/%s" % (page, last_page))

            page += 1

        self._close_cache()

        return self

    def _find_pull_requests(self, issues):

        pulls = []

        for issue in issues:
            if not 'head' in issue.keys() and not 'pull_request' in issue.keys():
            # An issue that it is not a PR
                continue
            pulls.append(issue)

        return pulls


    def getIssuesPullRequests(self):
        _type = "issues_pullrequests"
        last_page = page = 1

        if self.use_cache:
            self.pull_requests =  self.cache['pull_requests']

        else:
            # last_update = self.getLastUpdateFromES(_type)
            last_update = None  # broken order in github API
            if last_update is not None:
                logging.info("Getting issues since: " + last_update)
                self.url += "&since="+last_update
            url_next = self.url

            while url_next:
                logging.info("Get issues pulls requests from " + url_next)
                r = requests.get(url_next, verify=False,
                                 headers={'Authorization':'token ' + self.auth_token})
                issues = r.json()

                pulls = self._find_pull_requests(issues)

                self.pull_requests += pulls
                self._dump()
                self._pull_requests_to_cache(pulls)

                logging.info(r.headers['X-RateLimit-Remaining'])

                url_next = None
                if 'next' in r.links:
                    url_next = r.links['next']['url']  # Loving requests :)

                if last_page == 1:
                    if 'last' in r.links:
                        last_page = r.links['last']['url'].split('&page=')[1].split('&')[0]

                logging.info("Page: %i/%s" % (page, last_page))

                page += 1

            self._close_cache()

        return self

    # Iterator
    def __iter__(self):

        self.iter = 0
        return self

    def __next__(self):

        if self.iter == len(self.pull_requests):
            raise StopIteration
        item = self.pull_requests[self.iter]

        self.iter += 1

        return item

