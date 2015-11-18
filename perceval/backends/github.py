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

from datetime import datetime
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

        Backend.add_params(cmdline_parser)



    def __init__(self, owner, repository, auth_token,
                 use_cache = False, incremental = True):

        self.owner = owner
        self.repository = repository
        self.auth_token = auth_token
        self.pull_requests = []  # All pull requests from github repo
        self.cache = {}  # cache for pull requests
        self.url = self._get_url()

        super(GitHub, self).__init__(use_cache, incremental)

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

        url = url_issues + url_params

        return url


    def _restore_state(self):
        '''Restore JSON full data from storage '''

        pass  # Last state now stored in ES


    def _dump_state(self):
        ''' Dump JSON full data to storage '''

        pass  # Last state dumped to ES


    def fetch(self):
        ''' Returns an iterator for the data gathered '''

        return self.getIssuesPullRequests()

    def _get_name(self):

        return GitHub._name


    def get_id(self):

        _id = "_%s_%s" % (self.owner, self.repository)

        return _id.lower()

    def _get_field_unique_id(self):
        return "id"


    def _clean_cache(self):
        filelist = [ f for f in os.listdir(self._get_storage_dir()) if
                    f.startswith("cache_pull_request_") ]
        for f in filelist:
            os.remove(os.path.join(self._get_storage_dir(), f))

    def _close_cache(self):

        pass  # nothing is needed in github


    def getLastUpdateFromES(self, _type):

        last_update = self.elastic.get_last_date(_type, 'updated_at')

        return last_update

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


    def _find_pull_requests(self, issues):

        pulls = []

        for issue in issues:
            if not 'head' in issue.keys() and not 'pull_request' in issue.keys():
            # An issue that it is not a PR
                continue
            pulls.append(issue)

        return pulls

    def _pull_requests_to_cache(self, pull_requests):
        ''' Update pull requests cache files  '''

        for pull in pull_requests:

            data_json = json.dumps(pull)
            cache_file = os.path.join(self._get_storage_dir(),
                          "cache_pull_request_%s.json" % (pull['id']))

            with open(cache_file, "w") as cache:
                cache.write(data_json)


    def _get_pull_requests_from_cache(self):
        logging.info("Reading issues from cache")
        # Just read all issues cache files
        filelist = [ f for f in os.listdir(self._get_storage_dir()) if
                    f.startswith("cache_pull_request_") ]
        logging.debug("Total pull requests in cache: %i" % (len(filelist)))
        for f in filelist:
            fname = os.path.join(self._get_storage_dir(), f)
            with open(fname,"r") as f:
                issue = json.loads(f.read())
                self.pull_requests.append(issue)
        logging.info("Cache read completed")
        return self


    def getIssuesPullRequests(self):

        if self.use_cache:
            return self._get_pull_requests_from_cache()

        _type = "issues_pullrequests"
        last_page = page = 1

        last_update = self.getLastUpdateFromES(_type)
        if last_update is not None:

            logging.info("Github issues API broken for incremental analysis")
            self.incremental = False

            if self.incremental:
                logging.info("Getting issues since: " + last_update)
                self.url += "&since="+last_update

        url_next = self.url

        while url_next:
            task_init = datetime.now()

            logging.info("Get issues pulls requests from " + url_next)
            r = requests.get(url_next, verify=False,
                             headers={'Authorization':'token ' + self.auth_token})
            issues = r.json()

            pulls = self._find_pull_requests(issues)

            self.pull_requests += pulls
            self._items_state_to_es(pulls)
            self._pull_requests_to_cache(pulls)

            logging.debug("Rate limit: %s" %
                          (r.headers['X-RateLimit-Remaining']))

            url_next = None
            if 'next' in r.links:
                url_next = r.links['next']['url']  # Loving requests :)

            if last_page == 1:
                if 'last' in r.links:
                    last_page = r.links['last']['url'].split('&page=')[1].split('&')[0]
                    last_page = int(last_page)

            logging.info("Page: %i/%i" % (page, last_page))

            task_time = (datetime.now() - task_init).total_seconds()
            eta_time = task_time * (last_page - page )
            eta_min = eta_time / 60.0

            logging.info("Completed %i/%i (ETA: %.2f min)" \
                         % (page, last_page, eta_min))

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

