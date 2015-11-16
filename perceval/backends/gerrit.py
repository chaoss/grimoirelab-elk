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

'''GitHub backend for Gerrit'''

import json
import logging
import os
import re
import subprocess
from perceval.utils import get_eta, remove_last_char_from_file

from perceval.backends.backend import Backend


class Gerrit(Backend):

    def __init__(self, user, repository, nreviews,
                 use_cache = False, history = False):

        self.gerrit_user = user
        self.project = repository
        self.nreviews = nreviews
        self.reviews = []  # All reviews from gerrit
        self.projects = []  # All projects from gerrit
        self.cache = {}  # cache for pull requests
        self.use_cache = use_cache
        self.use_history = history
        self.url = self._get_url()

        self.gerrit_cmd  = "ssh -p 29418 %s@%s" % (user, repository)
        self.gerrit_cmd += " gerrit "



        # Create storage dir if it not exists
        dump_dir = self._get_storage_dir()
        if not os.path.isdir(dump_dir):
            os.makedirs(dump_dir)

        if self.use_cache:
            # Don't use history data. Will be generated from cache.
            self.use_history = False

        if self.use_history:
            self._restore()  # Load history

        else:
            if self.use_cache:
                logging.info("Getting all data from cache")
                try:
                    self._load_cache()
                except:
                    # If any error loading the cache, clean it
                    logging.debug("Cache corrupted")
                    self.use_cache = False
                    self._clean_cache()
            else:
                self._clean_cache()  # Cache will be refreshed


    def _load_cache(self):
        ''' Load all cache files in memory '''

        fname = os.path.join(self._get_storage_dir(),
                             "cache_reviews.json")
        with open(fname,"r") as f:
            self.cache['pull_requests'] = json.loads(f.read())


    def _clean_cache(self):
        cache_files = ["cache_reviews.json"]

        for name in cache_files:
            fname = os.path.join(self._get_storage_dir(), name)
            with open(fname,"w") as f:
                f.write("[")

        cache_keys = ['reviews']

        for _id in cache_keys:
            self.cache[_id] = []

    def _close_cache(self):
        cache_file = os.path.join(self._get_storage_dir(),
                                  "cache_pull_requests.json")

        remove_last_char_from_file(cache_file)
        with open(cache_file,"a") as f:
                f.write("]")

    def _projects_to_cache(self, pull_requests):
        ''' Append to projects JSON cache '''

        cache_file = os.path.join(self._get_storage_dir(),
                                  "projects.json")

        with open(cache_file, "a") as cache:

            data_json = json.dumps(pull_requests)
            data_json = data_json[1:-1]  # remove []
            data_json += "," # join between arrays
            # We need to add the array to an already existing array
            cache.write(data_json)

    def _project_reviews_to_cache(self, project, reviews):
        ''' Append to reviews JSON cache '''

        cache_file = os.path.join(self._get_storage_dir(),
                                  "%s-reviews.json" % (project))

        with open(cache_file, "a") as cache:

            data_json = json.dumps(reviews)
            data_json = data_json[1:-1]  # remove []
            data_json += "," # join between arrays
            # We need to add the array to an already existing array
            cache.write(data_json)

    def _get_version(self):
        gerrit_cmd_prj = self.gerrit_cmd + " version "

        raw_data = subprocess.check_output(gerrit_cmd_prj, shell = True)

        # output: gerrit version 2.10-rc1-988-g333a9dd
        m = re.match("gerrit version (\d+)\.(\d+).*", raw_data)

        if not m:
            raise Exception("Invalid gerrit version %s" % raw_data)

        try:
            mayor = int(m.group(1))
            minor = int(m.group(2))
        except:
            raise Exception("Invalid gerrit version %s " %
                            (raw_data))

        return [mayor, minor]


    def _get_projects(self):
        """ Get all projects in gerrit """

        gerrit_cmd_projects = self.gerrit_cmd + "ls-projects "
        repos = subprocess.check_output(gerrit_cmd_projects, shell = True)

        self._projects_to_cache(repos)

        projects = repos.split("\n")
        projects.pop() # Remove last empty line

        return projects


    def _get_project_reviews(self, project):
        """ Get all reviews for a project """

        gerrit_version = self.get_version()
        last_item = None
        if gerrit_version[0] == 2 and gerrit_version[1] >= 9:
            last_item = 0

        gerrit_cmd_prj = self.gerrit_cmd + " query project:"+project+" "
        gerrit_cmd_prj += "limit:" + str(self.nreviews)
        gerrit_cmd_prj += " --all-approvals --comments --format=JSON"

        number_results = self.nreviews

        reviews = []

        while (number_results == self.nreviews or
               number_results == self.nreviews + 1):  # wikimedia gerrit returns limit+1

            cmd = gerrit_cmd_prj
            if last_item is not None:
                if gerrit_version[0] == 2 and gerrit_version[1] >= 9:
                    cmd += " --start=" + str(last_item)
                else:
                    cmd += " resume_sortkey:" + last_item

            raw_data = subprocess.check_output(cmd, shell = True)
            tickets_raw = "[" + raw_data.replace("\n", ",") + "]"
            tickets_raw = tickets_raw.replace(",]", "]")

            tickets = json.loads(tickets_raw)

            for entry in tickets:
                if 'project' in entry.keys():
                    reviews.append(entry)
                    if gerrit_version[0] == 2 and gerrit_version[1] >= 9:
                        last_item += 1
                    else:
                        last_item = entry['sortKey']
                elif 'rowCount' in entry.keys():
                    # logging.info("CONTINUE FROM: " + str(last_item))
                    number_results = entry['rowCount']

        self._project_reviews_to_cache(self, project, reviews)

        logging.info("Total reviews: %i" % len(reviews))

        return reviews

    def get_reviews(self):
        # First we need all projects
        projects = self._get_projects()

        total = len(projects)
        current_repo = 1


        for project in projects:
            # if repository != "openstack/cinder": continue
            logging.info("Processing repository:" + project + " " +
                         str(current_repo) + "/" + str(total))
            current_repo += 1












