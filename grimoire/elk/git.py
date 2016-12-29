#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
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
import time

import requests

from dateutil import parser

from .enrich import Enrich, metadata

try:
    from .sortinghat import SortingHat
    SORTINGHAT_LIBS = True
except ImportError:
    SORTINGHAT_LIBS = False

GITHUB = 'https://github.com/'
SH_GIT_COMMIT = 'github-commit'
DEMOGRAPHY_COMMIT_MIN_DATE='1980-01-01'

class GitEnrich(Enrich):

    roles = ['Author', 'Commit']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = [self.enrich_demography]

        # GitHub API management
        self.github_token = None
        self.github_logins = {}
        self.github_logins_committer_not_found = 0
        self.github_logins_author_not_found = 0
        self.rate_limit = None
        self.rate_limit_reset_ts = None
        self.min_rate_to_sleep = 100  # if pending rate < 100 sleep

    def set_github_token(self, token):
        self.github_token = token

    def get_field_author(self):
        return "Author"

    def get_field_unique_id(self):
        return "ocean-unique-id"

    def get_fields_uuid(self):
        return ["author_uuid", "committer_uuid"]

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
               "message_analyzed": {
                  "type": "string",
                  "index":"analyzed"
               }
           }
       }"""

        return {"items":mapping}

    def get_identities(self, item):
        """ Return the identities from an item.
            If the repo is in GitHub, get the usernames from GitHub. """
        identities = []

        def add_sh_github_identity(user, user_field, rol):
            """ Add a new github identity to SH if it does not exists """
            github_repo = None
            if GITHUB in item['origin']:
                github_repo = item['origin'].replace(GITHUB,'')
                github_repo = re.sub('.git$', '', github_repo)
            if not github_repo:
                return

            # Try to get the identity from SH
            user_data = item['data'][user_field]
            sh_identity = SortingHat.get_github_commit_username(self.sh_db, user, SH_GIT_COMMIT)
            if not sh_identity:
                # Get the usename from GitHub
                gh_username = self.get_github_login(user_data, rol, commit_hash, github_repo)
                # Create a new SH identity with name, email from git and username from github
                logging.debug("Adding new identity %s to SH %s: %s", gh_username, SH_GIT_COMMIT, user)
                user = self.get_sh_identity(user_data)
                user['username'] = gh_username
                SortingHat.add_identity(self.sh_db, user, SH_GIT_COMMIT)
            else:
                if user_data not in self.github_logins:
                    self.github_logins[user_data] = sh_identity['username']
                    logging.debug("GitHub-commit exists. username:%s user:%s",
                                  sh_identity['username'], user_data)

        commit_hash = item['data']['commit']

        if item['data']['Author']:
            user = self.get_sh_identity(item['data']["Author"])
            identities.append(user)
            if self.github_token:
                add_sh_github_identity(user, 'Author', 'author')
        if item['data']['Commit']:
            user = self.get_sh_identity(item['data']['Commit'])
            identities.append(user)
            if self.github_token:
                add_sh_github_identity(user, 'Commit', 'committer')

        return identities

    def get_sh_identity(self, item, identity_field=None):
        # John Smith <john.smith@bitergia.com>
        identity = {}

        git_user = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
            git_user = item['data'][identity_field]

        name = git_user.split("<")[0]
        name = name.strip()  # Remove space between user and email
        email = git_user.split("<")[1][:-1]
        identity['username'] = None
        identity['email'] = email
        identity['name'] = name

        return identity

    def get_project_repository(self, eitem):
        return eitem['origin']

    def get_github_login(self, user, rol, commit_hash, repo):
        """ rol: author or committer """
        login = None
        try:
            login = self.github_logins[user]
        except KeyError:
            # Get the login from github API
            GITHUB_API_URL = "https://api.github.com"
            commit_url = GITHUB_API_URL+"/repos/%s/commits/%s" % (repo, commit_hash)
            headers = {'Authorization': 'token ' + self.github_token}

            r = self.requests.get(commit_url, headers=headers)

            self.rate_limit = int(r.headers['X-RateLimit-Remaining'])
            self.rate_limit_reset_ts = int(r.headers['X-RateLimit-Reset'])
            logging.debug("Rate limit pending: %s", self.rate_limit)
            if self.rate_limit <= self.min_rate_to_sleep:
                seconds_to_reset = self.rate_limit_reset_ts - int(time.time()) + 1
                if seconds_to_reset < 0:
                    seconds_to_reset = 0
                cause = "GitHub rate limit exhausted."
                logging.info("%s Waiting %i secs for rate limit reset.", cause, seconds_to_reset)
                time.sleep(seconds_to_reset)
                # Retry once we have rate limit
                r = self.requests.get(commit_url, headers=headers)

            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                # commit not found probably or rate limit exhausted
                logging.error("Can't find commit %s %s", commit_url, ex)
                return login

            commit_json = r.json()
            author_login = None
            if 'author' in commit_json and commit_json['author']:
                author_login = commit_json['author']['login']
            else:
                self.github_logins_author_not_found += 1

            user_login = None
            if 'committer' in commit_json and commit_json['committer']:
                user_login = commit_json['committer']['login']
            else:
                self.github_logins_committer_not_found += 1

            if rol == "author":
                login = author_login
            elif rol == "committer":
                login = user_login
            else:
                logging.error("Wrong rol: %s" % (rol))
                raise RuntimeError

            self.github_logins[user] = login
            logging.debug("%s is %s in github (not found %i authors %i committers )", user, login,
                          self.github_logins_author_not_found,
                          self.github_logins_committer_not_found)

        return login

    @metadata
    def get_rich_item(self, item):
        eitem = {}
        # metadata fields to copy
        copy_fields = ["metadata__updated_on","metadata__timestamp","ocean-unique-id","origin"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        commit = item['data']
        # data fields to copy
        copy_fields = ["message","Author"]
        for f in copy_fields:
            if f in commit:
                eitem[f] = commit[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"commit": "hash","message":"message_analyzed","Commit":"Committer"}
        for fn in map_fields:
            if fn in commit:
                eitem[map_fields[fn]] = commit[fn]
            else:
                eitem[map_fields[fn]] = None
        eitem['hash_short'] = eitem['hash'][0:6]
        # Enrich dates
        author_date = parser.parse(commit["AuthorDate"])
        commit_date = parser.parse(commit["CommitDate"])
        eitem["author_date"] = author_date.replace(tzinfo=None).isoformat()
        eitem["commit_date"] = commit_date.replace(tzinfo=None).isoformat()
        eitem["utc_author"] = (author_date-author_date.utcoffset()).replace(tzinfo=None).isoformat()
        eitem["utc_commit"] = (commit_date-commit_date.utcoffset()).replace(tzinfo=None).isoformat()
        eitem["tz"]  = int(commit_date.strftime("%z")[0:3])
        # Other enrichment
        eitem["repo_name"] = item["origin"]
        # Number of files touched
        eitem["files"] = 0
        # Number of lines added and removed
        lines_added = 0
        lines_removed = 0
        for cfile in commit["files"]:
            if 'action' not in cfile:
                # merges are not counted
                continue
            eitem["files"] += 1
            if 'added' in cfile and 'removed' in cfile:
                try:
                    lines_added += int(cfile["added"])
                    lines_removed += int(cfile["removed"])
                except ValueError:
                    # logging.warning(cfile)
                    continue
        eitem["lines_added"] = lines_added
        eitem["lines_removed"] = lines_removed
        eitem["lines_changed"] = lines_added + lines_removed

        # author_name and author_domain are added always
        identity  = self.get_sh_identity(commit["Author"])
        eitem["author_name"] = identity['name']
        eitem["author_domain"] = self.get_identity_domain(identity)

        # committer data
        identity  = self.get_sh_identity(commit["Commit"])
        eitem["committer_name"] = identity['name']
        eitem["committer_domain"] = self.get_identity_domain(identity)

        # title from first line
        if 'message' in commit:
            eitem["title"] = commit['message'].split('\n')[0]
        else:
            eitem["title"] = None

        # If it is a github repo, include just the repo string
        if GITHUB in item['origin']:
            eitem['github_repo'] = item['origin'].replace(GITHUB,'')
            eitem['github_repo'] = re.sub('.git$', '', eitem['github_repo'])
            eitem["url_id"] = eitem['github_repo']+"/commit/"+eitem['hash']

        if 'project' in item:
            eitem['project'] = item['project']

        if self.sortinghat:
            eitem.update(self.get_item_sh(item, self.roles))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(commit["AuthorDate"], "commit"))

        return eitem

    def enrich_demography(self, from_date=None):
        logging.debug("Doing demography enrich from %s", self.elastic.index_url)

        if from_date:
            # The from_date must be max author_max_date
            from_date = self.elastic.get_last_item_field("author_max_date")
            logging.debug("Demography since: %s", from_date)

        date_field = self.get_field_date()

        # Don't use commits before DEMOGRAPHY_COMMIT_MIN_DATE
        filters = '''
        {"range":
            {"%s": {"gte": "%s"}}
        }
        ''' % (date_field, DEMOGRAPHY_COMMIT_MIN_DATE)

        if from_date:
            from_date = from_date.isoformat()

            filters += '''
            ,
            {"range":
                {"%s": {"gte": "%s"}}
            }
            ''' % (date_field, from_date)

        query = """
        "query": {
            "bool": {
                "must": [%s]
            }
        },
        """ % (filters)


        # First, get the min and max commit date for all the authors
        # Limit aggregations: https://github.com/elastic/elasticsearch/issues/18838
        # 10000 seems to be a sensible number of the number of people in git
        es_query = """
        {
          %s
          "size": 0,
          "aggs": {
            "author": {
              "terms": {
                "field": "Author",
                "size": 10000
              },
              "aggs": {
                "min": {
                  "min": {
                    "field": "utc_commit"
                  }
                },
                "max": {
                  "max": {
                    "field": "utc_commit"
                  }
                }
              }
            }
          }
        }
        """ % (query)

        logging.debug(es_query)

        r = self.requests.post(self.elastic.index_url+"/_search", data=es_query, verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            logging.error("Error getting authors mix and max date. Demography aborted.")
            logging.error(ex)
            return

        authors = r.json()['aggregations']['author']['buckets']

        author_items = []  # items from author with new date fields added
        nauthors_done = 0
        author_query = """
        {
            "query": {
                "bool": {
                    "must": [
                        {"term":
                            { "Author" : ""  }
                        }
                        ]
                }
            }

        }
        """
        author_query_json = json.loads(author_query)


        for author in authors:
            # print("%s: %s %s" % (author['key'], author['min']['value_as_string'], author['max']['value_as_string']))
            # Time to add all the commits (items) from this author
            author_query_json['query']['bool']['must'][0]['term']['Author'] = author['key']
            author_query_str = json.dumps(author_query_json)
            r = self.requests.post(self.elastic.index_url+"/_search?size=10000", data=author_query_str, verify=False)

            if "hits" not in r.json():
                logging.error("Can't find commits for %s" % (author['key']))
                print(r.json())
                print(author_query)
                continue
            for item in r.json()["hits"]["hits"]:
                new_item = item['_source']
                new_item.update(
                    {"author_min_date":author['min']['value_as_string'],
                     "author_max_date":author['max']['value_as_string']}
                )
                author_items.append(new_item)

            if len(author_items) >= self.elastic.max_items_bulk:
                self.elastic.bulk_upload(author_items, "ocean-unique-id")
                author_items = []

            nauthors_done += 1
            logging.info("Authors processed %i/%i" % (nauthors_done, len(authors)))

        self.elastic.bulk_upload(author_items, "ocean-unique-id")

        logging.debug("Completed demography enrich from %s" % (self.elastic.index_url))
