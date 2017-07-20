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
import sys

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
logger = logging.getLogger(__name__)

class GitEnrich(Enrich):

    # REGEX to extract authors from a multi author commit: several authors present
    # in the Author field in the commit. Used if self.pair_programming is True
    AUTHOR_P2P_REGEX = re.compile(r'(?P<first_authors>.* .*) and (?P<last_author>.* .*) (?P<email>.*)')
    # Temporal hack to use pair programing only in CloudFoundry (cloudfoundry and cloudfoundry-incubator)
    CLOUDFOUNDRY_URL = 'https://github.com/cloudfoundry'


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
        self.pair_programming = False

    def set_github_token(self, token):
        self.github_token = token

    def get_field_author(self):
        return "Author"

    def get_field_date(self):
        return "grimoire_creation_date"

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

    def __get_authors(self, authors_str):
        # Extract the authors from a multiauthor

        m = self.AUTHOR_P2P_REGEX.match(authors_str)
        if m:
            authors = m.group('first_authors').split(",")
            authors = [author.strip() for author in authors]
            authors += [m.group('last_author')]
        # Remove duplicates
        authors = list(set(authors))

        return authors

    def get_identities(self, item):
        """ Return the identities from an item.
            If the repo is in GitHub, get the usernames from GitHub. """
        identities = []

        # Temporal hack until all is integrated in mordred and p2o
        if self.CLOUDFOUNDRY_URL in item['origin']:
            self.pair_programming = True

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
                logger.debug("Adding new identity %s to SH %s: %s", gh_username, SH_GIT_COMMIT, user)
                user = self.get_sh_identity(user_data)
                user['username'] = gh_username
                SortingHat.add_identity(self.sh_db, user, SH_GIT_COMMIT)
            else:
                if user_data not in self.github_logins:
                    self.github_logins[user_data] = sh_identity['username']
                    logger.debug("GitHub-commit exists. username:%s user:%s",
                                  sh_identity['username'], user_data)

        commit_hash = item['data']['commit']

        if item['data']['Author']:
            # Check multi authors commits
            m = self.AUTHOR_P2P_REGEX.match(item['data']["Author"])
            if m and self.pair_programming:
                authors = self.__get_authors(item['data']["Author"])
                for author in authors:
                    user = self.get_sh_identity(author)
                    identities.append(user)
            else:
                user = self.get_sh_identity(item['data']["Author"])
                identities.append(user)
                if self.github_token:
                    add_sh_github_identity(user, 'Author', 'author')
        if item['data']['Commit']:
            m = self.AUTHOR_P2P_REGEX.match(item['data']["Commit"])
            if m and self.pair_programming:
                committers = self.__get_authors(item['data']['Commit'])
                for committer in committers:
                    user = self.get_sh_identity(committer)
                    identities.append(user)
            else:
                user = self.get_sh_identity(item['data']['Commit'])
                identities.append(user)
                if self.github_token:
                    add_sh_github_identity(user, 'Commit', 'committer')
        if 'Signed-off-by' in item['data'] and self.pair_programming:
            signers = item['data']["Signed-off-by"]
            for signer in signers:
                user = self.get_sh_identity(signer)
                identities.append(user)

        return identities

    def get_sh_identity(self, item, identity_field=None):
        # John Smith <john.smith@bitergia.com>
        identity = {}

        git_user = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
            git_user = item['data'][identity_field]

        fields = git_user.split("<")
        name = fields[0]
        name = name.strip()  # Remove space between user and email
        email = None
        if len(fields) > 1:
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

            try:
                r.raise_for_status()
            except requests.exceptions.ConnectionError as ex:
                # Connection error
                logger.error("Can't get github login for %s in %s because a connection error ", repo, commit_hash)
                return login

            self.rate_limit = int(r.headers['X-RateLimit-Remaining'])
            self.rate_limit_reset_ts = int(r.headers['X-RateLimit-Reset'])
            logger.debug("Rate limit pending: %s", self.rate_limit)
            if self.rate_limit <= self.min_rate_to_sleep:
                seconds_to_reset = self.rate_limit_reset_ts - int(time.time()) + 1
                if seconds_to_reset < 0:
                    seconds_to_reset = 0
                cause = "GitHub rate limit exhausted."
                logger.info("%s Waiting %i secs for rate limit reset.", cause, seconds_to_reset)
                time.sleep(seconds_to_reset)
                # Retry once we have rate limit
                r = self.requests.get(commit_url, headers=headers)

            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                # commit not found probably or rate limit exhausted
                logger.error("Can't find commit %s %s", commit_url, ex)
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
                logger.error("Wrong rol: %s" % (rol))
                raise RuntimeError

            self.github_logins[user] = login
            logger.debug("%s is %s in github (not found %i authors %i committers )", user, login,
                          self.github_logins_author_not_found,
                          self.github_logins_committer_not_found)

        return login

    @metadata
    def get_rich_item(self, item):

        eitem = {}
        for f in self.RAW_FIELDS_COPY:
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
        eitem["tz"]  = int(author_date.strftime("%z")[0:3])
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
                    # logger.warning(cfile)
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

        eitem.update(self.get_grimoire_fields(commit["AuthorDate"], "commit"))

        if self.sortinghat:
            # grimoire_creation_date is needed in the item
            item.update(self.get_grimoire_fields(commit["AuthorDate"], "commit"))
            eitem.update(self.get_item_sh(item, self.roles))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        if self.pair_programming:
            eitem = self.__add_pair_programming_metrics(commit, eitem)

        return eitem


    def __add_pair_programming_metrics(self, commit, eitem):

        def get_pair_programming_metrics(eitem, nauthors):
            ndecimals = 2
            metrics = {}
            files = eitem['files']
            ladded = eitem['lines_added']
            lremoved = eitem['lines_removed']
            lchanged = eitem['lines_changed']
            metrics['pair_programming_commit'] = round(1.0 / nauthors, ndecimals)
            metrics['pair_programming_files'] = round(files / nauthors, ndecimals)
            metrics["pair_programming_lines_added"] = round(ladded / nauthors, ndecimals)
            metrics["pair_programming_lines_removed"] = round(lremoved / nauthors, ndecimals)
            metrics["pair_programming_lines_changed"] = round(lchanged / nauthors, ndecimals)

            return metrics


        # Include pair programming metrics in all cases. In general, 1 author.
        eitem.update(get_pair_programming_metrics(eitem, 1))

        # Multi author support
        eitem['is_git_commit_multi_author'] = 0
        if 'is_git_commit_multi_author' in commit:
            eitem['is_git_commit_multi_author'] = commit['is_git_commit_multi_author']
        if 'authors' in commit:
            eitem['authors'] = commit['authors']
            nauthors = len(commit['authors'])
            eitem.update(get_pair_programming_metrics(eitem, nauthors))

        # Pair Programming support using Signed-off
        eitem['Signed-off-by_number'] = 0
        eitem['is_git_commit_signed_off'] = 0
        if 'Signed-off-by' in commit:
            eitem['Signed-off-by'] = commit['Signed-off-by']
            eitem['Signed-off-by_number'] = len(commit['Signed-off-by'])
            if 'is_git_commit_signed_off' in commit:
                # Commits generated for signed_off people
                eitem['is_git_commit_signed_off'] = commit['is_git_commit_signed_off']
            # The commit for the original Author also needs this data
            eitem['authors_signed_off'] = commit['authors_signed_off']
            nauthors = len(commit['authors_signed_off'])
            eitem.update(get_pair_programming_metrics(eitem, nauthors))
        return eitem

    def enrich_items(self, ocean_backend, events=False):
        """ Implementation supporting signed-off and multiauthor/committer commits.
        """

        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        total_signed_off = 0
        total_multi_author = 0

        url = self.elastic.index_url+'/items/_bulk'

        logger.debug("Adding items to %s (in %i packs)", url, max_items)

        items = ocean_backend.fetch()

        for item in items:

            if self.CLOUDFOUNDRY_URL in item['origin']:
                self.pair_programming = True

            if self.pair_programming:
                # First we need to add the authors field to all commits
                # Check multi author
                m = self.AUTHOR_P2P_REGEX.match(item['data']['Author'])
                if m:
                    logger.debug("Multiauthor detected. Creating one commit " +
                                 "per author: %s", item['data']['Author'])
                    item['data']['authors'] = self.__get_authors(item['data']['Author'])
                    item['data']['Author'] = item['data']['authors'][0]
                m = self.AUTHOR_P2P_REGEX.match(item['data']['Commit'])
                if m:
                    logger.debug("Multicommitter detected: using just the first committer")
                    item['data']['committers'] = self.__get_authors(item['data']['Commit'])
                    item['data']['Commit'] = item['data']['committers'][0]
                # Add the authors list using the original Author and the Signed-off list
                if 'Signed-off-by' in item['data']:
                    authors_all = item['data']['Signed-off-by']+[item['data']['Author']]
                    item['data']['authors_signed_off'] = list(set(authors_all))

            if current >= max_items:
                try:
                    r = self.requests.put(url, data=bulk_json)
                    r.raise_for_status()
                    json_size = sys.getsizeof(bulk_json) / (1024*1024)
                    logger.debug("Added %i items to %s (%0.2f MB)", total, url, json_size)
                except UnicodeEncodeError:
                    # Why is requests encoding the POST data as ascii?
                    logger.error("Unicode error in enriched items")
                    logger.debug(bulk_json)
                    safe_json = str(bulk_json.encode('ascii', 'ignore'), 'ascii')
                    self.requests.put(url, data=safe_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (item[self.get_field_unique_id()])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
            total += 1

            if self.pair_programming:
                # Multi author support
                if 'authors' in item['data']:
                    # First author already added in the above commit
                    authors = item['data']['authors']
                    for i in range(1, len(authors)):
                        # logger.debug('Adding a new commit for %s', authors[i])
                        item['data']['Author'] = authors[i]
                        item['data']['is_git_commit_multi_author'] = 1
                        rich_item = self.get_rich_item(item)
                        commit_id = item[self.get_field_unique_id()] + "_" + str(i-1)
                        data_json = json.dumps(rich_item)
                        bulk_json += '{"index" : {"_id" : "%s" } }\n' % commit_id
                        bulk_json += data_json +"\n"  # Bulk document
                        current += 1
                        total += 1
                        total_multi_author += 1

                if rich_item['Signed-off-by_number'] > 0:
                    nsg = 0
                    # Remove duplicates and the already added Author if exists
                    authors = list(set(item['data']['Signed-off-by']))
                    if item['data']['Author'] in authors:
                        authors.remove(item['data']['Author'])
                    for author in authors:
                        # logger.debug('Adding a new commit for %s', author)
                        # Change the Author in the original commit and generate
                        # a new enriched item with it
                        item['data']['Author'] = author
                        item['data']['is_git_commit_signed_off'] = 1
                        rich_item = self.get_rich_item(item)
                        commit_id = item[self.get_field_unique_id()] + "_" + str(nsg)
                        data_json = json.dumps(rich_item)
                        bulk_json += '{"index" : {"_id" : "%s" } }\n' % commit_id
                        bulk_json += data_json +"\n"  # Bulk document
                        current += 1
                        total += 1
                        total_signed_off += 1
                        nsg += 1

        if total == 0:
            # No items enriched, nothing to upload to ES
            return total

        r = self.requests.put(url, data=bulk_json)
        r.raise_for_status()

        if self.pair_programming:
            logger.info("Signed-off commits generated: %i", total_signed_off)
            logger.info("Multi author commits generated: %i", total_multi_author)

        return total


    def enrich_demography(self, from_date=None):
        logger.info("Doing demography enrich from %s since %s",
                    self.elastic.index_url, from_date)

        date_field = self.get_incremental_date()

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

        logger.debug(es_query)

        r = self.requests.post(self.elastic.index_url+"/_search", data=es_query, verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            logger.error("Error getting authors mix and max date. Demography aborted.")
            logger.error(ex)
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
                logger.error("Can't find commits for %s", author['key'])
                logger.error(r.json())
                logger.error(author_query)
                continue
            for item in r.json()["hits"]["hits"]:
                new_item = item['_source']
                new_item["author_max_date"] = author['max']['value_as_string']
                if "author_min_date" not in new_item or not new_item['author_min_date']:
                    new_item["author_min_date"] = author['min']['value_as_string']
                # In p2p the ids are created during enrichment
                new_item["_item_id"] = item['_id']
                author_items.append(new_item)

            if len(author_items) >= self.elastic.max_items_bulk:
                self.elastic.bulk_upload(author_items, "_item_id")
                author_items = []

            nauthors_done += 1
            logger.debug("%s: %s %s", author['key'],
                         author['min']['value_as_string'],
                         author['max']['value_as_string'])
            logger.info("Authors processed %i/%i", nauthors_done, len(authors))

        self.elastic.bulk_upload(author_items, "_item_id")

        logger.debug("Completed demography enrich from %s" % (self.elastic.index_url))
