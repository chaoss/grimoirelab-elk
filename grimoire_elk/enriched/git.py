# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
import sys
import time

import pkg_resources
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection

from grimoirelab_toolkit.datetime import (datetime_to_utc,
                                          str_to_datetime)
from perceval.backends.core.git import GitCommand, GitRepository
from .enrich import Enrich, metadata
from .study_ceres_aoc import areas_of_code, ESPandasConnector
from ..elastic_mapping import Mapping as BaseMapping
from ..elastic_items import HEADER_JSON, MAX_BULK_UPDATE_SIZE

try:
    from .sortinghat_gelk import SortingHat
    SORTINGHAT_LIBS = True
except ImportError:
    SORTINGHAT_LIBS = False

GITHUB = 'https://github.com/'
SH_GIT_COMMIT = 'github-commit'
DEMOGRAPHY_COMMIT_MIN_DATE = '1980-01-01'
AREAS_OF_CODE_ALIAS = 'git_areas_of_code'
logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
               "message_analyzed": {
                    "type": "text",
                    "index": true
               }
           }
        }"""

        return {"items": mapping}


class GitEnrich(Enrich):

    mapping = Mapping

    # REGEX to extract authors from a multi author commit: several authors present
    # in the Author field in the commit. Used if self.pair_programming is True
    AUTHOR_P2P_REGEX = re.compile(r'(?P<first_authors>.* .*) and (?P<last_author>.* .*) (?P<email>.*)')
    AUTHOR_P2P_NEW_REGEX = re.compile(r"Co-authored-by:(?P<first_authors>.* .*)<(?P<email>.*)>\n?")

    GIT_AOC_ENRICHED = "git_aoc-enriched"

    roles = ['Author', 'Commit']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', pair_programming=False):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []
        self.studies.append(self.enrich_demography)
        self.studies.append(self.enrich_areas_of_code)
        self.studies.append(self.enrich_onion)
        self.studies.append(self.enrich_git_branches)

        # GitHub API management
        self.github_token = None
        self.github_logins = {}
        self.github_logins_committer_not_found = 0
        self.github_logins_author_not_found = 0
        self.rate_limit = None
        self.rate_limit_reset_ts = None
        self.min_rate_to_sleep = 100  # if pending rate < 100 sleep
        self.pair_programming = pair_programming

    def set_github_token(self, token):
        self.github_token = token

    def get_field_author(self):
        return "Author"

    def get_field_unique_id(self):
        # In pair_programming the uuid is not unique: a commit can create
        # several commits
        if self.pair_programming:
            return "git_uuid"
        return "uuid"

    def get_fields_uuid(self):
        return ["author_uuid", "committer_uuid"]

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "grimoire_creation_date"

    def __get_authors(self, authors_str):
        # Extract the authors from a multiauthor

        m = self.AUTHOR_P2P_REGEX.match(authors_str)
        if m:
            authors = m.group('first_authors').split(",")
            authors = [author.strip() for author in authors]
            authors += [m.group('last_author')]

        n = self.AUTHOR_P2P_NEW_REGEX.findall(authors_str)
        if n:
            for i in n:
                authors += [i[0]]
        # Remove duplicates
        authors = list(set(authors))

        return authors

    def get_identities(self, item):
        """ Return the identities from an item.
            If the repo is in GitHub, get the usernames from GitHub. """

        def add_sh_github_identity(user, user_field, rol):
            """ Add a new github identity to SH if it does not exists """
            github_repo = None
            if GITHUB in item['origin']:
                github_repo = item['origin'].replace(GITHUB, '')
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
            n = self.AUTHOR_P2P_NEW_REGEX.match(item['data']["Author"])
            if (m or n) and self.pair_programming:
                authors = self.__get_authors(item['data']["Author"])
                for author in authors:
                    user = self.get_sh_identity(author)
                    yield user
            else:
                user = self.get_sh_identity(item['data']["Author"])
                yield user
                if self.github_token:
                    add_sh_github_identity(user, 'Author', 'author')
        if item['data']['Commit']:
            m = self.AUTHOR_P2P_REGEX.match(item['data']["Commit"])
            n = self.AUTHOR_P2P_NEW_REGEX.match(item['data']["Author"])
            if (m or n) and self.pair_programming:
                committers = self.__get_authors(item['data']['Commit'])
                for committer in committers:
                    user = self.get_sh_identity(committer)
                    yield user
            else:
                user = self.get_sh_identity(item['data']['Commit'])
                yield user
                if self.github_token:
                    add_sh_github_identity(user, 'Commit', 'committer')
        if 'Signed-off-by' in item['data'] and self.pair_programming:
            signers = item['data']["Signed-off-by"]
            for signer in signers:
                user = self.get_sh_identity(signer)
                yield user

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
            commit_url = GITHUB_API_URL + "/repos/%s/commits/%s" % (repo, commit_hash)
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
        # For pair programming uuid is not a unique field. Use git_uuid in general as unique field.
        eitem['git_uuid'] = eitem['uuid']
        # The real data
        commit = item['data']

        self.__fix_field_date(commit, 'AuthorDate')
        self.__fix_field_date(commit, 'CommitDate')

        # data fields to copy
        copy_fields = ["message"]
        for f in copy_fields:
            if f in commit:
                eitem[f] = commit[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"commit": "hash", "message": "message_analyzed"}
        for fn in map_fields:
            if fn in commit:
                eitem[map_fields[fn]] = commit[fn]
            else:
                eitem[map_fields[fn]] = None

        if 'message' in commit:
            eitem['message'] = commit['message'][:self.KEYWORD_MAX_SIZE]

        eitem['hash_short'] = eitem['hash'][0:6]
        # Enrich dates
        author_date = str_to_datetime(commit["AuthorDate"])
        commit_date = str_to_datetime(commit["CommitDate"])

        eitem["author_date"] = author_date.replace(tzinfo=None).isoformat()
        eitem["commit_date"] = commit_date.replace(tzinfo=None).isoformat()

        eitem["utc_author"] = datetime_to_utc(author_date).replace(tzinfo=None).isoformat()
        eitem["utc_commit"] = datetime_to_utc(commit_date).replace(tzinfo=None).isoformat()

        eitem["tz"] = int(author_date.strftime("%z")[0:3])
        eitem["branches"] = []

        # Compute time to commit
        time_to_commit_delta = datetime_to_utc(author_date) - datetime_to_utc(commit_date)
        eitem["time_to_commit_hours"] = round(time_to_commit_delta.seconds / 3600, 2)

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
        identity = self.get_sh_identity(commit["Author"])
        eitem["author_name"] = identity['name']
        eitem["author_domain"] = self.get_identity_domain(identity)

        # committer data
        identity = self.get_sh_identity(commit["Commit"])
        eitem["committer_name"] = identity['name']
        eitem["committer_domain"] = self.get_identity_domain(identity)

        # title from first line
        if 'message' in commit:
            eitem["title"] = commit['message'].split('\n')[0]
        else:
            eitem["title"] = None

        # If it is a github repo, include just the repo string
        if GITHUB in item['origin']:
            eitem['github_repo'] = item['origin'].replace(GITHUB, '')
            eitem['github_repo'] = re.sub('.git$', '', eitem['github_repo'])
            eitem["url_id"] = eitem['github_repo'] + "/commit/" + eitem['hash']

        if 'project' in item:
            eitem['project'] = item['project']

        # Adding the git author domain
        author_domain = self.get_identity_domain(self.get_sh_identity(item, 'Author'))
        eitem['git_author_domain'] = author_domain

        eitem.update(self.get_grimoire_fields(commit["AuthorDate"], "commit"))

        if self.sortinghat:
            # grimoire_creation_date is needed in the item
            item.update(self.get_grimoire_fields(commit["AuthorDate"], "commit"))
            eitem.update(self.get_item_sh(item, self.roles))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        if self.pair_programming:
            eitem = self.__add_pair_programming_metrics(commit, eitem)

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem

    def __fix_field_date(self, item, attribute):
        """Fix possible errors in the field date"""

        field_date = str_to_datetime(item[attribute])

        try:
            _ = int(field_date.strftime("%z")[0:3])
        except ValueError:
            logger.warning("%s in commit %s has a wrong format", attribute, item['commit'])
            item[attribute] = field_date.replace(tzinfo=None).isoformat()

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
        """ Implementation supporting signed-off and multiauthor/committer commits."""

        headers = {"Content-Type": "application/json"}

        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        total_signed_off = 0
        total_multi_author = 0

        url = self.elastic.index_url + '/items/_bulk'

        logger.debug("Adding items to %s (in %i packs)", self.elastic.anonymize_url(url), max_items)

        items = ocean_backend.fetch()

        for item in items:
            if self.pair_programming:
                # First we need to add the authors field to all commits
                # Check multi author
                m = self.AUTHOR_P2P_REGEX.match(item['data']['Author'])
                n = self.AUTHOR_P2P_NEW_REGEX.match(item['data']['Author'])
                if m or n:
                    logger.debug("Multiauthor detected. Creating one commit "
                                 "per author: %s", item['data']['Author'])
                    item['data']['authors'] = self.__get_authors(item['data']['Author'])
                    item['data']['Author'] = item['data']['authors'][0]
                m = self.AUTHOR_P2P_REGEX.match(item['data']['Commit'])
                n = self.AUTHOR_P2P_NEW_REGEX.match(item['data']['Author'])
                if m or n:
                    logger.debug("Multicommitter detected: using just the first committer")
                    item['data']['committers'] = self.__get_authors(item['data']['Commit'])
                    item['data']['Commit'] = item['data']['committers'][0]
                # Add the authors list using the original Author and the Signed-off list
                if 'Signed-off-by' in item['data']:
                    authors_all = item['data']['Signed-off-by'] + [item['data']['Author']]
                    item['data']['authors_signed_off'] = list(set(authors_all))

            if current >= max_items:
                try:
                    total += self.elastic.safe_put_bulk(url, bulk_json)
                    json_size = sys.getsizeof(bulk_json) / (1024 * 1024)
                    logger.debug("Added %i items to %s (%0.2f MB)", total, self.elastic.anonymize_url(url), json_size)
                except UnicodeEncodeError:
                    # Why is requests encoding the POST data as ascii?
                    logger.error("Unicode error in enriched items")
                    logger.debug(bulk_json)
                    safe_json = str(bulk_json.encode('ascii', 'ignore'), 'ascii')
                    total += self.elastic.safe_put_bulk(url, safe_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            unique_field = self.get_field_unique_id()
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (rich_item[unique_field])
            bulk_json += data_json + "\n"  # Bulk document
            current += 1

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
                        item['data']['is_git_commit_multi_author'] = 1
                        data_json = json.dumps(rich_item)
                        commit_id = item["uuid"] + "_" + str(i - 1)
                        rich_item['git_uuid'] = commit_id
                        bulk_json += '{"index" : {"_id" : "%s" } }\n' % rich_item['git_uuid']
                        bulk_json += data_json + "\n"  # Bulk document
                        current += 1
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
                        commit_id = item["uuid"] + "_" + str(nsg)
                        rich_item['git_uuid'] = commit_id
                        data_json = json.dumps(rich_item)
                        bulk_json += '{"index" : {"_id" : "%s" } }\n' % rich_item['git_uuid']
                        bulk_json += data_json + "\n"  # Bulk document
                        current += 1
                        total_signed_off += 1
                        nsg += 1

        if current > 0:
            total += self.elastic.safe_put_bulk(url, bulk_json)

        if total == 0:
            # No items enriched, nothing to upload to ES
            return total

        if self.pair_programming:
            logger.info("Signed-off commits generated: %i", total_signed_off)
            logger.info("Multi author commits generated: %i", total_multi_author)

        return total

    def enrich_demography(self, ocean_backend, enrich_backend, date_field="grimoire_creation_date",
                          author_field="author_uuid"):

        super().enrich_demography(ocean_backend, enrich_backend, date_field, author_field=author_field)

    def enrich_areas_of_code(self, ocean_backend, enrich_backend, no_incremental=False,
                             in_index="git-raw",
                             out_index=GIT_AOC_ENRICHED,
                             sort_on_field='metadata__timestamp'):

        log_prefix = "[git] study areas_of_code"

        logger.info(log_prefix + " Starting study - Input: " + in_index + " Output: " + out_index)

        # Creating connections
        es_in = Elasticsearch([ocean_backend.elastic.url], retry_on_timeout=True, timeout=100,
                              verify_certs=self.elastic.requests.verify,
                              connection_class=RequestsHttpConnection)
        es_out = Elasticsearch([enrich_backend.elastic.url], retry_on_timeout=True,
                               timeout=100, verify_certs=self.elastic.requests.verify,
                               connection_class=RequestsHttpConnection)
        in_conn = ESPandasConnector(es_conn=es_in, es_index=in_index, sort_on_field=sort_on_field)
        out_conn = ESPandasConnector(es_conn=es_out, es_index=out_index, sort_on_field=sort_on_field, read_only=False)

        exists_index = out_conn.exists()
        if no_incremental or not exists_index:
            logger.info(log_prefix + " Creating out ES index")
            # Initialize out index
            filename = pkg_resources.resource_filename('grimoire_elk', 'enriched/mappings/git_aoc.json')
            out_conn.create_index(filename, delete=exists_index)

        repos = []
        for source in self.json_projects.values():
            items = source.get('git')
            if items:
                repos.extend(items)

        for repo in repos:
            logger.info(log_prefix + " Processing repo: " + repo)
            in_conn.update_repo(repo)
            out_conn.update_repo(repo)
            areas_of_code(git_enrich=enrich_backend, in_conn=in_conn, out_conn=out_conn)

        # Create alias if output index exists and alias does not
        if out_conn.exists():
            if not out_conn.exists_alias(AREAS_OF_CODE_ALIAS) \
                    and not enrich_backend.elastic.alias_in_use(AREAS_OF_CODE_ALIAS):
                logger.info(log_prefix + " Creating alias: %s", AREAS_OF_CODE_ALIAS)
                out_conn.create_alias(AREAS_OF_CODE_ALIAS)
            else:
                logger.info(log_prefix + " Alias already exists: %s.", AREAS_OF_CODE_ALIAS)

        logger.info(log_prefix + " End")

    def enrich_onion(self, ocean_backend, enrich_backend,
                     no_incremental=False,
                     in_index='git_onion-src',
                     out_index='git_onion-enriched',
                     data_source='git',
                     contribs_field='hash',
                     timeframe_field='grimoire_creation_date',
                     sort_on_field='metadata__timestamp',
                     seconds=Enrich.ONION_INTERVAL):

        super().enrich_onion(enrich_backend=enrich_backend,
                             in_index=in_index,
                             out_index=out_index,
                             data_source=data_source,
                             contribs_field=contribs_field,
                             timeframe_field=timeframe_field,
                             sort_on_field=sort_on_field,
                             no_incremental=no_incremental,
                             seconds=seconds)

    def update_items(self, ocean_backend, enrich_backend):
        """Retrieve the commits not present in the original repository and delete
        the corresponding documents from the raw and enriched indexes"""

        fltr = {
            'name': 'origin',
            'value': [self.perceval_backend.origin]
        }

        logger.debug("[update-items] Checking commits for %s.", self.perceval_backend.origin)

        git_repo = GitRepository(self.perceval_backend.uri, self.perceval_backend.gitpath)

        try:
            current_hashes = set([commit for commit in git_repo.rev_list()])
        except Exception as e:
            logger.error("Skip updating branch info for repo %s, git rev-list command failed: %s", git_repo.uri, e)
            return

        raw_hashes = set([item['data']['commit']
                          for item in ocean_backend.fetch(ignore_incremental=True, _filter=fltr)])

        hashes_to_delete = list(raw_hashes.difference(current_hashes))

        to_process = []
        for _hash in hashes_to_delete:
            to_process.append(_hash)

            if len(to_process) != MAX_BULK_UPDATE_SIZE:
                continue

            # delete documents from the raw index
            self.remove_commits(to_process, ocean_backend.elastic.index_url,
                                'data.commit', self.perceval_backend.origin)
            # delete documents from the enriched index
            self.remove_commits(to_process, enrich_backend.elastic.index_url,
                                'hash', self.perceval_backend.origin)

            to_process = []

        if to_process:
            # delete documents from the raw index
            self.remove_commits(to_process, ocean_backend.elastic.index_url,
                                'data.commit', self.perceval_backend.origin)
            # delete documents from the enriched index
            self.remove_commits(to_process, enrich_backend.elastic.index_url,
                                'hash', self.perceval_backend.origin)

        logger.debug("[update-items] %s commits deleted from %s with origin %s.",
                     len(hashes_to_delete), ocean_backend.elastic.anonymize_url(ocean_backend.elastic.index_url),
                     self.perceval_backend.origin)
        logger.debug("[update-items] %s commits deleted from %s with origin %s.",
                     len(hashes_to_delete), enrich_backend.elastic.anonymize_url(enrich_backend.elastic.index_url),
                     self.perceval_backend.origin)

    def remove_commits(self, items, index, attribute, origin):
        """Delete documents that correspond to commits deleted in the Git repository

        :param items: target items to be deleted
        :param index: target index
        :param attribute: name of the term attribute to search items
        :param origin: name of the origin from where the items must be deleted
        """
        es_query = '''
            {
              "query": {
                "bool": {
                    "must": {
                        "term": {
                            "origin": "%s"
                        }
                    },
                    "filter": {
                        "terms": {
                            "%s": [%s]
                        }
                    }
                }
              }
            }
            ''' % (origin, attribute, ",".join(['"%s"' % i for i in items]))

        r = self.requests.post(index + "/_delete_by_query?refresh", data=es_query, headers=HEADER_JSON, verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            logger.error("Error updating deleted commits for %s.", self.elastic.anonymize_url(index))
            logger.error(r.text)
            return

    def enrich_git_branches(self, ocean_backend, enrich_backend):
        """Update the information about branches within the documents representing
        commits in the enriched index.

        :param ocean_backend: the ocean backend
        :param enrich_backend: the enrich backend
        """
        logger.debug("[git-branches] Start study")
        for ds in self.prjs_map:
            if ds != "git":
                continue

            urls = self.prjs_map[ds]

            for url in urls:
                cmd = GitCommand(*[url])

                git_repo = GitRepository(cmd.parsed_args.uri, cmd.parsed_args.gitpath)

                logger.debug("[git-branches] Delete branch info for repo %s in index %s",
                             git_repo.uri, self.elastic.anonymize_url(enrich_backend.elastic.index_url))
                self.delete_commit_branches(git_repo, enrich_backend)

                logger.debug("[git-branches] Add branch info for repo %s in index %s",
                             git_repo.uri, self.elastic.anonymize_url(enrich_backend.elastic.index_url))
                self.add_commit_branches(git_repo, enrich_backend)

                logger.debug("[git-branches] Repo %s in index %s processed",
                             git_repo.uri, self.elastic.anonymize_url(enrich_backend.elastic.index_url))

        logger.debug("[git-branches] End study")

    def delete_commit_branches(self, git_repo, enrich_backend):
        """Delete the information about branches from the documents representing
        commits in the enriched index.

        :param git_repo: GitRepository object
        :param enrich_backend: the enrich backend
        """
        fltr = """
            "filter": [
                {
                    "term": {
                        "origin": "%s"
                    }
                }
            ]
        """ % git_repo.uri

        # reset references in enrich index
        es_query = """
            {
              "script": {
                "source": "ctx._source.branches = new HashSet();",
                "lang": "painless"
              },
              "query": {
                "bool": {
                    %s
                }
              }
            }
            """ % fltr

        index = enrich_backend.elastic.index_url
        r = self.requests.post(index + "/_update_by_query?refresh", data=es_query, headers=HEADER_JSON, verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            logger.error("Error while deleting branches on %s",
                         self.elastic.anonymize_url(index))
            logger.error(r.text)
            return

        logger.debug("Delete branches %s, index %s", r.text, self.elastic.anonymize_url(index))

    def add_commit_branches(self, git_repo, enrich_backend):
        """Add the information about branches to the documents representing commits in
        the enriched index. Branches are obtained using the command `git ls-remote`,
        then for each branch, the list of commits is retrieved via the command `git rev-list branch-name` and
        used to update the corresponding items in the enriched index.

        :param git_repo: GitRepository object
        :param enrich_backend: the enrich backend
        """
        to_process = []
        for hash, refname in git_repo._discover_refs(remote=True):

            if not refname.startswith('refs/heads/'):
                continue

            commit_count = 0
            branch_name = refname.replace('refs/heads/', '')

            try:
                commits = git_repo.rev_list([branch_name])

                for commit in commits:
                    to_process.append(commit)
                    commit_count += 1

                    if commit_count == MAX_BULK_UPDATE_SIZE:
                        self.__process_commits_in_branch(enrich_backend, git_repo.uri, branch_name, to_process)

                        # reset the counter
                        to_process = []
                        commit_count = 0

                if commit_count:
                    self.__process_commits_in_branch(enrich_backend, git_repo.uri, branch_name, to_process)

            except Exception as e:
                logger.error("Skip adding branch info for repo %s due to %s", git_repo.uri, e)
                return

    def __process_commits_in_branch(self, enrich_backend, repo_origin, branch_name, commits):
        commits_str = ",".join(['"%s"' % c for c in commits])

        # process branch names which include quotes or single quote
        digested_branch_name = branch_name
        if "'" in branch_name:
            digested_branch_name = branch_name.replace("'", "---")
            logger.warning("Change branch name from %s to %s", branch_name, digested_branch_name)
        if '"' in branch_name:
            digested_branch_name = branch_name.replace('"', "---")
            logger.warning("Change branch name from %s to %s", branch_name, digested_branch_name)

        # update enrich index
        fltr = self.__prepare_filter("hash", commits_str, repo_origin)

        es_query = """
            {
              "script": {
                "source": "if(!ctx._source.branches.contains(params.branch)){ctx._source.branches.add(params.branch);}",
                "lang": "painless",
                "params": {
                    "branch": "'%s'"
                }
              },
              "query": {
                "bool": {
                    %s
                }
              }
            }
            """ % (digested_branch_name, fltr)

        index = enrich_backend.elastic.index_url
        r = self.requests.post(index + "/_update_by_query?refresh", data=es_query, headers=HEADER_JSON, verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            logger.error("Error adding branch info for %s", self.elastic.anonymize_url(index))
            logger.error(r.text)
            return

        logger.debug("Add branches %s, index %s", r.text, self.elastic.anonymize_url(index))

    def __prepare_filter(self, terms_attr, terms_value, repo_origin):
        fltr = """
            "filter": [
                {
                    "terms": {
                        "%s": [%s]
                    }
                },
                {
                    "term": {
                        "origin": "%s"
                    }
                }
            ]
        """ % (terms_attr, terms_value, repo_origin)

        return fltr
