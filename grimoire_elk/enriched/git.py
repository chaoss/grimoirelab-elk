# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2023 Bitergia
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#   Georg Link <georglink@bitergia.com>
#   Quan`Zhou <quan@bitergia.com>
#

import json
import logging
import re
import sys

import pkg_resources
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection

from grimoirelab_toolkit.datetime import (datetime_to_utc,
                                          str_to_datetime,
                                          datetime_utcnow)
from perceval.backends.core.git import (GitCommand,
                                        GitRepository,
                                        EmptyRepositoryError,
                                        RepositoryError)
from .enrich import Enrich, metadata
from .study_ceres_aoc import areas_of_code, ESPandasConnector
from ..elastic_mapping import Mapping as BaseMapping
from ..elastic_items import HEADER_JSON, MAX_BULK_UPDATE_SIZE
from .utils import anonymize_url

GITHUB = 'https://github.com/'
DEMOGRAPHY_COMMIT_MIN_DATE = '1980-01-01'
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
    AUTHOR_P2P_REGEX = re.compile(r'(?P<first_authors>.* .*) ([aA][nN][dD]|&|\+) (?P<last_author>.* .*) (?P<email>.*)')
    AUTHOR_P2P_NEW_REGEX = re.compile(r"Co-authored-by:(?P<first_authors>.* .*)<(?P<email>.*)>\n?")

    # REGEX to extract authors from the commit
    AUTHOR_REGEX = re.compile(r'^\s*(?P<field>.*): (?P<author>.* <.*>)$')

    GIT_AOC_ENRICHED = "git_aoc-enriched"

    roles = ['Author', 'Commit']
    meta_fields = ['acked_by_multi', 'approved_by_multi', 'co_authored_by_multi', 'co_developed_by_multi',
                   'merged_by_multi', 'reported_by_multi', 'reviewed_by_multi', 'signed_off_by_multi',
                   'suggested_by_multi', 'tested_by_multi']
    meta_fields_suffixes = ['_bots', '_domains', '_names', '_org_names', '_uuids']
    meta_non_authored_prefix = 'non_authored_'

    def __init__(self, db_sortinghat=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', db_path=None,
                 db_port=None, db_ssl=False, db_verify_ssl=True, db_tenant=None,
                 pair_programming=False):
        super().__init__(db_sortinghat=db_sortinghat, json_projects_map=json_projects_map,
                         db_user=db_user, db_password=db_password, db_host=db_host,
                         db_port=db_port, db_path=db_path, db_ssl=db_ssl, db_verify_ssl=db_verify_ssl,
                         db_tenant=db_tenant)

        self.studies = []
        self.studies.append(self.enrich_demography)
        self.studies.append(self.enrich_areas_of_code)
        self.studies.append(self.enrich_onion)
        self.studies.append(self.enrich_git_branches)
        self.studies.append(self.enrich_forecast_activity)
        self.studies.append(self.enrich_extra_data)

        self.rate_limit = None
        self.rate_limit_reset_ts = None
        self.min_rate_to_sleep = 100  # if pending rate < 100 sleep
        self.pair_programming = pair_programming

    def get_field_author(self):
        return "Author"

    def get_field_unique_id(self):
        # In pair_programming the uuid is not unique: a commit can create
        # several commits
        if self.pair_programming:
            return "git_uuid"
        return "uuid"

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
        """Return the identities from an item."""

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
        if 'Signed-off-by' in item['data'] and self.pair_programming:
            signers = item['data']["Signed-off-by"]
            for signer in signers:
                user = self.get_sh_identity(signer)
                yield user

    def get_sh_identity(self, item, identity_field=None):
        # John Smith <john.smith@bitergia.com>
        identity = {}

        git_user = item  # by default a specific user dict is expected
        if isinstance(item, dict) and 'data' in item:
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

    @metadata
    def get_rich_item(self, item):

        eitem = {}
        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)
        # For pair programming uuid is not a unique field. Use git_uuid in general as unique field.
        eitem['git_uuid'] = eitem['uuid']
        # The real data
        commit = item['data']

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
            eitem['message'] = commit['message'][:self.KEYWORD_MAX_LENGTH]

        if 'refs' in commit:
            eitem["commit_tags"] = list(filter(lambda r: "tag: " in r, commit['refs']))

        eitem['hash_short'] = eitem['hash'][0:6]
        # Enrich dates

        author_date = self.__cast_str_to_datetime(commit, 'AuthorDate')
        commit_date = self.__cast_str_to_datetime(commit, 'CommitDate')

        eitem["author_date"] = author_date.replace(tzinfo=None).isoformat()
        eitem["commit_date"] = commit_date.replace(tzinfo=None).isoformat()

        eitem["author_date_weekday"] = author_date.replace(tzinfo=None).isoweekday()
        eitem["author_date_hour"] = author_date.replace(tzinfo=None).hour

        eitem["commit_date_weekday"] = commit_date.replace(tzinfo=None).isoweekday()
        eitem["commit_date_hour"] = commit_date.replace(tzinfo=None).hour

        utc_author_date = datetime_to_utc(author_date)
        utc_commit_date = datetime_to_utc(commit_date)

        eitem["utc_author"] = utc_author_date.replace(tzinfo=None).isoformat()
        eitem["utc_commit"] = utc_commit_date.replace(tzinfo=None).isoformat()

        eitem["utc_author_date_weekday"] = utc_author_date.replace(tzinfo=None).isoweekday()
        eitem["utc_author_date_hour"] = utc_author_date.replace(tzinfo=None).hour

        eitem["utc_commit_date_weekday"] = utc_commit_date.replace(tzinfo=None).isoweekday()
        eitem["utc_commit_date_hour"] = utc_commit_date.replace(tzinfo=None).hour

        eitem["tz"] = int(author_date.strftime("%z")[0:3])
        eitem["branches"] = []

        # Compute time to commit
        time_to_commit_delta = datetime_to_utc(author_date) - datetime_to_utc(commit_date)
        eitem["time_to_commit_hours"] = round(time_to_commit_delta.seconds / 3600, 2)

        # Other enrichment
        eitem["repo_name"] = item["origin"]

        if eitem["repo_name"].startswith('http'):
            eitem["repo_name"] = anonymize_url(eitem["repo_name"])

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

        grimoire_fields = self.get_grimoire_fields(author_date, "commit")
        eitem.update(grimoire_fields)

        # grimoire_creation_date is needed in the item
        item.update(grimoire_fields)
        eitem.update(self.get_item_sh(item, self.roles))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        if self.pair_programming:
            eitem = self.__add_pair_programming_metrics(commit, eitem)

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        self.__add_commit_meta_fields(eitem, commit)
        return eitem

    def __cast_str_to_datetime(self, item, attribute):
        """Convert str to datetime fixing possible errors"""

        field_date = str_to_datetime(item[attribute])

        try:
            _ = int(field_date.strftime("%z")[0:3])
        except ValueError:
            logger.warning("[git] {} in commit {} has a wrong format".format(
                           attribute, item['commit']))
            return field_date.replace(tzinfo=None).isoformat()
        return field_date

    def __add_commit_meta_fields(self, eitem, commit):
        """Add commit meta fields as signed_off_by, reviwed_by, tested_by, etc."""
        non_authored = []
        if self.meta_non_authored_prefix:
            non_authored = [self.meta_non_authored_prefix + field for field in self.meta_fields]
        all_meta_fields = self.meta_fields + non_authored

        for field in all_meta_fields:
            for suffix in self.meta_fields_suffixes:
                eitem[field + suffix] = []

        if 'message' not in commit:
            return

        meta_eitem = {}
        for line in commit['message'].split('\n'):
            m = self.AUTHOR_REGEX.match(line)
            if not m:
                continue

            meta_field = m.group('field').lower().replace('-', '_') + '_multi'
            if meta_field not in self.meta_fields:
                continue

            author = m.group('author')
            identity = self.get_sh_identity(author)

            if self.sortinghat:
                # Create SH identity if it does not exist
                backend_name = self.get_sh_backend_name()
                identity_id = self.generate_uuid(backend_name,
                                                 email=identity['email'],
                                                 name=identity['name'],
                                                 username=identity['username'])
                individual = self.get_entity(identity_id)
                if not individual:
                    identity_tuple = tuple(identity.items())
                    self.add_sh_identity_cache(identity_tuple)
                    logger.debug("Create a new individual {} in commit_meta_fields".format(identity_id))
                item_date = eitem[self.get_field_date()]
                sh_fields = self.get_item_sh_fields(identity, item_date, rol=meta_field)
            else:
                sh_fields = self.get_item_no_sh_fields(identity, rol=meta_field)

            uuid = sh_fields[meta_field + '_uuid']
            self.add_meta_fields(eitem, meta_eitem, sh_fields, meta_field, uuid, self.meta_fields_suffixes,
                                 self.meta_non_authored_prefix)
        eitem.update(meta_eitem)

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
        Multiauthor/Multcommiter commits are the ones authored/commited by more
        than one users, in such a case, ELK first extracts authors names from
        raw data through regex, for instance in,

        ...
        "data": {
            "Author": "Eduardo Morais and Zhongpeng Lin <companheiro.vermelho@gmail.com>",
            "AuthorDate": "Tue Aug 14 14:32:15 2012 -0300",
            "Commit": "Eduardo Morais and Zhongpeng Lin <companheiro.vermelho@gmail.com>",
            "CommitDate": "Tue Aug 14 14:32:15 2012 -0300",
            "Signed-off-by": [
                "Eduardo Morais <companheiro.vermelho@gmail.com>"
            ],
            "commit": "87783129c3f00d2c81a3a8e585eb86a47e39891a",
        ...

        authors extracted are ['Eduardo Morais', 'Zhongpeng Lin'] (can be more than 2).
        A new rich item is now created using each author name. For multicommitter only
        the first committer name is used to create the rich item. In case the commit is
        signed-off by committer, raw data has extra "Signed-off-by" attribute used to
        create a new rich item for every author who signed off the commit, making sure
        duplicate entries are not created.

        Note -
            "message": "Enable users to pass flags\n\nCo-authored-by: mariiapunda <mariiapunda@users.noreply.github.com>",
        Co-authored commits like these are not considered as multiauthored commits in ELK.
        """
        headers = {"Content-Type": "application/json"}

        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        total_signed_off = 0
        total_multi_author = 0

        url = self.elastic.get_bulk_url()

        logger.debug("[git] Adding items to {} (in {} packs)".format(anonymize_url(url), max_items))
        items = ocean_backend.fetch()

        for item in items:
            if self.pair_programming:
                # First we need to add the authors field to all commits
                # Check multi author
                m = self.AUTHOR_P2P_REGEX.match(item['data']['Author'])
                n = self.AUTHOR_P2P_NEW_REGEX.match(item['data']['Author'])
                if m or n:
                    logger.debug("[git] Multiauthor detected. Creating one commit "
                                 "per author: {}".format(item['data']['Author']))
                    item['data']['authors'] = self.__get_authors(item['data']['Author'])
                    item['data']['Author'] = item['data']['authors'][0]
                    item['data']['is_git_commit_multi_author'] = 1
                m = self.AUTHOR_P2P_REGEX.match(item['data']['Commit'])
                n = self.AUTHOR_P2P_NEW_REGEX.match(item['data']['Author'])
                if m or n:
                    logger.debug("[git] Multicommitter detected: using just the first committer")
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
                    logger.debug("[git] Added {} items to {} ({:.2f} MB)".format(
                                 total, anonymize_url(url), json_size))
                except UnicodeEncodeError:
                    # Why is requests encoding the POST data as ascii?
                    logger.warning("[git] Unicode error in enriched items, converting to ascii")
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
                        commit_id = item["uuid"] + "_" + str(i - 1)
                        rich_item['git_uuid'] = commit_id
                        data_json = json.dumps(rich_item)
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
            logger.info("[git] Signed-off commits generated: {}".format(total_signed_off))
            logger.info("[git] Multi author commits generated: {}".format(total_multi_author))

        return total

    def enrich_demography(self, ocean_backend, enrich_backend, alias, date_field="grimoire_creation_date",
                          author_field="author_uuid"):

        super().enrich_demography(ocean_backend, enrich_backend, alias, date_field, author_field=author_field)

    def enrich_areas_of_code(self, ocean_backend, enrich_backend, alias, no_incremental=False,
                             in_index="git-raw",
                             out_index=GIT_AOC_ENRICHED,
                             sort_on_field='metadata__timestamp'):

        log_prefix = "[git] study areas_of_code"

        logger.info("{} Starting study - Input: {} Output: {}".format(log_prefix, in_index, out_index))

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
            logger.info("{} Creating out ES index".format(log_prefix))
            # Initialize out index

            if not self.elastic.is_legacy():
                filename = pkg_resources.resource_filename('grimoire_elk', 'enriched/mappings/git_aoc_es7.json')
            else:
                filename = pkg_resources.resource_filename('grimoire_elk', 'enriched/mappings/git_aoc.json')
            out_conn.create_index(filename, delete=exists_index)

        repos = []
        for source in self.json_projects.values():
            items = source.get('git')
            if items:
                repos.extend(items)

        for repo in repos:
            anonymize_repo = anonymize_url(repo)
            repo_name = anonymize_repo.split()[0]
            logger.info("{} Processing repo: {}".format(log_prefix, repo_name))
            in_conn.update_repo(repo_name)
            out_conn.update_repo(repo_name)
            areas_of_code(git_enrich=enrich_backend, in_conn=in_conn, out_conn=out_conn)

            # delete the documents in the AOC index which correspond to commits that don't exist in the raw index
            if out_conn.exists():
                self.update_items_aoc(ocean_backend, es_out, out_index, repo_name)

        # Create alias if output index exists and alias does not
        if out_conn.exists():
            if not out_conn.exists_alias(alias) \
                    and not enrich_backend.elastic.alias_in_use(alias):
                logger.info("{} creating alias: {}".format(log_prefix, alias))
                out_conn.create_alias(alias)
            else:
                logger.warning("{} alias already exists: {}.".format(log_prefix, alias))

        logger.info("{} end".format(log_prefix))

    def get_unique_hashes_aoc(self, es_aoc, index_aoc, repository):
        """Retrieve the unique commit hashes in the AOC index

        :param es_aoc: the ES object to access AOC data
        :param index_aoc: the AOC index
        :param repository: the target repository
        """

        def __unique_commit_hashes_aoc(repository, until_date=None):
            """Retrieve all unique commit hashes in ascending order on grimoire_creation_date
            for a given repository in the AOC index"""

            fltr = [
                {
                    "term": {
                        "repository": repository
                    }
                }
            ]

            if until_date:
                fltr.append({
                    "range": {
                        "metadata__updated_on": {
                            "gte": until_date
                        }
                    }
                })

            query_unique_hashes = """
            {
                "aggs": {
                    "2": {
                      "terms": {
                        "field": "hash",
                        "size": 1000,
                        "order": {
                          "1": "asc"
                        }
                      },
                      "aggs": {
                        "1": {
                          "max": {
                            "field": "grimoire_creation_date"
                          }
                        }
                      }
                    }
                },
                "size": 0,
                "query": {
                    "bool": {
                        "filter": %s
                    }
                }
            }
            """ % json.dumps(fltr)

            return query_unique_hashes

        aoc_hashes = []
        fetching = True
        last_date = None
        previous_date = None
        while fetching:
            hits = es_aoc.search(index=index_aoc, body=__unique_commit_hashes_aoc(repository, last_date))
            buckets = hits['aggregations']['2']['buckets']

            if not buckets:
                fetching = False

            for bucket in buckets:
                aoc_hashes.append(bucket['key'])
                last_date = bucket['1']['value_as_string']

            if previous_date == last_date:
                fetching = False

            previous_date = last_date

        return aoc_hashes

    def get_diff_commits_raw_aoc(self, ocean_backend, es_aoc, index_aoc, repository):
        """Return the commit hashes which are stored in the AOC index but not in the Git raw index.

        :param ocean_backend: Ocean backend
        :param es_aoc: the ES object to access AOC data
        :param index_aoc: the AOC index
        :param repository: the target repository
        """
        fltr = {
            'name': 'origin',
            'value': [repository]
        }

        raw_hashes = set([item['data']['commit']
                          for item in ocean_backend.fetch(ignore_incremental=True, _filter=fltr)])
        aoc_hashes = set(self.get_unique_hashes_aoc(es_aoc, index_aoc, repository))

        hashes_to_delete = list(aoc_hashes.difference(raw_hashes))

        return hashes_to_delete

    def update_items_aoc(self, ocean_backend, es_aoc, index_aoc, repository):
        """Update the documents stored in the AOC index by deleting those ones corresponding
        to deleted commits

        :param ocean_backend: the Ocean backend to access the raw data
        :param es_aoc: the ES object to access AOC data
        :param index_aoc: the AOC index
        :param repository: the target repository
        """
        aoc_index_url = self.elastic_url + '/' + index_aoc
        hashes_to_delete = self.get_diff_commits_raw_aoc(ocean_backend, es_aoc, index_aoc, repository)
        to_process = []
        for _hash in hashes_to_delete:
            to_process.append(_hash)

            if len(to_process) != MAX_BULK_UPDATE_SIZE:
                continue

            # delete documents from the AOC index
            self.remove_commits(to_process, aoc_index_url, 'hash', repository)

            to_process = []

        if to_process:
            # delete documents from the AOC index
            self.remove_commits(to_process, aoc_index_url, 'hash', repository)

        logger.debug("[git] study areas_of_code {} commits deleted from {} with origin {}.".format(
            len(hashes_to_delete), anonymize_url(aoc_index_url), repository))

    def enrich_onion(self, ocean_backend, enrich_backend, alias,
                     no_incremental=False,
                     in_index='git_onion-src',
                     out_index='git_onion-enriched',
                     data_source='git',
                     contribs_field='hash',
                     timeframe_field='grimoire_creation_date',
                     sort_on_field='metadata__timestamp',
                     seconds=Enrich.ONION_INTERVAL):

        super().enrich_onion(enrich_backend=enrich_backend,
                             alias=alias,
                             in_index=in_index,
                             out_index=out_index,
                             data_source=data_source,
                             contribs_field=contribs_field,
                             timeframe_field=timeframe_field,
                             sort_on_field=sort_on_field,
                             no_incremental=no_incremental,
                             seconds=seconds)

    def get_diff_commits_origin_raw(self, ocean_backend):
        """Return the commit hashes which are stored in the raw index but not in the original repo.

        :param ocean_backend: Ocean backend
        """
        repo_origin = anonymize_url(self.perceval_backend.origin)
        fltr = {
            'name': 'origin',
            'value': [repo_origin]
        }

        current_hashes = []
        try:
            git_repo = GitRepository(self.perceval_backend.uri, self.perceval_backend.gitpath)
            current_hashes = [commit for commit in git_repo.rev_list()]
        except EmptyRepositoryError:
            logger.warning("No commits retrieved from {}, repo is empty".format(repo_origin))
        except RepositoryError:
            logger.warning("No commits retrieved from {}, repo doesn't exist locally".format(repo_origin))
        except Exception as e:
            logger.error("[git] No commits retrieved from {}, git rev-list command failed: {}".format(repo_origin, e))

        if not current_hashes:
            return current_hashes

        current_hashes = set(current_hashes)
        raw_hashes = set([item['data']['commit']
                          for item in ocean_backend.fetch(ignore_incremental=True, _filter=fltr)])

        hashes_to_delete = list(raw_hashes.difference(current_hashes))

        return hashes_to_delete

    def update_items(self, ocean_backend, enrich_backend):
        """Retrieve the commits not present in the original repository and delete
        the corresponding documents from the raw and enriched indexes"""

        repo_origin = anonymize_url(self.perceval_backend.origin)
        logger.debug("[git] update-items Checking commits for {}.".format(repo_origin))
        hashes_to_delete = self.get_diff_commits_origin_raw(ocean_backend)

        to_process = []
        for _hash in hashes_to_delete:
            to_process.append(_hash)

            if len(to_process) != MAX_BULK_UPDATE_SIZE:
                continue

            # delete documents from the raw index
            self.remove_commits(to_process, ocean_backend.elastic.index_url, 'data.commit', repo_origin)
            # delete documents from the enriched index
            self.remove_commits(to_process, enrich_backend.elastic.index_url, 'hash', repo_origin)

            to_process = []

        if to_process:
            # delete documents from the raw index
            self.remove_commits(to_process, ocean_backend.elastic.index_url, 'data.commit', repo_origin)
            # delete documents from the enriched index
            self.remove_commits(to_process, enrich_backend.elastic.index_url, 'hash', repo_origin)

        logger.debug("[git] update-items {} commits deleted from {} with origin {}.".format(
                     len(hashes_to_delete), anonymize_url(ocean_backend.elastic.index_url),
                     repo_origin))
        logger.debug("[git] update-items {} commits deleted from {} with origin {}.".format(
                     len(hashes_to_delete), anonymize_url(enrich_backend.elastic.index_url),
                     repo_origin))

    def remove_commits(self, items, index, attr, origin, origin_attr='origin'):
        """Delete documents that correspond to commits deleted in the Git repository

        :param items: target items to be deleted
        :param index: target index
        :param attr: name of the term attribute to search items
        :param origin: name of the origin from where the items must be deleted
        :param origin_attr: attribute where the origin info is stored.
        """
        es_query = '''
            {
              "query": {
                "bool": {
                    "must": {
                        "term": {
                            "%s": "%s"
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
            ''' % (origin_attr, origin, attr, ",".join(['"%s"' % i for i in items]))

        r = self.requests.post(index + "/_delete_by_query?refresh", data=es_query, headers=HEADER_JSON, verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            logger.error("[git] Error updating deleted commits for {}.".format(anonymize_url(index)))
            logger.error(r.text)
            return

    def enrich_git_branches(self, ocean_backend, enrich_backend, run_month_days=[7, 14, 21, 28]):
        """Update the information about branches within the documents representing
        commits in the enriched index.

        The example below shows how to activate the study by modifying the setup.cfg. The study
        `enrich_git_branches` will be run on days depending on the parameter `run_month_days`,
        by default the days are 7, 14, 21, and 28 of each month.

        ```
        [git]
        raw_index = git_raw
        enriched_index = git_enriched
        ...
        studies = [enrich_git_branches]

        [enrich_git_branches]
        run_month_days = [5, 22]
        ```

        :param ocean_backend: the ocean backend
        :param enrich_backend: the enrich backend
        :param run_month_days: days of the month to run this study
        """
        logger.info("[git] study git-branches start")
        day = datetime_utcnow().day
        run_month_days = list(map(int, run_month_days))
        if day not in run_month_days:
            logger.info("[git] study git-branches will execute only the days {} of each month".format(run_month_days))
            logger.info("[git] study git-branches end")
            return

        for ds in self.prjs_map:
            if ds != "git":
                continue

            urls = self.prjs_map[ds]

            for url in urls:
                if '--filter-no-collection=true' in url:
                    # Skip study when --filter-no-collection is present
                    logger.info("[git] study git-branches skipping repo {}".format(anonymize_url(url)))
                    continue
                cmd = GitCommand(*[url])
                try:
                    git_repo = GitRepository(cmd.parsed_args.uri, cmd.parsed_args.gitpath)
                except RepositoryError:
                    logger.error("[git] study git-branches skipping not cloned repo {}".format(anonymize_url(url)))
                    continue

                logger.debug("[git] study git-branches delete branch info for repo {} in index {}".format(
                             git_repo.uri, anonymize_url(enrich_backend.elastic.index_url)))
                self.delete_commit_branches(git_repo, enrich_backend)

                logger.debug("[git] study git-branches add branch info for repo {} in index {}".format(
                             git_repo.uri, anonymize_url(enrich_backend.elastic.index_url)))
                try:
                    self.add_commit_branches(git_repo, enrich_backend)
                except Exception as e:
                    logger.error("[git] study git-branches failed on repo {}, due to {}".format(git_repo.uri, e))
                    continue

                logger.debug("[git] study git-branches repo {} in index {} processed".format(
                             git_repo.uri, anonymize_url(enrich_backend.elastic.index_url)))

        logger.info("[git] study git-branches end")

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
        """ % anonymize_url(git_repo.uri)

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
            logger.error("[git] Error while deleting branches on {}".format(anonymize_url(index)))
            logger.error(r.text)
            return

        logger.debug("[git] Delete branches {}, index {}".format(r.text, anonymize_url(index)))

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
                logger.error("[git] Skip adding branch info for repo {} due to {}".format(git_repo.uri, e))
                return

    def __process_commits_in_branch(self, enrich_backend, repo_origin, branch_name, commits):
        commits_str = ",".join(['"%s"' % c for c in commits])

        # process branch names which include quotes or single quote
        digested_branch_name = branch_name
        if "'" in branch_name:
            digested_branch_name = branch_name.replace("'", "---")
            logger.warning("[git] Change branch name from {} to {}".format(branch_name, digested_branch_name))
        if '"' in branch_name:
            digested_branch_name = branch_name.replace('"', "---")
            logger.warning("[git] Change branch name from {} to {}".format(branch_name, digested_branch_name))

        # update enrich index
        fltr = self.__prepare_filter("hash", commits_str, anonymize_url(repo_origin))

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
            logger.error("[git] Error adding branch info for {}".format(anonymize_url(index)))
            logger.error(r.text)
            return

        logger.debug("[git] Add branches {}, index {}".format(r.text, anonymize_url(index)))

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
