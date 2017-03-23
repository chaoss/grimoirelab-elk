#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2017 Bitergia
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

import argparse
import json
import logging
import os
import tempfile

import requests

from grimoire_elk.arthur import load_identities
from grimoire_elk.elk.gerrit import GerritEnrich
from grimoire_elk.elk.git import GitEnrich
from grimoire_elk.elk.elastic import ElasticSearch


# Logging formats
LOG_FORMAT = "[%(asctime)s] - %(message)s"
DEBUG_LOG_FORMAT = "[%(asctime)s - %(name)s - %(levelname)s] - %(message)s"

# Default values that can be changed from command line
GERRIT_INDEX_ENRICH = 'gerrit_opnfv_170207_enriched_170306'
GERRIT_INDEX_RAW = 'gerrit_openstack_170322'
GIT_INDEX_ENRICH = 'git_openstack_170313_enriched_170313'
GIT_INDEX_RAW = 'git_openstack_170313'
OPNFV_UPSTREAM_FILE = 'https://git.opnfv.org/doctor/plain/UPSTREAM'
PROJECT_NAME = 'openstack'  # upstream project name

logger = logging.getLogger(__name__)

def configure_logging(debug=False):
    """Configure logging
    The function configures log messages. By default, log messages
    are sent to stderr. Set the parameter `debug` to activate the
    debug mode.
    :param debug: set the debug mode
    """
    if not debug:
        logging.basicConfig(level=logging.INFO,
                            format=LOG_FORMAT)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('urrlib3').setLevel(logging.WARNING)
    else:
        logging.basicConfig(level=logging.DEBUG,
                            format=DEBUG_LOG_FORMAT)


def get_params():
    args_parser = argparse.ArgumentParser(usage="usage: track_items [options]",
                                          description="Track items from different data sources.")
    args_parser.add_argument("-e", "--elastic-url-raw", required=True,
                             help="ElasticSearch URL with raw indexes wich includes the items to track")
    args_parser.add_argument("--elastic-url-enrich", required=True,
                             help="ElasticSearch URL for enriched track items")
    args_parser.add_argument("-u", "--upstream-url", default=OPNFV_UPSTREAM_FILE,
                             help="URL with upstream file with the items to track")
    args_parser.add_argument('-g', '--debug', dest='debug', action='store_true')
    args_parser.add_argument("--index-gerrit-raw", default=GERRIT_INDEX_RAW,
                             help="ES index with gerrit raw items")
    args_parser.add_argument("--index-gerrit-enrich", default=GERRIT_INDEX_ENRICH,
                             help="ES index with gerrit enriched items")
    args_parser.add_argument("--index-git-raw", default=GIT_INDEX_RAW,
                             help="ES index with git raw items")
    args_parser.add_argument("--index-git-enrich", default=GIT_INDEX_ENRICH,
                             help="ES index with git enriched items")
    args_parser.add_argument("--project", default=PROJECT_NAME,
                             help="project to be used in enriched items")


    return args_parser.parse_args()

def fetch_track_items(upstream_file_url, data_source):
    """ The file format is:

    # Upstream contributions, bitergia will crawl this and extract the relevant information
    # system is one of Gerrit, Bugzilla, Launchpad (insert more)
    ---
    -
      url: https://review.openstack.org/169836
      system: Gerrit
    """

    track_uris = []
    req = requests.get(upstream_file_url)
    req.raise_for_status()
    lines = iter(req.text.split("\n"))
    for line in lines:
        if 'url: ' in line:
            dso = next(lines).split('system: ')[1].strip('\n')
            if dso == data_source:
                track_uris.append(line.split('url: ')[1].strip('\n'))
    return track_uris

def get_gerrit_number(gerrit_uri):
    # Get the uuid for this item_uri. Possible formats
    # https://review.openstack.org/424868/
    # https://review.openstack.org/#/c/430428
    # https://review.openstack.org/314915

    if gerrit_uri[-1] == "/":
        gerrit_uri = gerrit_uri[:-1]

    number = gerrit_uri.rsplit("/", 1)[1]

    return number

def get_gerrit_origin(gerrit_uri):
    # Get the uuid for this item_uri. Possible formats
    # https://review.openstack.org/424868/
    # https://review.openstack.org/#/c/430428
    # https://review.openstack.org/314915
    # https://review.openstack.org/314915 redirects to https://review.openstack.org/#/c/314915

    if gerrit_uri[-1] == "/":
        gerrit_uri = gerrit_uri[:-1]

    gerrit_uri = gerrit_uri.replace('#/c/', '')
    origin = gerrit_uri.rsplit("/", 1)[0]

    return origin

def get_gerrit_numbers(gerrit_uris):
    # uuid to search the gerrit review in ElasticSearch
    numbers = []
    for gerrit_uri in gerrit_uris:
        gerrit_number = get_gerrit_number(gerrit_uri)
        numbers.append(gerrit_number)

    return numbers

def get_gerrit_reviews(es, index_gerrit_raw, gerrit_numbers):
    # Get gerrit raw items
    query = {
        "query": {
            "terms": {"data.number": gerrit_numbers}
        }
    }

    req = requests.post(es + "/" + index_gerrit_raw + "/_search?limit=10000",
                        data=json.dumps(query))
    req.raise_for_status()
    reviews_es = req.json()["hits"]["hits"]
    reviews = []
    for review in reviews_es:
        reviews.append(review['_source'])
    return reviews

def create_projects_file(project_name, data_source, items):
    """ Create a projects file from the items origin data """

    repositories = []
    for item in items:
        if item['origin'] not in repositories:
            repositories.append(item['origin'])
    projects = {
        project_name: {
            data_source: repositories
        }
    }

    projects_file, projects_file_path = tempfile.mkstemp(prefix='track_items_')

    with open(projects_file_path, "w") as pfile:
        json.dump(projects, pfile, indent=True)

    return projects_file_path


def enrich_gerrit_items(es, index_gerrit_raw, gerrit_numbers, project):
    reviews = get_gerrit_reviews(es, index_gerrit_raw, gerrit_numbers)
    projects_file_path = create_projects_file(project, "gerrit", reviews)
    logger.info("Total gerrit track items to be enriched: %i", len(reviews))

    enriched_items = []
    enricher = GerritEnrich(db_sortinghat='opnfv_track_sh',
                            db_user='root', db_host='localhost',
                            json_projects_map=projects_file_path)

    # First load identities
    load_identities(reviews, enricher)

    # Then enrich
    for review in reviews:
        enriched_items.append(enricher.get_rich_item(review))

    os.unlink(projects_file_path)

    return enriched_items

def get_git_commits(es, index_git_raw, commits_sha_list):
    # Get gerrit raw items
    query = {
        "query": {
            "terms": {"data.commit" : commits_sha_list}
        }
    }

    req = requests.post(es + "/" + index_git_raw + "/_search?limit=10000",
                        data=json.dumps(query))
    req.raise_for_status()
    commits_es = req.json()["hits"]["hits"]
    commits = []
    commits_sha_list_found = []
    for commit in commits_es:
        commits.append(commit['_source'])
        commits_sha_list_found.append(commit['_source']['data']['commit'])
    commits_not_found = set(commits_sha_list) - set(commits_sha_list_found)
    logger.debug("Review commits not found upstream %i: %s",
                 len(commits_not_found), commits_not_found)
    return commits

def enrich_git_items(es, index_git_raw, commits_sha_list, project):
    commits = get_git_commits(es, index_git_raw, commits_sha_list)
    projects_file_path = create_projects_file(project, "git", commits)
    logger.info("Total git track items to be enriched: %i", len(commits))

    enriched_items = []
    enricher = GitEnrich(db_sortinghat='opnfv_track_sh',
                         db_user='root', db_host='localhost',
                         json_projects_map=projects_file_path)

    # First load identities
    load_identities(commits, enricher)

    # Then enrich
    for commit in commits:
        enriched_items.append(enricher.get_rich_item(commit))

    os.unlink(projects_file_path)

    return enriched_items

def get_commits_from_gerrit(es, index_gerrit_raw, gerrit_numbers):
    # Get the gerrit reviews from ES and extract the commits sha
    commits_sha = []

    reviews = get_gerrit_reviews(es, index_gerrit_raw, gerrit_numbers)
    logger.info("Total gerrit track items found upstream: %i", len(reviews))

    for review in reviews:
        for patch in review['data']['patchSets']:
            if patch['revision'] not in commits_sha:
                commits_sha.append(patch['revision'])

    return commits_sha

if __name__ == '__main__':

    args = get_params()
    configure_logging(args.debug)

    logger.info("Importing track items from %s ", args.upstream_url)

    total = 0

    #
    # Gerrit Reviews
    #
    gerrit_uris = fetch_track_items(args.upstream_url, "Gerrit")
    gerrit_numbers = get_gerrit_numbers(gerrit_uris)
    logger.info("Total gerrit track items to be imported: %i", len(gerrit_numbers))
    enriched_items = enrich_gerrit_items(args.elastic_url_raw,
                                         args.index_gerrit_raw, gerrit_numbers,
                                         args.project)
    logger.info("Total gerrit track items enriched: %i", len(enriched_items))
    elastic = ElasticSearch(args.elastic_url_enrich, args.index_gerrit_enrich)
    total = elastic.bulk_upload(enriched_items, "uuid")

    #
    # Git Commits
    #
    commits_sha = get_commits_from_gerrit(args.elastic_url_raw,
                                          args.index_gerrit_raw, gerrit_numbers)
    logger.info("Total git track items to be imported: %i", len(commits_sha))
    enriched_items = enrich_git_items(args.elastic_url_raw,
                                      args.index_git_raw, commits_sha,
                                      args.project)
    logger.info("Total git track items enriched: %i", len(enriched_items))
    elastic = ElasticSearch(args.elastic_url_enrich, args.index_git_enrich)
    total = elastic.bulk_upload(enriched_items, "uuid")
