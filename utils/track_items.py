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

import requests

from grimoire_elk.arthur import load_identities
from grimoire_elk.elk.gerrit import GerritEnrich
from grimoire_elk.elk.git import GitEnrich
from grimoire_elk.elk.elastic import ElasticSearch

# Logging formats
LOG_FORMAT = "[%(asctime)s] - %(message)s"
DEBUG_LOG_FORMAT = "[%(asctime)s - %(name)s - %(levelname)s] - %(message)s"

OPNFV_UPSTREAM_FILE='https://git.opnfv.org/doctor/plain/UPSTREAM'
# numbers for already downloaded gerrit reviews fo test with
GERRIT_INDEX_RAW = 'gerrit_openstack'
GERRIT_INDEX_ENRICH = 'gerrit_openstack_enrich'
GERRIT_NUMBERS_TEST = [
    "242602",
    "437760",
    "442063",
    "442115",
    "442143",
    "438979",
    "439058",
    "442111",
    "441435",
    "442142"
]
# numbers for already downloaded GIT reviews fo test with
GIT_INDEX_RAW = 'git_openstack'
GIT_INDEX_ENRICH = 'git_openstack_enrich'
GIT_COMMITS_SHA_TEST = [
    "ceb6642235c3ad084954e55fc89fc53eaffe3889",
    "8bdb511230b679a847708ee3d63b5a53481348f5",
    "cd62577c66f76368ab3b54c5f8a93b7ae4fa53a6",
    "179e22065287b5b7197c158c691d193ee848f135",
    "0f670fb799a9df1174dc501d09610d506ffecba7",
    "f5ee1bc45d46c0439830b4410ad1e6bed51eedda",
    "0f03c25b556b6c285e14e702ef5904d013f2cdef",
    "cdbceeaee485a5095a59cc7f05470a440065832e",
    "fff8aad420967488c67f9402fbdb872c4c41e4e2",
    "83f8a20c82434fb4b90c2e0c30779a361f2c7944"
]



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
    r = requests.get(upstream_file_url)
    r.raise_for_status()
    lines = iter(r.text.split("\n"))
    for line in lines:
        if 'url: ' in line:
            ds = next(lines).split('system: ')[1].strip('\n')
            if ds == data_source:
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

    gerrit_uri = gerrit_uri.replace('#/c/','')
    origin = gerrit_uri.rsplit("/", 1)[0]

    return origin

def get_gerrit_numbers(gerrit_uris):
    # uuid to search the gerrit review in ElasticSearch
    numbers = []
    for gerrit_uri in gerrit_uris:
        gerrit_number = get_gerrit_number(gerrit_uri)
        numbers.append(gerrit_number)

    return numbers

def get_gerrit_reviews(es,  gerrit_numbers):
    # Get gerrit raw items
    query = {
          "query": {
            "terms" : { "data.number" : gerrit_numbers}
          }
        }

    r = requests.post(es + "/" + GERRIT_INDEX_RAW + "/_search?limit=10000",
                      data=json.dumps(query))
    r.raise_for_status()
    reviews_es = r.json()["hits"]["hits"]
    reviews = []
    for review in reviews_es:
        reviews.append(review['_source'])
    return reviews

def enrich_gerrit_items(es, gerrit_numbers):
    reviews = get_gerrit_reviews(es, gerrit_numbers)
    logging.info("Total gerrit track items to be enriched: %i", len(reviews))

    enriched_items = []
    enricher = GerritEnrich(db_sortinghat='opnfv_track_sh',
                            db_user='root', db_host='localhost')

    # First load identities
    load_identities(reviews, enricher)

    # Then enrich
    for review in reviews:
        enriched_items.append(enricher.get_rich_item(review))

    return enriched_items

def get_git_commits(es,  commits_sha):
    # Get gerrit raw items
    query = {
          "query": {
            "terms" : { "data.commit" : commits_sha}
          }
        }

    r = requests.post(es + "/" + GIT_INDEX_RAW + "/_search?limit=10000",
                      data=json.dumps(query))
    r.raise_for_status()
    commits_es = r.json()["hits"]["hits"]
    commits = []
    for commit in commits_es:
        commits.append(commit['_source'])
    return commits

def enrich_git_items(es, commits_sha):
    commits = get_git_commits(es, commits_sha)
    logging.info("Total git track items to be enriched: %i", len(commits))

    enriched_items = []
    enricher = GitEnrich(db_sortinghat='opnfv_track_sh',
                         db_user='root', db_host='localhost')

    # First load identities
    load_identities(commits, enricher)

    # Then enrich
    for commit in commits:
        enriched_items.append(enricher.get_rich_item(commit))

    return enriched_items

def get_commits_from_gerrit(es, gerrit_numbers):
    # Get the gerrit reviews from ES and extract the commits sha
    commits_sha = []

    reviews = get_gerrit_reviews(es, gerrit_numbers)
    logging.info("Total gerrit track items found upstream: %i", len(reviews))

    for review in reviews:
        for patch in review['data']['patchSets']:
            commits_sha.append(patch['revision'])

    return commits_sha

if __name__ == '__main__':

    args = get_params()
    configure_logging(args.debug)

    logging.info("Importing track items from %s ", args.upstream_url)

    total = 0

    #
    # Gerrit Reviews
    #
    gerrit_uris = fetch_track_items(args.upstream_url, "Gerrit")
    gerrit_numbers = get_gerrit_numbers(gerrit_uris)
    # TODO: testing with gerrit numbers already downloaded
    gerrit_numbers = GERRIT_NUMBERS_TEST
    logging.info("Total gerrit track items to be imported: %i", len(gerrit_numbers))
    enriched_items = enrich_gerrit_items(args.elastic_url_raw, gerrit_numbers)
    logging.info("Total gerrit track items enriched: %i", len(enriched_items))
    elastic = ElasticSearch(args.elastic_url_enrich, GERRIT_INDEX_ENRICH)
    total = elastic.bulk_upload(enriched_items, "uuid")

    #
    # Git Commits
    #
    commits_sha = get_commits_from_gerrit(args.elastic_url_raw, gerrit_numbers)
    # TODO: testing with git commits sha already downloaded
    commits_sha = GIT_COMMITS_SHA_TEST
    logging.info("Total git track items to be imported: %i", len(commits_sha))
    enriched_items = enrich_git_items(args.elastic_url_raw, commits_sha)
    logging.info("Total git track items enriched: %i", len(enriched_items))
    elastic = ElasticSearch(args.elastic_url_enrich, GIT_INDEX_ENRICH)
    total = elastic.bulk_upload(enriched_items, "uuid")
