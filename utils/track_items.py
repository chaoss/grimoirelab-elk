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

# Logging formats
LOG_FORMAT = "[%(asctime)s] - %(message)s"
DEBUG_LOG_FORMAT = "[%(asctime)s - %(name)s - %(levelname)s] - %(message)s"

OPNFV_UPSTREAM_FILE='https://git.opnfv.org/doctor/plain/UPSTREAM'
# numbers for already downloaded gerrit reviews fo test with
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
    args_parser.add_argument("-e", "--elastic-url", required=True,
                             help="ElasticSearch URL with raw indexes wich includes the items to track")
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

def get_commits_from_gerrit(es, gerrit_numbers):
    # Get the gerrit reviews from ES and extract the commits sha

    def get_query(numbers):
        query = {
              "query": {
                "terms" : { "data.number" : gerrit_numbers}
              }
            }
        return query


    numbers_found = 0
    commits_sha = []

    r = requests.post(es + "/" + es_index + "/_search",
                      data=json.dumps(get_query(gerrit_numbers)))
    r.raise_for_status()
    numbers_found += r.json()["hits"]["total"]

    for review in r.json()["hits"]["hits"]:
        for patch in review['_source']['data']['patchSets']:
            commits_sha.append(patch['revision'])

    logging.info("Total gerrit track items found upstream: %i", numbers_found)

    return commits_sha

if __name__ == '__main__':

    args = get_params()
    configure_logging(args.debug)

    logging.info("Importing track items from %s ", args.upstream_url)

    es_index = "gerrit_openstack"

    total = 0

    gerrit_uris = fetch_track_items(args.upstream_url, "Gerrit")
    gerrit_numbers = get_gerrit_numbers(gerrit_uris)

    # TODO: testing with gerrit uuids already downloaded
    gerrit_numbers = GERRIT_NUMBERS_TEST
    logging.info("Total gerrit track items to be imported: %i", len(gerrit_numbers))

    commits_sha = get_commits_from_gerrit(args.elastic_url, gerrit_numbers)
    logging.info("Total commit track items to be imported: %i", len(commits_sha))


    # Now we need to enrich all gerrit and commits track raw items and publish
    # them to the OPNFV ES enriched indexes for gerrit and git

    # total = elastic.bulk_upload(tweets, "id_str")
