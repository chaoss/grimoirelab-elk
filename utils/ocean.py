#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Ocean Manager Tool
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

import argparse
from datetime import datetime
import logging
import requests
import sys

from grimoire.elk.elastic import ElasticSearch, ElasticConnectException, ElasticWriteException
from grimoire.ocean.elastic import ElasticOcean
from grimoire.ocean.conf import ConfOcean

def get_elastic():

    try:
        ocean_index = ConfOcean.get_index()
        elastic_ocean = ElasticSearch(args.elastic_url, ocean_index)

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    except ElasticWriteException:
        logging.error("Can't write to Elastic Search.")
        sys.exit(1)


    return elastic_ocean


def get_params():
    ''' Get params definition from ElasticOcean '''
    parser = argparse.ArgumentParser()
    ElasticOcean.add_params(parser)

    # Commands supported

    parser.add_argument("-l", "--list",  action='store_true',
                        help="Lists repositories")
    parser.add_argument("-r", "--remove",
                        help="Remove a repository")
#     parser.add_argument("--rename",
#                         help="Rename a repository")

    args = parser.parse_args()

    return args

def list_repos_ids():
    logging.debug("Listing repos ids")
    elastic = get_elastic()
    ConfOcean.set_elastic(elastic)

    for repo_id in ConfOcean.get_repos_ids():
        print(repo_id)


def list_repos():
    logging.debug("Listing repos")
    elastic = get_elastic()
    ConfOcean.set_elastic(elastic)

    for repo_id in ConfOcean.get_repos_ids():
        elastic = get_elastic()
        url = elastic.index_url + "/repos/" + repo_id
        r = requests.get(url)
        repo = r.json()['_source']
        print ("%s %s %s" % (repo_id, repo['repo_update'], repo['success']))


def remove_repo(repo_id):
    logging.info("Removing repo: %s" % (repo_id))
    elastic = get_elastic()
    url = elastic.index_url + "/repos/" + repo_id
    r = requests.delete(url)

    if r.status_code == 200:
        logging.info("Done")
    else:
        logging.error("Can not remove %s (%i)" % (repo_id, r.status_code))

def config_logging(debug):

    if debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


if __name__ == '__main__':

    app_init = datetime.now()

    args = get_params()

    config_logging(args.debug)

    if args.list:
        list_repos()
    elif args.remove:
        remove_repo(args.remove)

