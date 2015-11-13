#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# GitHub Pull Requests loader for Elastic Search
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
# TODO: Just a playing script yet.
#     - Use the _bulk API from ES to improve indexing

import argparse
from datetime import datetime
import logging
from os import sys
import requests

from grimoire.elk.elastic import ElasticSearch, ElasticConnectException
from grimoire.elk.github import GitHubElastic
from perceval.backends.github import GitHub


def parse_args ():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--owner", required = True,
                        help = "github owner")
    parser.add_argument("-r", "--repository", required = True,
                        help = "github repository")
    parser.add_argument("-t", "--token", required = True,
                        help = "github access token")
    parser.add_argument("-e", "--elastic_host",  default = "127.0.0.1",
                        help = "Host with Elastic Search" + \
                        "(default: 127.0.0.1)")
    parser.add_argument("--elastic_port",  default = "9200",
                        help = "Elastic Search port " + \
                        "(default: 9200)")
    parser.add_argument("--no_history",  action='store_true',
                        help="don't use history for repository")
    parser.add_argument("--cache",  action='store_true',
                        help="Use perseval cache")
    parser.add_argument("--debug",  action='store_true',
                        help="Increase logging to debug")



    args = parser.parse_args()
    return args


if __name__ == '__main__':

    args = parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)

    github_owner = args.owner
    github_repo = args.repository
    auth_token = args.token

    es_index_github = "github_%s_%s" % (github_owner, github_repo)
    es_mappings = GitHubElastic.get_elastic_mappings()

    github = GitHub(args.owner, args.repository, args.token, args.cache,
                    args.no_history)

    try:
        elastic = ElasticSearch(args.elastic_host,
                                args.elastic_port,
                                es_index_github, es_mappings, args.no_history)
    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)


    egithub = GitHubElastic(elastic, github)
    GitHub.users = egithub.usersFromES()

    # prs_count = getPullRequests(url_pulls+url_params)
    issues_prs_count = 0
    for pr in github.fetch():
        issues_prs_count += 1
        print (pr)


    GitHubElastic.usersToES(GitHub.users)  # cache users in ES
    GitHubElastic.geoLocationsToES()

    # logging.info("Total Pull Requests " + str(prs_count))
    logging.info("Total Issues Pull Requests " + str(issues_prs_count))