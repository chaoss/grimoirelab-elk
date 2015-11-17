#!/usr/bin/python3
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
import logging
from os import sys


from grimoire.elk.elastic import ElasticSearch, ElasticConnectException
from grimoire.elk.github import GitHubElastic
from perceval.backends.github import GitHub



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    GitHub.add_params(parser)
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)

    es_index_github = "github_%s_%s" % (args.owner, args.repository)
    es_mappings = GitHubElastic.get_elastic_mappings()

    github = GitHub(args.owner, args.repository, args.token, args.cache,
                    args.no_incremental)

    try:
        elastic = ElasticSearch(args.elastic_host,
                                args.elastic_port,
                                es_index_github, es_mappings, args.no_incremental)
    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)


    egithub = GitHubElastic(elastic, github)
    GitHub.users = egithub.usersFromES()

    issues_prs_count = 1
    pulls = []
    for pr in github.fetch():
        pulls.append(pr)
        issues_prs_count += 1

    egithub.pullrequests2ES(pulls)

    # logging.info("Total Pull Requests " + str(prs_count))
    logging.info("Total Issues Pull Requests " + str(issues_prs_count))
