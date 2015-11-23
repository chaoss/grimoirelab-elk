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
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    es_index_github = "github_%s_%s" % (args.owner, args.repository)

    github = GitHub(args.owner, args.repository, args.token, args.cache,
                    not args.no_incremental)

    egithub = GitHubElastic(github)

    clean = args.no_incremental

    if args.cache:
        clean = True

    try:
        state_index = es_index_github+"_state"
        elastic_state = ElasticSearch(args.elastic_host, args.elastic_port,
                                      state_index, github.get_elastic_mappings(),
                                      clean)

        elastic = ElasticSearch(args.elastic_host, args.elastic_port,
                                es_index_github, egithub.get_elastic_mappings(),
                                clean)

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    github.set_elastic(elastic_state)
    egithub.set_elastic(elastic)

    GitHub.users = egithub.users_from_es()

    issues_prs_count = 1
    pulls = []

    try:
        for pr in github.fetch():
            pulls.append(pr)
            issues_prs_count += 1

        egithub.pullrequests_to_es(pulls)

        # logging.info("Total Pull Requests " + str(prs_count))
        logging.info("Total Issues Pull Requests " + str(issues_prs_count))
    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        logging.debug("Recovering cache")
        github.cache.recover()
        sys.exit(0)
