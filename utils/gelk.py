#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Gelk: perceval2ocean and ocean2kibana
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
from os import sys

from grimoire.elk.elastic import ElasticSearch
from grimoire.elk.elastic import ElasticConnectException

from grimoire.elk.bugzilla import BugzillaElastic
from grimoire.elk.gerrit import GerritElastic
from grimoire.elk.github import GitHubElastic

from perceval.backends.bugzilla import Bugzilla
from perceval.backends.github import GitHub
from perceval.backends.gerrit import Gerrit

if __name__ == '__main__':

    backends = [Bugzilla, GitHub, Gerrit]  # Registry

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest='backend', help='perceval backend')

    for backend in backends:
        name = backend.get_name()
        subparser = subparsers.add_parser(name, help='gelk %s -h' % name)
        backend.add_params(subparser)

    args = parser.parse_args()

    app_init = datetime.now()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    backend_name = args.backend

    if backend_name == "bugzilla":
        backend = Bugzilla(args.url, args.nissues, args.detail,
                            not args.no_incremental, args.cache)
        ebackend = BugzillaElastic(backend)
    elif backend_name == "github":
        backend = GitHub(args.owner, args.repository, args.token, args.cache,
                        not args.no_incremental)
        ebackend = GitHubElastic(backend)
    elif backend_name == "gerrit":
        backend = Gerrit(args.user, args.url, args.nreviews, args.cache,
                        not args.no_incremental)
        ebackend = GerritElastic(backend, args.sortinghat_db,
                                args.projects_grimoirelib_db,
                                args.gerrit_grimoirelib_db)

    es_index = backend.get_name() + backend.get_id()

    clean = args.no_incremental

    if args.cache:
        clean = True


    try:
        state_index = es_index+"_state"
        elastic_state = ElasticSearch(args.elastic_host, args.elastic_port,
                                      state_index, backend.get_elastic_mappings(),
                                      clean)

        elastic = ElasticSearch(args.elastic_host, args.elastic_port,
                                es_index, ebackend.get_elastic_mappings(),
                                clean)

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    backend.set_elastic(elastic_state)
    ebackend.set_elastic(elastic)

    try:

        if backend_name == "bugzilla":
            if args.detail == "list":
                ebackend.issues_list_to_es()
            else:
                ebackend.issues_to_es()
        elif backend_name == "github":
            GitHub.users = ebackend.users_from_es()

            issues_prs_count = 1
            pulls = []

            for pr in backend.fetch():
                pulls.append(pr)
                issues_prs_count += 1

            ebackend.pullrequests_to_es(pulls)
            # logging.info("Total Pull Requests " + str(prs_count))
            logging.info("Total Issues Pull Requests " + str(issues_prs_count))

        elif backend_name == "gerrit":
            logging.info("Adding enrichment data to %s" % (ebackend.elastic.index_url))
            for review in backend.fetch():
                ebackend.fetch_events(review)


    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        logging.debug("Recovering cache")
        backend.cache.recover()
        sys.exit(0)


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
