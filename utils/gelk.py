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

from grimoire.ocean.bugzilla import BugzillaOcean
from grimoire.ocean.gerrit import GerritOcean
from grimoire.ocean.github import GitHubOcean
from grimoire.ocean.elastic import ElasticOcean


from perceval.backends.bugzilla import Bugzilla
from perceval.backends.github import GitHub
from perceval.backends.gerrit import Gerrit

if __name__ == '__main__':

    backends = [Bugzilla, GitHub, Gerrit]  # Registry
    backends_ocean = [GitHubOcean]  # Registry

    parser = argparse.ArgumentParser()
    ElasticOcean.add_params(parser)

    subparsers = parser.add_subparsers(dest='backend',
                                       help='perceval backend')

    for backend in backends:
        name = backend.get_name()
        subparser = subparsers.add_parser(name, help='gelk %s -h' % name)
        backend.add_params(subparser)

    args = parser.parse_args()

    app_init = datetime.now()

    backend_name = args.backend

    if not backend_name:
        parser.print_help()
        sys.exit(0)

    if 'debug' in args and args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


    if backend_name == "bugzilla":
        backend = Bugzilla(args.url, args.nissues, args.detail, args.cache)
        enrich_backend = BugzillaElastic(backend)
        ocean_backend = BugzillaOcean(backend, args.cache, not args.no_incremental)
    elif backend_name == "github":
        backend = GitHub(args.owner, args.repository, args.token, args.cache)
        # Ocean enrich
        enrich_backend = GitHubElastic(backend)
        # Ocean
        ocean_backend = GitHubOcean(backend, args.cache, not args.no_incremental)

    elif backend_name == "gerrit":
        backend = Gerrit(args.user, args.url, args.nreviews, args.cache)
        enrich_backend = GerritElastic(backend, args.sortinghat_db,
                                       args.projects_grimoirelib_db,
                                       args.gerrit_grimoirelib_db)
        ocean_backend = GerritOcean(backend, args.cache, not args.no_incremental)

    es_index = backend.get_name() + "_" + backend.get_id()

    clean = args.no_incremental

    if args.cache:
        clean = True


    try:
        # Ocean
        state_index = es_index+"_state"
        elastic_state = ElasticSearch(args.elastic_host, args.elastic_port,
                                      state_index,
                                      ocean_backend.get_elastic_mappings(),
                                      clean)

        # Enriched ocean
        elastic = ElasticSearch(args.elastic_host, args.elastic_port,
                                es_index,
                                enrich_backend.get_elastic_mappings(),
                                clean)

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    ocean_backend.set_elastic(elastic_state)
    enrich_backend.set_elastic(elastic)

    try:
        # First feed the item in Ocean to use it later
        ocean_backend.feed()


        if backend_name == "bugzilla":
            if args.detail == "list":
                enrich_backend.issues_list_to_es(ocean_backend)
            else:
                enrich_backend.issues_to_es(ocean_backend)

        elif backend_name == "github":
            GitHub.users = enrich_backend.users_from_es()

            issues_prs_count = 1
            pulls = []

            # Now use the items in Ocean iterator
            for pr in ocean_backend:
                if len(pulls) >= elastic.max_items_bulk:
                    enrich_backend.pullrequests_to_es(pulls)
                    pulls = []
                pulls.append(pr)
                issues_prs_count += 1
            enrich_backend.pullrequests_to_es(pulls)

            # logging.info("Total Pull Requests " + str(prs_count))
            logging.info("Total Issues Pull Requests " + str(issues_prs_count))

        elif backend_name == "gerrit":
            logging.info("Adding data to %s" % (ocean_backend.elastic.index_url))
            logging.info("Adding enrichment data to %s" % (enrich_backend.elastic.index_url))

            for review in ocean_backend:
                enrich_backend.fetch_events(review)


    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        logging.debug("Recovering cache")
        backend.cache.recover()
        sys.exit(0)


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
