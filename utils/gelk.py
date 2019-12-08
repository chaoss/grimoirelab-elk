#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
#

import argparse
import logging
from datetime import datetime
from os import sys

from perceval.backends.bugzilla import Bugzilla
from perceval.backends.gerrit import Gerrit
from perceval.backends.github import GitHub

from grimoire_elk.errors import ElasticError
from grimoire_elk.elastic import ElasticSearch
from grimoire_elk.enriched.bugzilla import BugzillaEnrich
from grimoire_elk.enriched.gerrit import GerritEnrich
from grimoire_elk.enriched.github import GitHubEnrich
from grimoire_elk.enriched.sortinghat_gelk import SortingHat
from grimoire_elk.raw.bugzilla import BugzillaOcean
from grimoire_elk.raw.elastic import ElasticOcean
from grimoire_elk.raw.gerrit import GerritOcean
from grimoire_elk.raw.github import GitHubOcean


def get_connector_from_name(name, connectors):
    found = None

    for connector in connectors:
        backend = connector[0]
        if backend.get_name() == name:
            found = connector

    return found


if __name__ == '__main__':
    """Gelk: perceval2ocean and ocean2kibana"""

    connectors = [[Bugzilla, BugzillaOcean, BugzillaEnrich],
                  [GitHub, GitHubOcean, GitHubEnrich],
                  [Gerrit, GerritOcean, GerritEnrich]]  # Will come from Registry

    parser = argparse.ArgumentParser()
    ElasticOcean.add_params(parser)

    subparsers = parser.add_subparsers(dest='backend',
                                       help='perceval backend')

    for connector in connectors:
        name = connector[0].get_name()
        subparser = subparsers.add_parser(name, help='gelk %s -h' % name)
        # We need params for feed
        connector[0].add_params(subparser)

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

    connector = get_connector_from_name(backend_name, connectors)
    backend = connector[0](**vars(args))
    ocean_backend = connector[1](backend, **vars(args))
    enrich_backend = connector[2](backend, **vars(args))

    es_index = backend.get_name() + "_" + backend.get_id()

    clean = args.no_incremental

    if args.cache:
        clean = True

    try:
        # Ocean
        elastic_state = ElasticSearch(args.elastic_url,
                                      es_index,
                                      ocean_backend.get_elastic_mappings(),
                                      clean)

        # Enriched ocean
        enrich_index = es_index + "_enrich"
        elastic = ElasticSearch(args.elastic_url,
                                enrich_index,
                                enrich_backend.get_elastic_mappings(),
                                clean)

    except ElasticError:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    ocean_backend.set_elastic(elastic_state)
    enrich_backend.set_elastic(elastic)

    try:
        # First feed the item in Ocean to use it later
        logging.info("Adding data to %s" % (ocean_backend.elastic.index_url))
        ocean_backend.feed()

        if backend_name == "github":
            GitHub.users = enrich_backend.users_from_es()

        logging.info("Adding enrichment data to %s" %
                     (enrich_backend.elastic.index_url))

        items = []
        new_identities = []
        items_count = 0

        for item in ocean_backend:
            # print("%s %s" % (item['url'], item['lastUpdated_date']))
            if len(items) >= elastic.max_items_bulk:
                enrich_backend.enrich_items(items)
                items = []
            items.append(item)
            # Get identities from new items to be added to SortingHat
            identities = ocean_backend.get_identities(item)

            if not identities:
                identities = []

            for identity in identities:
                if identity not in new_identities:
                    new_identities.append(identity)
            items_count += 1
        enrich_backend.enrich_items(items)

        logging.info("Total items enriched %i " % items_count)

        logging.info("Total new identities to be checked %i" % len(new_identities))

        merged_identities = SortingHat.add_identities(new_identities, backend_name)

        # Redo enrich for items with new merged identities

    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        logging.debug("Recovering cache")
        backend.cache.recover()
        sys.exit(0)

    total_time_min = (datetime.now() - app_init).total_seconds() / 60

    logging.info("Finished in %.2f min" % (total_time_min))
