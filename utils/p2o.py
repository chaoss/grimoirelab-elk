#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Perceval2Ocean tool
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

# Connectors for Ocean
from grimoire.ocean.bugzilla import BugzillaOcean
from grimoire.ocean.gerrit import GerritOcean
from grimoire.ocean.github import GitHubOcean
from grimoire.ocean.elastic import ElasticOcean

# Connectors for Perceval
from perceval.backends.bugzilla import Bugzilla
from perceval.backends.github import GitHub
from perceval.backends.gerrit import Gerrit

def get_connector_from_name(name, connectors):
    found = None

    for connector in connectors:
        backend = connector[0]
        if backend.get_name() == name:
            found = connector

    return found

if __name__ == '__main__':

    connectors = [[Bugzilla, BugzillaOcean],
                  [GitHub, GitHubOcean],
                  [Gerrit, GerritOcean]]  # Will come from Registry

    parser = argparse.ArgumentParser()
    ElasticOcean.add_params(parser)

    subparsers = parser.add_subparsers(dest='backend',
                                       help='perceval backend')

    for connector in connectors:
        backend = connector[0]
        name = backend.get_name()
        subparser = subparsers.add_parser(name, help='p2o %s -h' % name)
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

    connector = get_connector_from_name(backend_name, connectors)
    backend = connector[0](**vars(args))
    ocean_backend = connector[1](backend, **vars(args))

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

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    ocean_backend.set_elastic(elastic_state)

    try:
        logging.info("Feeding Ocean from %s" % (backend.get_name()))

        ocean_backend.feed()

        logging.info("Done")


    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        sys.exit(0)


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
