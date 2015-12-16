#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Grimoire general utils
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
import sys

from grimoire.ocean.elastic import ElasticOcean

# Connectors for Ocean
from grimoire.ocean.bugzilla import BugzillaOcean
from grimoire.ocean.gerrit import GerritOcean
from grimoire.ocean.github import GitHubOcean

# Connectors for Perceval
from perceval.backends.bugzilla import Bugzilla
from perceval.backends.github import GitHub
from perceval.backends.gerrit import Gerrit

from grimoire.elk.elastic import ElasticSearch
from grimoire.elk.elastic import ElasticConnectException

def get_connector_from_name(name, connectors):
    found = None

    for connector in connectors:
        backend = connector[0]
        if backend.get_name() == name:
            found = connector

    return found

def get_connectors():

    return [[Bugzilla, BugzillaOcean],
            [GitHub, GitHubOcean],
            [Gerrit, GerritOcean]]  # Will come from Registry

def get_elastic(url, es_index, clean = None, ocean_backend = None):

    mapping = None

    if ocean_backend:
        mapping = ocean_backend.get_elastic_mappings()

    try:
        ocean_index = es_index
        elastic_ocean = ElasticSearch(url, ocean_index, mapping, clean)

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    return elastic_ocean

def config_logging(debug):

    if debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

def get_params(connectors):
    ''' Get params definition from ElasticOcean and from all the backends '''
    parser = argparse.ArgumentParser()
    ElasticOcean.add_params(parser)

    subparsers = parser.add_subparsers(dest='backend',
                                       help='perceval backend')

    for connector in connectors:
        backend = connector[0]
        name = backend.get_name()
        subparser = subparsers.add_parser(name, help='p2o %s -h' % name)
        backend.add_params(subparser)

    # And now a specific param to do the update until process termination
    parser.add_argument("--loop",  action='store_true',
                        help="loop the ocean update until process termination")
    parser.add_argument("--redis",  default="redis",
                        help="url for the redis server")

    args = parser.parse_args()

    return args
