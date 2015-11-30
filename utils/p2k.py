#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Perceval to Kibana Publisher
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
import json
import logging
from os import sys
import requests

from grimoire.elk.elastic import ElasticSearch
from grimoire.elk.elastic import ElasticConnectException

# Connectors for Ocean
from grimoire.ocean.bugzilla import BugzillaOcean
from grimoire.ocean.gerrit import GerritOcean
from grimoire.ocean.github import GitHubOcean
from grimoire.ocean.elastic import ElasticOcean
from grimoire.ocean.conf import ConfOcean

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


def feed_backend(url, params, connectors, clean):
    ''' Feed Ocean with backend data '''

    backend = None
    backend_name = params['backend']
    repo = {}    # repository data to be stored in conf
    repo['params'] = params
    es_index = None

    connector = get_connector_from_name(backend_name, connectors)
    if not connector:
        logging.error("Cant find %s backend" % (backend_name))
        sys.exit(1)

    try:
        backend = connector[0](**params)
        ocean_backend = connector[1](backend, **params)

        logging.info("Feeding Ocean from %s (%s)" % (backend.get_name(),
                                                     backend.get_id()))

        es_index = backend.get_name() + "_" + backend.get_id()
        elastic_ocean = get_elastic(url, es_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)

        ConfOcean.set_elastic(elastic_ocean)

        ocean_backend.feed()
    except Exception as ex:
        if backend:
            logging.error("Error feeding ocean from %s (%s): %s" %
                          (backend.get_name(), backend.get_id()), ex)
        else:
            logging.error("Error feeding ocean %s" % ex)

        repo['success'] = False
        repo['error'] = ex
    else:
        repo['success'] = True

    repo['repo_update'] = datetime.now().isoformat()

    if es_index:
        ConfOcean.add_repo(es_index, repo)
    else:
        logging.debug("Repository not added to Ocean because errors.")
        logging.debug(params)

    logging.info("Done %s " % (backend_name))

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


    args = parser.parse_args()

    return args

def get_connectors():

    return [[Bugzilla, BugzillaOcean],
            [GitHub, GitHubOcean],
            [Gerrit, GerritOcean]]  # Will come from Registry

def create_index_pattern(params, connectors):
    logging.debug("Generating index pattern")

    backend_name = params['backend']
    url = params['elastic_url']

    connector = get_connector_from_name(backend_name, connectors)
    if not connector:
        logging.error("Cant find %s backend" % (backend_name))
        sys.exit(1)


    backend = connector[0](**params)
    ds = backend.get_name()
    ds_id = ds+"_"+backend.get_id()
    # ds_id = ds+"_"+"elastic_kibana"

    # Get the index pattern template
    es_index = "/.kibana"
    elastic = get_elastic(url, es_index)


    index_template = elastic.index_url+"/index-pattern"
    index_template_search = index_template+"/_search"

    query = '''
        {"query": {"prefix": {"_id": {"value": "%s_" }}}}
    ''' % (ds)

    r = requests.post(index_template_search, data = query)

    index_pattern_template =r.json()['hits']['hits']

    if len(index_pattern_template) == 0:
        logging.error("Can not find template for data source: %s" % (ds))
        sys.exit(0)

    # All index pattern can be used as template. Use the first one.
    index_pattern = index_pattern_template[0]["_source"]
    index_pattern['title'] = ds_id

    # fields = json.loads(index_pattern['fields'])
    # import pprint
    # pprint.pprint(fields)
    # raise

    url = index_template+"/"+ds_id

    r = requests.post(url, data = json.dumps(index_pattern))


def create_dashboard(args):
    logging.debug("Generating dashboard")

def create_visualizations(args):
    logging.debug("Generating visualization")



if __name__ == '__main__':

    app_init = datetime.now()

    connectors = get_connectors() 

    args = get_params(connectors)

    config_logging(args.debug)

    clean = args.no_incremental
    if args.cache:
        clean = True

    try:
        feed_backend(args.elastic_url, vars(args), connectors, clean)
        # Time to create Kibana dashbnoard
        logging.info("Generating Kibana dashboard")
        create_index_pattern(vars(args), connectors)
        create_dashboard(args)
        create_visualizations(args)

    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        sys.exit(0)


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
