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
from dateutil import parser
import logging
import sys

from grimoire.ocean.elastic import ElasticOcean

# Connectors for Ocean
from grimoire.ocean.bugzilla import BugzillaOcean
from grimoire.ocean.gerrit import GerritOcean
from grimoire.ocean.github import GitHubOcean
from grimoire.ocean.git import GitOcean
from grimoire.ocean.mbox import MBoxOcean
from grimoire.ocean.stackexchange import StackExchangeOcean
from grimoire.ocean.jira import JiraOcean
from grimoire.ocean.jenkins import JenkinsOcean

# Connectors for EnrichOcean
from grimoire.elk.bugzilla import BugzillaEnrich
from grimoire.elk.gerrit import GerritEnrich
from grimoire.elk.github import GitHubEnrich
from grimoire.elk.git import GitEnrich
from grimoire.elk.mbox import MBoxEnrich
from grimoire.elk.stackexchange import StackExchangeEnrich
from grimoire.elk.jira import JiraEnrich
from grimoire.elk.jenkins import JenkinsEnrich


# Connectors for Perceval
from perceval.backends.bugzilla import Bugzilla, BugzillaCommand
from perceval.backends.github import GitHub, GitHubCommand
from perceval.backends.gerrit import Gerrit, GerritCommand
from perceval.backends.git import Git, GitCommand
from perceval.backends.mbox import MBox, MBoxCommand
from perceval.backends.stackexchange import StackExchange, StackExchangeCommand
from perceval.backends.jira import Jira, JiraCommand
from perceval.backends.jenkins import Jenkins, JenkinsCommand

from grimoire.elk.elastic import ElasticSearch
from grimoire.elk.elastic import ElasticConnectException

def get_connector_from_name(name):
    found = None
    connectors = get_connectors()

    for cname in connectors:
        if cname == name:
            found = connectors[cname]

    return found

def get_connector_name(cls):
    found = None
    connectors = get_connectors()

    for cname in connectors:
        for con in connectors[cname]:
            if cls == con:
                found = cname
    return found

def get_connectors():

    return {"bugzilla":[Bugzilla, BugzillaOcean, BugzillaEnrich, BugzillaCommand],
            "github":[GitHub, GitHubOcean, GitHubEnrich, GitHubCommand],
            "gerrit":[Gerrit, GerritOcean, GerritEnrich, GerritCommand],
            "git":[Git, GitOcean, GitEnrich, GitCommand],
            "mbox":[Git, MBoxOcean, MBoxEnrich, MBoxCommand],
            "stackexchange":[StackExchange, StackExchangeOcean,
                             StackExchangeEnrich, StackExchangeCommand],
            "jira":[Jira, JiraOcean, JiraEnrich, JiraCommand],
            "jenkins":[Jenkins, JenkinsOcean, JenkinsEnrich, JenkinsCommand]
            }  # Will come from Registry

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

ARTHUR_USAGE_MSG = ''
ARTHUR_DESC_MSG = ''
ARTHUR_EPILOG_MSG = ''

def get_params_parser():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(usage=ARTHUR_USAGE_MSG,
                                     description=ARTHUR_DESC_MSG,
                                     epilog=ARTHUR_EPILOG_MSG,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     add_help=False)

    ElasticOcean.add_params(parser)

    parser.add_argument('-h', '--help', action='help',
                       help=argparse.SUPPRESS)
    parser.add_argument('-g', '--debug', dest='debug',
                        action='store_true',
                        help=argparse.SUPPRESS)

    parser.add_argument("--no_incremental",  action='store_true',
                        help="don't use last state for data source")
    parser.add_argument("--fetch_cache",  action='store_true',
                        help="Use cache for item retrieval")

    parser.add_argument("--loop",  action='store_true',
                        help="loop the ocean update until process termination")
    parser.add_argument("--redis",  default="redis",
                        help="url for the redis server")
    parser.add_argument("--enrich",  action='store_true',
                        help="Enrich items after retrieving")
    parser.add_argument("--enrich_only",  action='store_true',
                        help="Only enrich items")
    parser.add_argument('--index', help="Ocean index name")
    parser.add_argument('--index-enrich', dest="index_enrich", help="Ocean enriched index name")
    parser.add_argument('--db-projects-map', help="Projects Mapping DB")
    parser.add_argument('--project', help="Project for the repository (origin)")
    parser.add_argument('--db-sortinghat', help="SortingHat DB")
    parser.add_argument('backend', help=argparse.SUPPRESS)
    parser.add_argument('backend_args', nargs=argparse.REMAINDER,
                        help=argparse.SUPPRESS)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    return parser


def get_params():
    ''' Get params definition from ElasticOcean and from all the backends '''

    parser = get_params_parser()
    args = parser.parse_args()

    return args

def get_time_diff_days(start_txt, end_txt):
    ''' Number of days between two days  '''

    if start_txt is None or end_txt is None:
        return None

    start = parser.parse(start_txt)
    end = parser.parse(end_txt)

    seconds_day = float(60*60*24)
    diff_days = \
        (end-start).total_seconds() / seconds_day
    diff_days = float('%.2f' % diff_days)

    return diff_days
