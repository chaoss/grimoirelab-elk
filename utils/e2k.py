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

from os import sys

from grimoire_elk.raw.elastic import ElasticOcean
from grimoire_elk.panels import create_dashboard
from grimoire_elk.utils import config_logging


def get_params_parser_create_dash():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(usage="usage: e2k.py [options]",
                                     description="Create a Kibana dashboard from a template")

    ElasticOcean.add_params(parser)

    parser.add_argument("-d", "--dashboard", help="dashboard to be used as template")
    parser.add_argument("-i", "--index", help="enriched index to be used as data source")
    parser.add_argument("--kibana", dest="kibana_index", default=".kibana",
                        help="Kibana index name (.kibana default)")
    parser.add_argument('-g', '--debug', dest='debug', action='store_true')

    return parser


def get_params():
    parser = get_params_parser_create_dash()
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    """Enriched to Kibana Publisher"""

    args = get_params()

    config_logging(args.debug)

    # TODO: Use param to build a real URL if possible
    kibana_host = "http://localhost:5601"

    try:
        url = create_dashboard(args.elastic_url, args.dashboard, args.index,
                               kibana_host, args.kibana_index)
    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        sys.exit(0)

    logging.info("Kibana dashboard generated %s", url)
