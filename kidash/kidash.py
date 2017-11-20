#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Import and Export Kibana dashboards
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
import os
import sys

from grimoire_elk.ocean.elastic import ElasticOcean
from grimoire_elk.utils import config_logging
from grimoire_elk.panels import import_dashboard, export_dashboard, list_dashboards


def get_params_parser_create_dash():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(usage="usage: kidash [options]",
                                     description="Import or Export a Kibana Dashboard")

    ElasticOcean.add_params(parser)

    parser.add_argument("--dashboard", help="Kibana dashboard id to export")
    parser.add_argument("--export", dest="export_file", help="file with the dashboard exported")
    parser.add_argument("--import", dest="import_file", help="file with the dashboard to be imported")
    parser.add_argument("--kibana", dest="kibana_index", default=".kibana", help="Kibana index name (.kibana default)")
    parser.add_argument("--list", action='store_true', help="list available dashboards")
    parser.add_argument('-g', '--debug', dest='debug', action='store_true')
    parser.add_argument("--data-sources", nargs='+', dest="data_sources", help="Data sources to be included")

    return parser

def get_params():
    parser = get_params_parser_create_dash()
    args = parser.parse_args()

    if not (args.export_file or args.import_file or args.list):
        parser.error("--export or --import or --list needed")
    else:
        if args.export_file and not args.dashboard:
            parser.error("--export needs --dashboard")
    return args


if __name__ == '__main__':

    ARGS = get_params()

    config_logging(ARGS.debug)

    if ARGS.import_file:
        import_dashboard(ARGS.elastic_url, ARGS.import_file, ARGS.kibana_index, ARGS.data_sources)
    elif ARGS.export_file:
        if os.path.isfile(ARGS.export_file):
            logging.info("%s exists. Remove it before running.", ARGS.export_file)
            sys.exit(0)
        export_dashboard(ARGS.elastic_url, ARGS.dashboard, ARGS.export_file, ARGS.kibana_index)
    elif ARGS.list:
        list_dashboards(ARGS.elastic_url, ARGS.kibana_index)
