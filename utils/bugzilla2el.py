#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Bugzilla tickets for Elastic Search
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

from perceval.backends.bugzilla import Bugzilla

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user",
                        help="Bugzilla user")
    parser.add_argument("--password",
                        help="Bugzilla user password")
    parser.add_argument("-d", "--delay", default="1",
                        help="delay between requests in seconds (1s default)")
    parser.add_argument("-u", "--url", required=True,
                        help="Bugzilla url")
    parser.add_argument("-e", "--elastic_host",  default="127.0.0.1",
                        help="Host with elastic search" +
                        "(default: 127.0.0.1)")
    parser.add_argument("--elastic_port",  default="9200",
                        help="elastic search port " +
                        "(default: 9200)")
    parser.add_argument("--no_history",  action='store_true',
                        help="don't use history for repository")
    parser.add_argument("--detail",  default="change",
                        help="list, issue or change (default) detail")
    parser.add_argument("--nissues",  default=200, type=int,
                        help="Number of XML issues to get per query")
    parser.add_argument("--cache",  action='store_true',
                        help="Use perseval cache")
    parser.add_argument("--debug",  action='store_true',
                        help="Increase logging to debug")


    args = parser.parse_args()
    return args


if __name__ == '__main__':

    args = parse_args()

    app_init = datetime.now()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)



    bugzilla = Bugzilla(args.url, args.nissues, args.detail,
                        not args.no_history, args.cache)

    es_index_bugzilla = "bugzilla_" + bugzilla.get_id()
    es_mappings = BugzillaElastic.get_elastic_mappings()

    try:
        elastic = ElasticSearch(args.elastic_host,
                                args.elastic_port,
                                es_index_bugzilla, es_mappings, args.no_history)
    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    ebugzilla = BugzillaElastic(bugzilla, elastic)


    if args.detail == "list":
        ebugzilla.issues_list_to_es()
    else:
        ebugzilla.issues_to_es()


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
