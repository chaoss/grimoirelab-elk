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


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    Bugzilla.add_params(parser)
    args = parser.parse_args()

    app_init = datetime.now()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)


    bugzilla = Bugzilla(args.url, args.nissues, args.detail,
                        not args.no_incremental, args.cache)
    ebugzilla = BugzillaElastic(bugzilla)

    es_index_bugzilla = "bugzilla_" + bugzilla.get_id()

    clean = args.no_incremental

    if args.cache:
        clean = True


    try:
        state_index = es_index_bugzilla+"_state"
        elastic_state = ElasticSearch(args.elastic_host, args.elastic_port,
                                      state_index, bugzilla.get_elastic_mappings(),
                                      clean)

        elastic = ElasticSearch(args.elastic_host, args.elastic_port,
                                es_index_bugzilla, ebugzilla.get_elastic_mappings(),
                                clean)

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    bugzilla.set_elastic(elastic_state)
    ebugzilla.set_elastic(elastic)

    if args.detail == "list":
        ebugzilla.issues_list_to_es()
    else:
        ebugzilla.issues_to_es()


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
