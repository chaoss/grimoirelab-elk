#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Gerrit reviews loader for Elastic Search
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

from grimoire.elk.elastic import ElasticSearch, ElasticConnectException
from grimoire.elk.gerrit import GerritElastic

from perceval.backends.gerrit import Gerrit

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    Gerrit.add_params(parser)

    args = parser.parse_args()

    app_init = datetime.now()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)


    gerrit = Gerrit(args.user, args.url, args.nreviews, args.cache,
                    not args.no_incremental)

    egerrit = GerritElastic(gerrit, args.sortinghat_db,
                            args.projects_grimoirelib_db,
                            args.gerrit_grimoirelib_db)



    es_index_gerrit = gerrit.get_id()

    clean = args.no_incremental

    if args.cache:
        clean = True

    try:
        state_index = es_index_gerrit+"_state"
        elastic_state = ElasticSearch(args.elastic_host, args.elastic_port,
                                      state_index, gerrit.get_elastic_mappings(),
                                      clean)

        elastic = ElasticSearch(args.elastic_host, args.elastic_port,
                                es_index_gerrit, egerrit.get_elastic_mappings(),
                                clean)

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)


    gerrit.set_elastic(elastic_state)
    egerrit.set_elastic(elastic)


    logging.info("Adding enrichment data to %s" % (egerrit.elastic.index_url))
    for review in gerrit.fetch():
        egerrit.fetch_events(review)

    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
