#!/usr/bin/env python3
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

import logging
from datetime import datetime
from os import sys

from grimoire_elk.elk import feed_backend, enrich_backend
from grimoire_elk.elastic import ElasticSearch
from grimoire_elk.elastic_items import ElasticItems
from grimoire_elk.utils import get_params, config_logging


if __name__ == '__main__':

    app_init = datetime.now()

    args = get_params()

    config_logging(args.debug)

    url = args.elastic_url

    clean = args.no_incremental
    if args.fetch_cache:
        clean = True

    try:
        if args.backend:
            # Configure elastic bulk size and scrolling
            if args.bulk_size:
                ElasticSearch.max_items_bulk = args.bulk_size
            if args.scroll_size:
                ElasticItems.scroll_size = args.scroll_size
            if not args.enrich_only:
                feed_backend(url, clean, args.fetch_cache,
                             args.backend, args.backend_args,
                             args.index, args.index_enrich, args.project,
                             args.arthur)
                logging.info("Backend feed completed")

            studies_args = None
            if args.studies_list:
                # Convert the list to the expected format in enrich_backend method
                studies_args = []

                for study in args.studies_list:
                    studies_args.append({"name": study,
                                         "type": study,
                                         "params": {}
                                         })

            if args.enrich or args.enrich_only:
                unaffiliated_group = None
                enrich_backend(url, clean, args.backend, args.backend_args,
                               args.index, args.index_enrich,
                               args.db_projects_map, args.json_projects_map,
                               args.db_sortinghat,
                               args.no_incremental, args.only_identities,
                               args.github_token,
                               args.studies, args.only_studies,
                               args.elastic_url_enrich, args.events_enrich,
                               args.db_user, args.db_password, args.db_host,
                               args.refresh_projects, args.refresh_identities,
                               args.author_id, args.author_uuid,
                               args.filter_raw, args.filters_raw_prefix,
                               args.jenkins_rename_file, unaffiliated_group,
                               args.pair_programming, studies_args)
                logging.info("Enrich backend completed")
            elif args.events_enrich:
                logging.info("Enrich option is needed for events_enrich")
        else:
            logging.error("You must configure a backend")

    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        sys.exit(0)

    total_time_min = (datetime.now() - app_init).total_seconds() / 60

    logging.info("Finished in %.2f min" % (total_time_min))
