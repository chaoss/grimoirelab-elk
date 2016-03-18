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

from datetime import datetime
import logging
from os import sys
from time import time, sleep

from grimoire.arthur import feed_backend, enrich_backend

from grimoire.ocean.conf import ConfOcean

from grimoire.utils import get_elastic
from grimoire.utils import get_params_parser, config_logging

from redis import Redis
from rq import Queue


def get_params():
    ''' Get params definition from ElasticOcean and from all the backends '''

    parser = get_params_parser()

    args = parser.parse_args()

    return args


def feed_backends(url, clean, debug = False, redis = None):
    ''' Update Ocean for all existing backends '''

    logging.info("Updating all Ocean")
    elastic = get_elastic(url, ConfOcean.get_index(), clean)
    ConfOcean.set_elastic(elastic)

    q = Queue('update', connection=Redis(redis), async=async_)

    for repo in ConfOcean.get_repos():
        task_feed = q.enqueue(feed_backend, url, clean,
                              repo['backend_name'], repo['backend_params'], repo['index'])
        logging.info("Queued job")
        logging.info(task_feed)


def loop_update(min_update_time, url, clean, debug, redis):

    while True:
        ustart = time()

        feed_backends(url, clean, debug, redis)

        update_time = int(time()-ustart)
        update_sleep = min_update_time - update_time
        logging.info("Ocean update time: %i minutes" % (update_time/60))
        if update_sleep > 0:
            logging.debug("Waiting for updating %i seconds " % (update_sleep))
            sleep(update_sleep)


if __name__ == '__main__':

    app_init = datetime.now()

    args = get_params()

    async_ = False  # Use RQ or not

    config_logging(args.debug)

    url = args.elastic_url

    clean = args.no_incremental
    if args.fetch_cache:
        clean = True

    try:
        if args.loop:
            # minimal update duration to avoid too much frequency in secs
            min_update_time = 60
            loop_update(min_update_time, url, clean, args.debug,
                        args.redis)
        elif args.backend:
            if not args.enrich_only:
                q = Queue('create', connection=Redis(args.redis), async=async_)
                task_feed = q.enqueue(feed_backend, url, clean, args.fetch_cache,
                                      args.backend, args.backend_args,
                                      args.index)
                logging.info("Queued feed_backend job")
                logging.info(task_feed)

            if args.enrich or args.enrich_only:
                q = Queue('enrich', connection=Redis(args.redis), async=async_)
                if async_:
                    # Task enrich after feed
                    result = q.enqueue(enrich_backend, url, clean,
                                       args.backend, args.backend_args,
                                       args.index,
                                       args.db_projects_map, args.db_sortinghat,
                                       depends_on=task_feed)
                else:
                    result = q.enqueue(enrich_backend, url, clean,
                                       args.backend, args.backend_args,
                                       args.index,
                                       args.db_projects_map, args.db_sortinghat)
                logging.info("Queued enrich_backend job")
                logging.info(result)

        else:
            feed_backends(url, clean, args.debug, args.redis)

    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        sys.exit(0)


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
