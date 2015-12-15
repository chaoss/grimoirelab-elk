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

from grimoire.ocean.conf import ConfOcean

from grimoire.utils import get_connector_from_name, get_connectors, get_elastic
from grimoire.utils import get_params, config_logging


def feed_backends(url, connectors, clean, debug = False):
    ''' Update Ocean for all existing backends '''

    logging.info("Updating all Ocean")
    elastic = get_elastic(url, ConfOcean.get_index(), clean)
    ConfOcean.set_elastic(elastic)

    for repo in ConfOcean.get_repos():
        params = repo['params']
        params['no_incremental'] = True  # Always try incremental
        params['debug'] = debug  # Use for all debug level defined in p2o

        feed_backend(url, params, connectors, clean)


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


def loop_update(min_update_time, url, connectors, clean, debug):

    while True:
        ustart = time()

        feed_backends(url, connectors, clean, debug)

        update_time = int(time()-ustart)
        update_sleep = min_update_time - update_time
        logging.info("Ocean update time: %i minutes" % (update_time/60))
        if update_sleep > 0:
            logging.debug("Waiting for updating %i seconds " % (update_sleep))
            sleep(update_sleep)


if __name__ == '__main__':

    app_init = datetime.now()

    connectors = get_connectors() 

    args = get_params(connectors)

    config_logging(args.debug)

    url = args.elastic_url

    clean = args.no_incremental
    if args.cache:
        clean = True

    try:
        if args.backend:
            feed_backend(url, vars(args), connectors, clean)
        else:
            if args.loop:
                # minimal update duration to avoid too much frequency in secs
                min_update_time = 60
                loop_update(min_update_time, url, connectors, clean, args.debug)
            else:
                feed_backends(url, connectors, clean, args.debug)

    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        sys.exit(0)


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
