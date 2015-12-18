#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Grimoire Arthur lib
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
import sys

from grimoire.ocean.conf import ConfOcean

from grimoire.utils import get_elastic
from grimoire.utils import get_connector_from_name

def feed_backend(url, params, clean):
    ''' Feed Ocean with backend data '''

    backend = None
    backend_name = params['backend']
    repo = {}    # repository data to be stored in conf
    repo['params'] = params
    es_index = None

    connector = get_connector_from_name(backend_name)
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
                          (backend.get_name(), backend.get_id(), ex))
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
