#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Identities extractor for Ocean Items to Sorting Hat
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
import requests


from grimoire.ocean.elastic import ElasticOcean
from grimoire.ocean.conf import ConfOcean
from grimoire.utils import get_elastic, config_logging, get_connector_from_name

from sortinghat import api
from sortinghat.db.database import Database
from sortinghat.db.model import UniqueIdentity, Identity, Profile,\
    Organization, Domain, Country, Enrollment, MatchingBlacklist
from sortinghat.exceptions import AlreadyExistsError, NotFoundError
from sortinghat.matcher import create_identity_matcher

def get_params():
    ''' Get params definition from ElasticOcean '''
    parser = argparse.ArgumentParser()
    ElasticOcean.add_params(parser)

    # Commands supported

    parser.add_argument("--index", help="Ocean index with identities to extract")

    args = parser.parse_args()

    return args

def get_index_backend(url, index):
    logging.info("Get backend for index: %s" % (index))
    elastic = get_elastic(url, ConfOcean.get_index())
    ConfOcean.set_elastic(elastic)

    r = requests.get(elastic.index_url+"/repos/"+index)
    backend = r.json()['_source']['params']['backend']

    return backend

def get_identities(obackend):
    identities = []
    unique_identities = []
    for item in obackend:
        identities = obackend.get_identities(item)
        for identity in identities:
            if identity not in unique_identities:
                unique_identities.append(identity)
    return unique_identities

def add_identities(identities, backend):
    logging.info("Adding the identities to SortingHat")

    db = Database("root", "", "ocean_sh", "mariadb")

    for identity in identities:
        try:
            res = api.add_identity(db, backend, identity['email'],
                                   identity['name'], identity['username'])
        except AlreadyExistsError:
            pass


if __name__ == '__main__':

    app_init = datetime.now()

    args = get_params()

    config_logging(args.debug)

    if args.index is None:
        # Extract identities from all indexes
        pass
    else:
        logging.info("Extracting identities from: %s" % (args.index))
        backend_name = get_index_backend(args.elastic_url, args.index)

        obackend_class = get_connector_from_name(backend_name)[1]
        perceval_backend = None  # Don't use perceval

        obackend =  obackend_class(perceval_backend, incremental=False)
        obackend.set_elastic(get_elastic(args.elastic_url, args.index))

        identities = get_identities(obackend)
        add_identities(identities, backend_name)

        # Add the identities to Sorting Hat

        print ("Total identities processed: %i" % (len(identities)))