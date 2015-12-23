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

from grimoire.elk.sortinghat import SortingHat


def get_params():
    ''' Get params definition from ElasticOcean '''
    parser = argparse.ArgumentParser()
    ElasticOcean.add_params(parser)

    # Commands supported

    parser.add_argument("--index", help="Ocean index with identities to extract")

    args = parser.parse_args()

    return args

def get_perceval_params(url, index):
    logging.info("Get perceval params for index: %s" % (index))
    elastic = get_elastic(url, ConfOcean.get_index())
    ConfOcean.set_elastic(elastic)

    r = requests.get(elastic.index_url+"/repos/"+index)

    params = r.json()['_source']['params']

    return params

def get_identities(obackend):
    """ Get identities from items in ocean backend and remove duplicates """
    identities = []
    unique_identities = []
    for item in obackend:
        identities = obackend.get_identities(item)
        for identity in identities:
            if identity not in unique_identities:
                unique_identities.append(identity)
    return unique_identities


if __name__ == '__main__':

    app_init = datetime.now()

    args = get_params()

    config_logging(args.debug)

    if args.index is None:
        # Extract identities from all indexes
        pass
    else:
        logging.info("Extracting identities from: %s" % (args.index))
        perceval_params = get_perceval_params(args.elastic_url, args.index)
        backend_name = perceval_params['backend']
        connector = get_connector_from_name(backend_name)
        perceval_backend_class = connector[0]
        ocean_backend_class = connector[1]
        perceval_backend = None  # Don't use perceval

        perceval_backend = perceval_backend_class(**perceval_params)

        obackend =  ocean_backend_class(perceval_backend, incremental=False)
        obackend.set_elastic(get_elastic(args.elastic_url, args.index))

        identities = get_identities(obackend)
        SortingHat.add_identities(identities, backend_name)

        # Add the identities to Sorting Hat

        print ("Total identities processed: %i" % (len(identities)))