#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Grimoire Arthur lib.
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
from dateutil import parser
import logging
import requests
import traceback

from grimoire.elk.sortinghat import SortingHat
from grimoire.ocean.conf import ConfOcean
from grimoire.utils import get_elastic
from grimoire.utils import get_connector_from_name

def feed_backend(url, clean, fetch_cache, backend_name, backend_params,
                 es_index=None, es_index_enrich=None, project=None):
    """ Feed Ocean with backend data """

    backend = None
    repo = {}    # repository data to be stored in conf
    repo['backend_name'] = backend_name
    repo['backend_params'] = backend_params

    if es_index:
        clean = False  # don't remove index, it could be shared

    if not get_connector_from_name(backend_name):
        raise RuntimeError("Unknown backend %s" % backend_name)
    connector = get_connector_from_name(backend_name)
    klass = connector[3]  # BackendCmd for the connector

    try:
        backend_cmd = klass(*backend_params)

        backend = backend_cmd.backend
        ocean_backend = connector[1](backend, fetch_cache=fetch_cache, project=project)

        logging.info("Feeding Ocean from %s (%s)", backend_name, backend.origin)

        if not es_index:
            es_index = backend_name + "_" + backend.origin
        elastic_ocean = get_elastic(url, es_index, clean, ocean_backend)

        ocean_backend.set_elastic(elastic_ocean)

        ConfOcean.set_elastic(elastic_ocean)

        repo['repo_update_start'] = datetime.now().isoformat()

        # offset param suppport
        offset = None

        try:
            offset = backend_cmd.offset
        except AttributeError:
            # The backend does not support offset
            pass

        # from_date param support
        try:
            if offset:
                ocean_backend.feed(offset=offset)
            else:
                if backend_cmd.from_date.replace(tzinfo=None) == \
                    parser.parse("1970-01-01").replace(tzinfo=None):
                    # Don't use the default value
                    ocean_backend.feed()
                else:
                    ocean_backend.feed(backend_cmd.from_date)
        except AttributeError:
            # The backend does not support from_date
            ocean_backend.feed()

    except Exception as ex:
        if backend:
            logging.error("Error feeding ocean from %s (%s): %s" %
                          (backend_name, backend.origin, ex))
            # this print makes blackbird fails
            traceback.print_exc()
        else:
            logging.error("Error feeding ocean %s" % ex)

        repo['success'] = False
        repo['error'] = str(ex)
    else:
        repo['success'] = True

    repo['repo_update'] = datetime.now().isoformat()
    repo['index'] = es_index
    repo['index_enrich'] = es_index_enrich
    repo['project'] = project

    if es_index:
        unique_id = es_index+"_"+backend.origin
        ConfOcean.add_repo(unique_id, repo)
    else:
        logging.debug("Repository not added to Ocean because errors.")
        logging.debug(backend_params)

    logging.info("Done %s " % (backend_name))


def get_items_from_uuid(uuid, enrich_backend, ocean_backend):
    """ Get all items that include uuid """

    # logging.debug("Getting items for merged uuid %s "  % (uuid))

    uuid_fields = enrich_backend.get_fields_uuid()

    terms = ""  # all terms with uuids in the enriched item

    for field in uuid_fields:
        terms += """
         {"term": {
           "%s": {
              "value": "%s"
           }
         }}
         """ % (field, uuid)
        terms += ","

    terms = terms[:-1]  # remove last , for last item

    query = """
    {"query": { "bool": { "should": [%s] }}}
    """ % (terms)

    url_search = enrich_backend.elastic.index_url+"/_search"
    url_search +="?size=1000"  # TODO get all items

    r = requests.post(url_search, data=query)

    eitems = r.json()['hits']['hits']

    if len(eitems) == 0:
        # logging.warning("No enriched items found for uuid: %s " % (uuid))
        return []

    items_ids = []

    for eitem in eitems:
        item_id = enrich_backend.get_item_id(eitem)
        # For one item several eitems could be generated
        if item_id not in items_ids:
            items_ids.append(item_id)

    # Time to get the items
    logging.debug ("Items to be renriched for merged uuids: %s" % (",".join(items_ids)))

    url_mget = ocean_backend.elastic.index_url+"/_mget"

    items_ids_query = ""

    for item_id in items_ids:
        items_ids_query += '{"_id" : "%s"}' % (item_id)
        items_ids_query += ","
    items_ids_query = items_ids_query[:-1]  # remove last , for last item

    query = '{"docs" : [%s]}' % (items_ids_query)
    r = requests.post(url_mget, data=query)

    res_items = r.json()['docs']

    items = []
    for res_item in res_items:
        if res_item['found']:
            items.append(res_item["_source"])

    return items


def enrich_backend(url, clean, backend_name, backend_params, ocean_index=None,
                   ocean_index_enrich = None,
                   db_projects_map=None, db_sortinghat=None,
                   no_incremental=False, only_identities=False,
                   github_token=None, studies=False, only_studies=False,
                   url_enrich=None):
    """ Enrich Ocean index """

    def enrich_items(items, enrich_backend):
        total = 0

        items_pack = []

        for item in items:
            # print("%s %s" % (item['url'], item['lastUpdated_date']))
            if len(items_pack) >= enrich_backend.elastic.max_items_bulk:
                logging.info("Adding %i (%i done) enriched items to %s" % \
                             (enrich_backend.elastic.max_items_bulk, total,
                              enrich_backend.elastic.index_url))
                enrich_backend.enrich_items(items_pack)
                items_pack = []
            items_pack.append(item)
            total += 1
        enrich_backend.enrich_items(items_pack)

        return total

    def enrich_sortinghat(ocean_backend, enrich_backend):
        # First we add all new identities to SH
        item_count = 0
        new_identities = []

        for item in ocean_backend:
            item_count += 1
            # Get identities from new items to be added to SortingHat
            identities = enrich_backend.get_identities(item)
            for identity in identities:
                if identity not in new_identities:
                    new_identities.append(identity)
            if item_count % 1000 == 0:
                logging.debug("Processed %i items identities (%i identities)" \
                               % (item_count, len(new_identities)))
        logging.debug("TOTAL ITEMS: %i" % (item_count))

        logging.info("Total new identities to be checked %i" % len(new_identities))

        merged_identities = SortingHat.add_identities(enrich_backend.sh_db,
                                                      new_identities, enrich_backend.get_connector_name())

        # Redo enrich for items with new merged identities
        renrich_items = []
        # For testing
        # merged_identities = ['7e0bcf6ff46848403eaffa29ef46109f386fa24b']
        for mid in merged_identities:
            renrich_items += get_items_from_uuid(mid, enrich_backend, ocean_backend)

        # Enrich items with merged identities
        enrich_count_merged = enrich_items(renrich_items, enrich_backend)
        return enrich_count_merged

    def do_studies(enrich_backend, last_enrich):
        try:
            for study in enrich_backend.studies:
                logging.info("Starting study: %s", study)
                study(from_date=last_enrich)
        except Exception as e:
            logging.warning("Problem executing studies for %s", backend_name)
            print(e)



    backend = None
    enrich_index = None

    if ocean_index or ocean_index_enrich:
        clean = False  # don't remove index, it could be shared

    if not get_connector_from_name(backend_name):
        raise RuntimeError("Unknown backend %s" % backend_name)
    connector = get_connector_from_name(backend_name)
    klass = connector[3]  # BackendCmd for the connector

    try:
        backend = None
        if klass:
            # Data is retrieved from Perceval
            backend_cmd = klass(*backend_params)

            backend = backend_cmd.backend

        if ocean_index_enrich:
            enrich_index = ocean_index_enrich
        else:
            if not ocean_index:
                ocean_index = backend_name + "_" + backend.origin
            enrich_index = ocean_index+"_enrich"

        enrich_backend = connector[2](backend, db_sortinghat, db_projects_map)
        if url_enrich:
            elastic_enrich = get_elastic(url_enrich, enrich_index, clean, enrich_backend)
        else:
            elastic_enrich = get_elastic(url, enrich_index, clean, enrich_backend)
        enrich_backend.set_elastic(elastic_enrich)
        if github_token and backend_name == "git":
            enrich_backend.set_github_token(github_token)

        # We need to enrich from just updated items since last enrichment
        # Always filter by origin to support multi origin indexes
        last_enrich = None
        if backend:
            # Only supported in data retrieved from a perceval backend
            filter_ = {"name":"origin",
                       "value":backend.origin}
            last_enrich = enrich_backend.get_last_update_from_es(filter_)
        if no_incremental:
            last_enrich = None

        logging.debug("Last enrichment: %s", last_enrich)

        # last_enrich=parser.parse('2016-06-01')

        ocean_backend = connector[1](backend, from_date=last_enrich)
        clean = False  # Don't remove ocean index when enrich
        elastic_ocean = get_elastic(url, ocean_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)

        logging.info("Adding enrichment data to %s", enrich_backend.elastic.index_url)

        if only_studies:
            logging.info("Running only studies (no SH and no enrichment)")
            do_studies(enrich_backend, last_enrich)

        else:
            if db_sortinghat:
                enrich_count_merged = 0

                enrich_count_merged = enrich_sortinghat(ocean_backend, enrich_backend)
                logging.info("Total items enriched for merged identities %i ", enrich_count_merged)

            if only_identities:
                logging.info("Only SH identities added. Enrich not done!")

            else:
                # Enrichment for the new items once SH update is finished
                enrich_count = enrich_items(ocean_backend, enrich_backend)
                logging.info("Total items enriched %i ", enrich_count)

                if studies:
                    do_studies(enrich_backend, last_enrich)

    except Exception as ex:
        traceback.print_exc()
        if backend:
            logging.error("Error enriching ocean from %s (%s): %s",
                          backend_name, backend.origin, ex)
        else:
            logging.error("Error enriching ocean %s", ex)

    logging.info("Done %s ", backend_name)
