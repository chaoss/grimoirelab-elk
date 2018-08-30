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

import inspect
import logging
import pickle
import traceback

import redis

from datetime import datetime
from dateutil import parser

from arthur.common import Q_STORAGE_ITEMS
from perceval.backend import find_signature_parameters, Archive

from .utils import get_elastic
from .utils import get_connectors, get_connector_from_name
from .enriched.sortinghat_gelk import SortingHat
from .enriched.utils import get_last_enrich, grimoire_con


logger = logging.getLogger(__name__)

requests_ses = grimoire_con()

arthur_items = {}  # Hash with tag list with all items collected from arthur queue


def feed_arthur():
    """ Feed Ocean with backend data collected from arthur redis queue"""

    logger.info("Collecting items from redis queue")

    db_url = 'redis://localhost/8'

    conn = redis.StrictRedis.from_url(db_url)
    logger.debug("Redis connection stablished with %s.", db_url)

    # Get and remove queued items in an atomic transaction
    pipe = conn.pipeline()
    pipe.lrange(Q_STORAGE_ITEMS, 0, -1)
    pipe.ltrim(Q_STORAGE_ITEMS, 1, 0)
    items = pipe.execute()[0]

    for item in items:
        arthur_item = pickle.loads(item)
        if arthur_item['tag'] not in arthur_items:
            arthur_items[arthur_item['tag']] = []
        arthur_items[arthur_item['tag']].append(arthur_item)

    for tag in arthur_items:
        logger.debug("Items for %s: %i", tag, len(arthur_items[tag]))


def feed_backend_arthur(backend_name, backend_params):
    """ Feed Ocean with backend data collected from arthur redis queue"""

    # Always get pending items from arthur for all data sources
    feed_arthur()

    logger.debug("Items available for %s", arthur_items.keys())

    # Get only the items for the backend
    if not get_connector_from_name(backend_name):
        raise RuntimeError("Unknown backend %s" % backend_name)
    connector = get_connector_from_name(backend_name)
    klass = connector[3]  # BackendCmd for the connector

    backend_cmd = init_backend(klass(*backend_params))

    tag = backend_cmd.backend.tag
    logger.debug("Getting items for %s.", tag)

    if tag in arthur_items:
        logger.debug("Found items for %s.", tag)
        for item in arthur_items[tag]:
            yield item


def feed_backend(url, clean, fetch_archive, backend_name, backend_params,
                 es_index=None, es_index_enrich=None, project=None, arthur=False):
    """ Feed Ocean with backend data """

    backend = None
    repo = {'backend_name': backend_name, 'backend_params': backend_params}  # repository data to be stored in conf

    if es_index:
        clean = False  # don't remove index, it could be shared

    if not get_connector_from_name(backend_name):
        raise RuntimeError("Unknown backend %s" % backend_name)
    connector = get_connector_from_name(backend_name)
    klass = connector[3]  # BackendCmd for the connector

    try:
        logger.info("Feeding Ocean from %s (%s)", backend_name, es_index)

        if not es_index:
            logger.error("Raw index not defined for %s", backend_name)

        repo['repo_update_start'] = datetime.now().isoformat()

        # perceval backends fetch params
        offset = None
        from_date = None
        category = None
        latest_items = None

        backend_cmd = klass(*backend_params)

        parsed_args = vars(backend_cmd.parsed_args)
        init_args = find_signature_parameters(backend_cmd.BACKEND,
                                              parsed_args)

        if backend_cmd.archive_manager and fetch_archive:
            archive = Archive(parsed_args['archive_path'])
        else:
            archive = backend_cmd.archive_manager.create_archive() if backend_cmd.archive_manager else None

        init_args['archive'] = archive
        backend_cmd.backend = backend_cmd.BACKEND(**init_args)
        backend = backend_cmd.backend

        ocean_backend = connector[1](backend, fetch_archive=fetch_archive, project=project)
        elastic_ocean = get_elastic(url, es_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)

        if fetch_archive:
            signature = inspect.signature(backend.fetch_from_archive)
        else:
            signature = inspect.signature(backend.fetch)

        if 'from_date' in signature.parameters:
            try:
                # Support perceval pre and post BackendCommand refactoring
                from_date = backend_cmd.from_date
            except AttributeError:
                from_date = backend_cmd.parsed_args.from_date

        if 'offset' in signature.parameters:
            try:
                offset = backend_cmd.offset
            except AttributeError:
                offset = backend_cmd.parsed_args.offset

        if 'category' in signature.parameters:
            try:
                category = backend_cmd.category
            except AttributeError:
                try:
                    category = backend_cmd.parsed_args.category
                except AttributeError:
                    pass

        if 'latest_items' in signature.parameters:
            try:
                latest_items = backend_cmd.latest_items
            except AttributeError:
                latest_items = backend_cmd.parsed_args.latest_items

        # fetch params support
        if arthur:
            # If using arthur just provide the items generator to be used
            # to collect the items and upload to Elasticsearch
            aitems = feed_backend_arthur(backend_name, backend_params)
            ocean_backend.feed(arthur_items=aitems)
        elif latest_items:
            if category:
                ocean_backend.feed(latest_items=latest_items, category=category)
            else:
                ocean_backend.feed(latest_items=latest_items)
        elif offset:
            if category:
                ocean_backend.feed(from_offset=offset, category=category)
            else:
                ocean_backend.feed(from_offset=offset)
        elif from_date and from_date.replace(tzinfo=None) != parser.parse("1970-01-01"):
            if category:
                ocean_backend.feed(from_date, category=category)
            else:
                ocean_backend.feed(from_date)
        elif category:
            ocean_backend.feed(category=category)
        else:
            ocean_backend.feed()

    except Exception as ex:
        if backend:
            logger.error("Error feeding ocean from %s (%s): %s", backend_name, backend.origin, ex)
            # this print makes blackbird fails
            traceback.print_exc()
        else:
            logger.error("Error feeding ocean %s" % ex)
            traceback.print_exc()

    logger.info("Done %s " % (backend_name))


def get_items_from_uuid(uuid, enrich_backend, ocean_backend):
    """ Get all items that include uuid """

    # logger.debug("Getting items for merged uuid %s "  % (uuid))

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

    url_search = enrich_backend.elastic.index_url + "/_search"
    url_search += "?size=1000"  # TODO get all items

    r = requests_ses.post(url_search, data=query)

    eitems = r.json()['hits']['hits']

    if len(eitems) == 0:
        # logger.warning("No enriched items found for uuid: %s " % (uuid))
        return []

    items_ids = []

    for eitem in eitems:
        item_id = enrich_backend.get_item_id(eitem)
        # For one item several eitems could be generated
        if item_id not in items_ids:
            items_ids.append(item_id)

    # Time to get the items
    logger.debug("Items to be renriched for merged uuids: %s" % (",".join(items_ids)))

    url_mget = ocean_backend.elastic.index_url + "/_mget"

    items_ids_query = ""

    for item_id in items_ids:
        items_ids_query += '{"_id" : "%s"}' % (item_id)
        items_ids_query += ","
    items_ids_query = items_ids_query[:-1]  # remove last , for last item

    query = '{"docs" : [%s]}' % (items_ids_query)
    r = requests_ses.post(url_mget, data=query)

    res_items = r.json()['docs']

    items = []
    for res_item in res_items:
        if res_item['found']:
            items.append(res_item["_source"])

    return items


def refresh_projects(enrich_backend):
    logger.debug("Refreshing project field in %s", enrich_backend.elastic.index_url)
    total = 0

    eitems = enrich_backend.fetch()
    for eitem in eitems:
        new_project = enrich_backend.get_item_project(eitem)
        eitem.update(new_project)
        yield eitem
        total += 1

    logger.info("Total eitems refreshed for project field %i", total)


def refresh_identities(enrich_backend, filter_author=None):
    """Refresh identities in enriched index.

    Retrieve items from the enriched index corresponding to enrich_backend,
    and update their identities information, with fresh data from the
    SortingHat database.

    Instead of the whole index, only items matching the filter_author
    filter are fitered, if that parameters is not None.

    :param enrich_backend: enriched backend to update
    :param  filter_author: filter to use to match items
    """

    def update_items(new_filter_author):

        for eitem in enrich_backend.fetch(new_filter_author):
            # logger.info(eitem)
            roles = None
            try:
                roles = enrich_backend.roles
            except AttributeError:
                pass
            new_identities = enrich_backend.get_item_sh_from_id(eitem, roles)
            eitem.update(new_identities)
            yield eitem

    logger.debug("Refreshing identities fields from %s", enrich_backend.elastic.index_url)

    total = 0

    max_ids = enrich_backend.elastic.max_items_clause

    if filter_author is None:
        # No filter, update all items
        for item in update_items(None):
            yield item
            total += 1
    else:
        nidentities = len(filter_author['value'])
        logger.debug('Identities to refresh: %i', nidentities)
        if nidentities > max_ids:
            logger.warning('Refreshing identities in groups of %i', max_ids)
            while filter_author['value']:
                new_filter_author = {"name": filter_author['name'],
                                     "value": filter_author['value'][0:max_ids]}
                filter_author['value'] = filter_author['value'][max_ids:]
                for item in update_items(new_filter_author):
                    yield item
                    total += 1
        else:
            for item in update_items(filter_author):
                yield item
                total += 1
    logger.info("Total eitems refreshed for identities fields %i", total)


def load_identities(ocean_backend, enrich_backend):
    # First we add all new identities to SH
    items_count = 0
    identities_count = 0
    new_identities = []

    # Support that ocean_backend is a list of items (old API)
    if isinstance(ocean_backend, list):
        items = ocean_backend
    else:
        items = ocean_backend.fetch()

    for item in items:
        items_count += 1
        # Get identities from new items to be added to SortingHat
        identities = enrich_backend.get_identities(item)
        for identity in identities:
            if identity not in new_identities:
                new_identities.append(identity)

            if len(new_identities) > 100:
                inserted_identities = load_bulk_identities(items_count,
                                                           new_identities,
                                                           enrich_backend.sh_db,
                                                           enrich_backend.get_connector_name())
                identities_count += inserted_identities
                new_identities = []

    if new_identities:
        inserted_identities = load_bulk_identities(items_count,
                                                   new_identities,
                                                   enrich_backend.sh_db,
                                                   enrich_backend.get_connector_name())
        identities_count += inserted_identities

    return identities_count


def load_bulk_identities(items_count, new_identities, sh_db, connector_name):
    identities_count = len(new_identities)

    SortingHat.add_identities(sh_db, new_identities, connector_name)

    logger.debug("Processed %i items identities (%i identities) from %s",
                 items_count, len(new_identities), connector_name)

    return identities_count


def enrich_items(ocean_backend, enrich_backend, events=False):
    total = 0

    if not events:
        total = enrich_backend.enrich_items(ocean_backend)
    else:
        total = enrich_backend.enrich_events(ocean_backend)
    return total


def get_ocean_backend(backend_cmd, enrich_backend, no_incremental,
                      filter_raw=None, filter_raw_should=None):
    """ Get the ocean backend configured to start from the last enriched date """

    if no_incremental:
        last_enrich = None
    else:
        last_enrich = get_last_enrich(backend_cmd, enrich_backend)

    logger.debug("Last enrichment: %s", last_enrich)

    backend = None

    connector = get_connectors()[enrich_backend.get_connector_name()]

    if backend_cmd:
        backend_cmd = init_backend(backend_cmd)
        backend = backend_cmd.backend

        signature = inspect.signature(backend.fetch)
        if 'from_date' in signature.parameters:
            ocean_backend = connector[1](backend, from_date=last_enrich)
        elif 'offset' in signature.parameters:
            ocean_backend = connector[1](backend, offset=last_enrich)
        else:
            ocean_backend = connector[1](backend)
    else:
        # We can have params for non perceval backends also
        params = enrich_backend.backend_params
        if params:
            try:
                date_pos = params.index('--from-date')
                last_enrich = parser.parse(params[date_pos + 1])
            except ValueError:
                pass
        if last_enrich:
            ocean_backend = connector[1](backend, from_date=last_enrich)
        else:
            ocean_backend = connector[1](backend)

    if filter_raw:
        ocean_backend.set_filter_raw(filter_raw)
    if filter_raw_should:
        ocean_backend.set_filter_raw_should(filter_raw_should)

    return ocean_backend


def do_studies(ocean_backend, enrich_backend, studies_args):
    """

    :param ocean_backend: backend to access raw items
    :param enrich_backend: backend to access enriched items
    :param studies_args: list of studies to be executed
    :return: None
    """
    for study in enrich_backend.studies:
        selected_studies = [(s['name'], s['params']) for s in studies_args if s['type'] == study.__name__]

        for (name, params) in selected_studies:
            logger.info("Starting study: %s, params %s", name, str(params))
            try:
                study(ocean_backend, enrich_backend, **params)
            except Exception as e:
                logger.error("Problem executing study %s, %s", name, str(e))
                raise e


def enrich_backend(url, clean, backend_name, backend_params,
                   ocean_index=None,
                   ocean_index_enrich=None,
                   db_projects_map=None, json_projects_map=None,
                   db_sortinghat=None,
                   no_incremental=False, only_identities=False,
                   github_token=None, studies=False, only_studies=False,
                   url_enrich=None, events_enrich=False,
                   db_user=None, db_password=None, db_host=None,
                   do_refresh_projects=False, do_refresh_identities=False,
                   author_id=None, author_uuid=None, filter_raw=None,
                   filters_raw_prefix=None, jenkins_rename_file=None,
                   unaffiliated_group=None, pair_programming=False,
                   studies_args=None):
    """ Enrich Ocean index """

    backend = None
    enrich_index = None

    if ocean_index or ocean_index_enrich:
        clean = False  # don't remove index, it could be shared

    if do_refresh_projects or do_refresh_identities:
        clean = False  # refresh works over the existing enriched items

    if not get_connector_from_name(backend_name):
        raise RuntimeError("Unknown backend %s" % backend_name)
    connector = get_connector_from_name(backend_name)
    klass = connector[3]  # BackendCmd for the connector

    try:
        backend = None
        backend_cmd = None
        if klass:
            # Data is retrieved from Perceval
            backend_cmd = init_backend(klass(*backend_params))
            backend = backend_cmd.backend

        if ocean_index_enrich:
            enrich_index = ocean_index_enrich
        else:
            if not ocean_index:
                ocean_index = backend_name + "_" + backend.origin
            enrich_index = ocean_index + "_enrich"
        if events_enrich:
            enrich_index += "_events"

        enrich_backend = connector[2](db_sortinghat, db_projects_map, json_projects_map,
                                      db_user, db_password, db_host)
        enrich_backend.set_params(backend_params)
        if url_enrich:
            elastic_enrich = get_elastic(url_enrich, enrich_index, clean, enrich_backend)
        else:
            elastic_enrich = get_elastic(url, enrich_index, clean, enrich_backend)
        enrich_backend.set_elastic(elastic_enrich)
        if github_token and backend_name == "git":
            enrich_backend.set_github_token(github_token)
        if jenkins_rename_file and backend_name == "jenkins":
            enrich_backend.set_jenkins_rename_file(jenkins_rename_file)
        if unaffiliated_group:
            enrich_backend.unaffiliated_group = unaffiliated_group
        if pair_programming:
            enrich_backend.pair_programming = pair_programming

        # filter_raw must be converted from the string param to a dict
        filter_raw_dict = {}
        if filter_raw:
            filter_raw_dict['name'] = filter_raw.split(":")[0].replace('"', '')
            filter_raw_dict['value'] = filter_raw.split(":")[1].replace('"', '')
        # filters_raw_prefix must be converted from the list param to
        # DSL query format for a should filter inside a boolean filter
        filter_raw_should = None
        if filters_raw_prefix:
            filter_raw_should = {"should": []}
            for filter_prefix in filters_raw_prefix:
                fname = filter_prefix.split(":")[0].replace('"', '')
                fvalue = filter_prefix.split(":")[1].replace('"', '')
                filter_raw_should["should"].append(
                    {
                        "prefix": {fname: fvalue}
                    }
                )

        ocean_backend = get_ocean_backend(backend_cmd, enrich_backend,
                                          no_incremental, filter_raw_dict,
                                          filter_raw_should)

        if only_studies:
            logger.info("Running only studies (no SH and no enrichment)")
            do_studies(ocean_backend, enrich_backend, studies_args)
        elif do_refresh_projects:
            logger.info("Refreshing project field in %s", enrich_backend.elastic.index_url)
            field_id = enrich_backend.get_field_unique_id()
            eitems = refresh_projects(enrich_backend)
            enrich_backend.elastic.bulk_upload(eitems, field_id)
        elif do_refresh_identities:

            filter_author = None
            if author_id:
                filter_author = {'name': 'author_id',
                                 'value': author_id}
            elif author_uuid:
                filter_author = {'name': 'author_uuid',
                                 'value': author_uuid}

            logger.info("Refreshing identities fields in %s", enrich_backend.elastic.index_url)

            field_id = enrich_backend.get_field_unique_id()
            eitems = refresh_identities(enrich_backend, filter_author)
            enrich_backend.elastic.bulk_upload(eitems, field_id)
        else:
            clean = False  # Don't remove ocean index when enrich
            elastic_ocean = get_elastic(url, ocean_index, clean, ocean_backend)
            ocean_backend.set_elastic(elastic_ocean)

            logger.info("Adding enrichment data to %s", enrich_backend.elastic.index_url)

            if db_sortinghat:
                # FIXME: This step won't be done from enrich in the future
                total_ids = load_identities(ocean_backend, enrich_backend)
                logger.info("Total identities loaded %i ", total_ids)

            if only_identities:
                logger.info("Only SH identities added. Enrich not done!")

            else:
                # Enrichment for the new items once SH update is finished
                if not events_enrich:
                    enrich_count = enrich_items(ocean_backend, enrich_backend)
                    if enrich_count is not None:
                        logger.info("Total items enriched %i ", enrich_count)
                else:
                    enrich_count = enrich_items(ocean_backend, enrich_backend, events=True)
                    if enrich_count is not None:
                        logger.info("Total events enriched %i ", enrich_count)
                if studies:
                    do_studies(ocean_backend, enrich_backend, studies_args)

    except Exception as ex:
        logger.error("%s", traceback.format_exc())
        if backend:
            logger.error("Error enriching ocean from %s (%s): %s",
                         backend_name, backend.origin, ex)
        else:
            logger.error("Error enriching ocean %s", ex)

    logger.info("Done %s ", backend_name)


def init_backend(backend_cmd):
    """Init backend within the backend_cmd"""

    try:
        backend_cmd.backend
    except AttributeError:
        parsed_args = vars(backend_cmd.parsed_args)
        init_args = find_signature_parameters(backend_cmd.BACKEND,
                                              parsed_args)
        backend_cmd.backend = backend_cmd.BACKEND(**init_args)

    return backend_cmd
