# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import inspect
import logging

from elasticsearch import Elasticsearch

from perceval.backend import find_signature_parameters, Archive
from perceval.errors import RateLimitError
from grimoirelab_toolkit.datetime import (datetime_utcnow, str_to_datetime)

from .elastic_mapping import Mapping as BaseMapping
from .elastic_items import ElasticItems
from .enriched.sortinghat_gelk import SortingHat
from .enriched.utils import get_last_enrich, grimoire_con, get_diff_current_date, anonymize_url
from .utils import get_connectors, get_connector_from_name, get_elastic

IDENTITIES_INDEX = "grimoirelab_identities_cache"
SIZE_SCROLL_IDENTITIES_INDEX = 1000

logger = logging.getLogger(__name__)

requests_ses = grimoire_con()


def feed_backend(url, clean, fetch_archive, backend_name, backend_params,
                 es_index=None, es_index_enrich=None, project=None,
                 es_aliases=None, projects_json_repo=None, repo_labels=None,
                 anonymize=False):
    """ Feed Ocean with backend data """

    error_msg = None
    backend = None
    repo = {'backend_name': backend_name, 'backend_params': backend_params}  # repository data to be stored in conf

    if es_index:
        clean = False  # don't remove index, it could be shared

    if not get_connector_from_name(backend_name):
        raise RuntimeError("Unknown backend {}".format(backend_name))
    connector = get_connector_from_name(backend_name)
    klass = connector[3]  # BackendCmd for the connector

    try:
        logger.debug("Feeding raw from {} ({})".format(backend_name, es_index))

        if not es_index:
            logger.error("Raw index not defined for {}".format(backend_name))

        repo['repo_update_start'] = datetime_utcnow().isoformat()

        # perceval backends fetch params
        offset = None
        from_date = None
        category = None
        branches = None
        latest_items = None
        filter_classified = None

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

        ocean_backend = connector[1](backend, fetch_archive=fetch_archive, project=project, anonymize=anonymize)
        elastic_ocean = get_elastic(url, es_index, clean, ocean_backend, es_aliases)
        ocean_backend.set_elastic(elastic_ocean)
        ocean_backend.set_repo_labels(repo_labels)
        ocean_backend.set_projects_json_repo(projects_json_repo)

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

        if 'branches' in signature.parameters:
            try:
                branches = backend_cmd.branches
            except AttributeError:
                try:
                    branches = backend_cmd.parsed_args.branches
                except AttributeError:
                    pass

        if 'filter_classified' in signature.parameters:
            try:
                filter_classified = backend_cmd.parsed_args.filter_classified
            except AttributeError:
                pass

        if 'latest_items' in signature.parameters:
            try:
                latest_items = backend_cmd.latest_items
            except AttributeError:
                latest_items = backend_cmd.parsed_args.latest_items

        params = {}
        if latest_items:
            params['latest_items'] = latest_items
        if category:
            params['category'] = category
        if branches:
            params['branches'] = branches
        if filter_classified:
            params['filter_classified'] = filter_classified
        if from_date and (from_date.replace(tzinfo=None) != str_to_datetime("1970-01-01").replace(tzinfo=None)):
            params['from_date'] = from_date
        if offset:
            params['from_offset'] = offset

        ocean_backend.feed(**params)

    except RateLimitError as ex:
        logger.error("Error feeding raw from {} ({}): rate limit exceeded".format(backend_name, backend.origin))
        error_msg = "RateLimitError: seconds to reset {}".format(ex.seconds_to_reset)
    except Exception as ex:
        if backend:
            error_msg = "Error feeding raw from {} ({}): {}".format(backend_name, backend.origin, ex)
            logger.error(error_msg, exc_info=True)
        else:
            error_msg = "Error feeding raw from {}".format(ex)
            logger.error(error_msg, exc_info=True)

    logger.info("[{}] Done collection for {}".format(backend_name, anonymize_url(backend.origin)))
    return error_msg


def refresh_projects(enrich_backend):
    logger.debug("Refreshing project field in {}".format(
                 anonymize_url(enrich_backend.elastic.index_url)))
    total = 0

    eitems = enrich_backend.fetch()
    for eitem in eitems:
        new_project = enrich_backend.get_item_project(eitem)
        eitem.update(new_project)
        yield eitem
        total += 1

    logger.debug("Total eitems refreshed for project field {}".format(total))


def refresh_identities(enrich_backend, author_field=None, author_values=None):
    """Refresh identities in enriched index.

    Retrieve items from the enriched index corresponding to enrich_backend,
    and update their identities information, with fresh data from the
    SortingHat database.

    Instead of the whole index, only items matching the filter_author
    filter are fitered, if that parameters is not None.

    :param enrich_backend: enriched backend to update
    :param  author_field: field to match items authored by a user
    :param  author_values: values of the authored field to match items
    """

    def update_items(new_filter_author):

        for eitem in enrich_backend.fetch(new_filter_author):
            roles = None
            try:
                roles = enrich_backend.roles
            except AttributeError:
                pass

            new_identities = enrich_backend.get_item_sh_from_id(eitem, roles)
            eitem.update(new_identities)
            yield eitem

    logger.debug("Refreshing identities fields from {}".format(
                 anonymize_url(enrich_backend.elastic.index_url)))

    total = 0

    max_ids = enrich_backend.elastic.max_items_clause
    logger.debug('Refreshing identities')

    if author_field is None:
        # No filter, update all items
        for item in update_items(None):
            yield item
            total += 1
    else:
        to_refresh = []
        for author_value in author_values:
            to_refresh.append(author_value)

            if len(to_refresh) > max_ids:
                filter_author = {"name": author_field,
                                 "value": to_refresh}

                for item in update_items(filter_author):
                    yield item
                    total += 1

                to_refresh = []

        if len(to_refresh) > 0:
            filter_author = {"name": author_field,
                             "value": to_refresh}

            for item in update_items(filter_author):
                yield item
                total += 1

    logger.debug("Total eitems refreshed for identities fields {}".format(total))


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

        if not identities:
            continue

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

    logger.debug("Processed {} items identities ({} identities) from {}".format(
                 items_count, len(new_identities), connector_name))

    return identities_count


def enrich_items(ocean_backend, enrich_backend, events=False):
    total = 0

    if not events:
        total = enrich_backend.enrich_items(ocean_backend)
        enrich_backend.update_items(ocean_backend, enrich_backend)
    else:
        total = enrich_backend.enrich_events(ocean_backend)
    return total


def get_ocean_backend(backend_cmd, enrich_backend, no_incremental, filter_raw=None):
    """ Get the ocean backend configured to start from the last enriched date """

    if no_incremental:
        last_enrich = None
    else:
        last_enrich = get_last_enrich(backend_cmd, enrich_backend, filter_raw=filter_raw)

    logger.debug("Last enrichment: {}".format(last_enrich))

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
            if last_enrich:
                ocean_backend = connector[1](backend, from_date=last_enrich)
            else:
                ocean_backend = connector[1](backend)
    else:
        # We can have params for non perceval backends also
        params = enrich_backend.backend_params
        if params:
            try:
                date_pos = params.index('--from-date')
                last_enrich = str_to_datetime(params[date_pos + 1])
            except ValueError:
                pass
        if last_enrich:
            ocean_backend = connector[1](backend, from_date=last_enrich)
        else:
            ocean_backend = connector[1](backend)

    if filter_raw:
        ocean_backend.set_filter_raw(filter_raw)

    return ocean_backend


def do_studies(ocean_backend, enrich_backend, studies_args, retention_time=None):
    """Execute studies related to a given enrich backend. If `retention_time` is not None, the
    study data is deleted based on the number of minutes declared in `retention_time`.

    :param ocean_backend: backend to access raw items
    :param enrich_backend: backend to access enriched items
    :param retention_time: maximum number of minutes wrt the current date to retain the data
    :param studies_args: list of studies to be executed
    """
    for study in enrich_backend.studies:
        selected_studies = [(s['name'], s['params']) for s in studies_args if s['type'] == study.__name__]

        for (name, params) in selected_studies:
            data_source = enrich_backend.__class__.__name__.split("Enrich")[0].lower()
            logger.info("[{}] Starting study: {}, params {}".format(data_source, name, params))
            try:
                study(ocean_backend, enrich_backend, **params)
            except Exception as e:
                logger.error("[{}] Problem executing study {}, {}".format(data_source, name, e))
                raise e

            # identify studies which creates other indexes. If the study is onion,
            # it can be ignored since the index is recreated every week
            if name.startswith('enrich_onion'):
                continue

            index_params = [p for p in params if 'out_index' in p]

            for ip in index_params:
                index_name = params[ip]
                elastic = get_elastic(enrich_backend.elastic_url, index_name)

                elastic.delete_items(retention_time)


def enrich_backend(url, clean, backend_name, backend_params, cfg_section_name,
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
                   jenkins_rename_file=None,
                   unaffiliated_group=None, pair_programming=False,
                   node_regex=False, studies_args=None, es_enrich_aliases=None,
                   last_enrich_date=None, projects_json_repo=None, repo_labels=None):
    """ Enrich Ocean index """

    backend = None
    enrich_index = None

    if ocean_index or ocean_index_enrich:
        clean = False  # don't remove index, it could be shared

    if do_refresh_projects or do_refresh_identities:
        clean = False  # refresh works over the existing enriched items

    if not get_connector_from_name(backend_name):
        raise RuntimeError("Unknown backend {}".format(backend_name))
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
        # store the cfg section name in the enrich backend to recover the corresponding project name in projects.json
        enrich_backend.set_cfg_section_name(cfg_section_name)
        enrich_backend.set_from_date(last_enrich_date)
        if url_enrich:
            elastic_enrich = get_elastic(url_enrich, enrich_index, clean, enrich_backend, es_enrich_aliases)
        else:
            elastic_enrich = get_elastic(url, enrich_index, clean, enrich_backend, es_enrich_aliases)
        enrich_backend.set_elastic(elastic_enrich)
        if jenkins_rename_file and backend_name == "jenkins":
            enrich_backend.set_jenkins_rename_file(jenkins_rename_file)
        if unaffiliated_group:
            enrich_backend.unaffiliated_group = unaffiliated_group
        if pair_programming:
            enrich_backend.pair_programming = pair_programming
        if node_regex:
            enrich_backend.node_regex = node_regex

        # The filter raw is needed to be able to assign the project value to an enriched item
        # see line 544, grimoire_elk/enriched/enrich.py (fltr = eitem['origin'] + ' --filter-raw=' + self.filter_raw)
        if filter_raw:
            enrich_backend.set_filter_raw(filter_raw)

        enrich_backend.set_projects_json_repo(projects_json_repo)
        enrich_backend.set_repo_labels(repo_labels)
        ocean_backend = get_ocean_backend(backend_cmd, enrich_backend, no_incremental, filter_raw)

        if only_studies:
            logger.info("Running only studies (no SH and no enrichment)")
            do_studies(ocean_backend, enrich_backend, studies_args)
        elif do_refresh_projects:
            logger.info("Refreshing project field in {}".format(
                        anonymize_url(enrich_backend.elastic.index_url)))
            field_id = enrich_backend.get_field_unique_id()
            eitems = refresh_projects(enrich_backend)
            enrich_backend.elastic.bulk_upload(eitems, field_id)
        elif do_refresh_identities:

            author_attr = None
            author_values = None
            if author_id:
                author_attr = 'author_id'
                author_values = [author_id]
            elif author_uuid:
                author_attr = 'author_uuid'
                author_values = [author_uuid]

            logger.info("Refreshing identities fields in {}".format(
                        anonymize_url(enrich_backend.elastic.index_url)))

            field_id = enrich_backend.get_field_unique_id()
            eitems = refresh_identities(enrich_backend, author_attr, author_values)
            enrich_backend.elastic.bulk_upload(eitems, field_id)
        else:
            clean = False  # Don't remove ocean index when enrich
            elastic_ocean = get_elastic(url, ocean_index, clean, ocean_backend)
            ocean_backend.set_elastic(elastic_ocean)

            logger.debug("Adding enrichment data to {}".format(
                         anonymize_url(enrich_backend.elastic.index_url)))

            if db_sortinghat and enrich_backend.has_identities():
                # FIXME: This step won't be done from enrich in the future
                total_ids = load_identities(ocean_backend, enrich_backend)
                logger.debug("Total identities loaded {} ".format(total_ids))

            if only_identities:
                logger.debug("Only SH identities added. Enrich not done!")

            else:
                # Enrichment for the new items once SH update is finished
                if not events_enrich:
                    enrich_count = enrich_items(ocean_backend, enrich_backend)
                    if enrich_count is not None:
                        logger.debug("Total items enriched {} ".format(enrich_count))
                else:
                    enrich_count = enrich_items(ocean_backend, enrich_backend, events=True)
                    if enrich_count is not None:
                        logger.debug("Total events enriched {} ".format(enrich_count))
                if studies:
                    do_studies(ocean_backend, enrich_backend, studies_args)

    except Exception as ex:
        if backend:
            logger.error("Error enriching raw from {} ({}): {}".format(
                         backend_name, anonymize_url(backend.origin), ex), exc_info=True)
        else:
            logger.error("Error enriching raw {}".format(ex), exc_info=True)

    logger.info("[{}] Done enrichment for {}".format(backend_name, anonymize_url(backend.origin)))


def delete_orphan_unique_identities(es, sortinghat_db, current_data_source, active_data_sources):
    """Delete all unique identities which appear in SortingHat, but not in the IDENTITIES_INDEX.

    :param es: ElasticSearchDSL object
    :param sortinghat_db: instance of the SortingHat database
    :param current_data_source: current data source
    :param active_data_sources: list of active data sources
    """
    def get_uuids_in_index(target_uuids):
        """Find a set of uuids in IDENTITIES_INDEX and return them if exist.

        :param target_uuids: target uuids
        """
        page = es.search(
            index=IDENTITIES_INDEX,
            scroll="360m",
            size=SIZE_SCROLL_IDENTITIES_INDEX,
            body={
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "terms": {
                                    "sh_uuid": target_uuids
                                }
                            }
                        ]
                    }
                }
            }
        )

        hits = []
        if page['hits']['total'] != 0:
            hits = page['hits']['hits']

        return hits

    def delete_unique_identities(target_uuids):
        """Delete a list of uuids from SortingHat.

        :param target_uuids: uuids to be deleted
        """
        count = 0

        for uuid in target_uuids:
            success = SortingHat.remove_unique_identity(sortinghat_db, uuid)
            count = count + 1 if success else count

        return count

    def delete_identities(unique_ident, data_sources):
        """Remove the identities in non active data sources.

        :param unique_ident: unique identity object
        :param data_sources: target data sources
        """
        count = 0
        for ident in unique_ident.identities:
            if ident.source not in data_sources:
                success = SortingHat.remove_identity(sortinghat_db, ident.id)
                count = count + 1 if success else count

        return count

    def has_identities_in_data_sources(unique_ident, data_sources):
        """Check if a unique identity has identities in a set of data sources.

        :param unique_ident: unique identity object
        :param data_sources: target data sources
        """
        in_active = False
        for ident in unique_ident.identities:
            if ident.source in data_sources:
                in_active = True
                break

        return in_active

    deleted_unique_identities = 0
    deleted_identities = 0
    uuids_to_process = []

    # Collect all unique identities
    for unique_identity in SortingHat.unique_identities(sortinghat_db):

        # Remove a unique identity if all its identities are in non active data source
        if not has_identities_in_data_sources(unique_identity, active_data_sources):
            deleted_unique_identities += delete_unique_identities([unique_identity.uuid])
            continue

        # Remove the identities of non active data source for a given unique identity
        deleted_identities += delete_identities(unique_identity, active_data_sources)

        # Process only the unique identities that include the current data source, since
        # it may be that unique identities in other data source have not been
        # added yet to IDENTITIES_INDEX
        if not has_identities_in_data_sources(unique_identity, [current_data_source]):
            continue

        # Add the uuid to the list to check its existence in the IDENTITIES_INDEX
        uuids_to_process.append(unique_identity.uuid)

        # Process the uuids in block of SIZE_SCROLL_IDENTITIES_INDEX
        if len(uuids_to_process) != SIZE_SCROLL_IDENTITIES_INDEX:
            continue

        # Find which uuids to be processed exist in IDENTITIES_INDEX
        results = get_uuids_in_index(uuids_to_process)
        uuids_found = [item['_source']['sh_uuid'] for item in results]

        # Find the uuids which exist in SortingHat but not in IDENTITIES_INDEX
        orphan_uuids = set(uuids_to_process) - set(uuids_found)
        # Delete the orphan uuids from SortingHat
        deleted_unique_identities += delete_unique_identities(orphan_uuids)
        # Reset the list
        uuids_to_process = []

    # Check that no uuids have been left to process
    if uuids_to_process:
        # Find which uuids to be processed exist in IDENTITIES_INDEX
        results = get_uuids_in_index(uuids_to_process)
        uuids_found = [item['_source']['sh_uuid'] for item in results]

        # Find the uuids which exist in SortingHat but not in IDENTITIES_INDEX
        orphan_uuids = set(uuids_to_process) - set(uuids_found)

        # Delete the orphan uuids from SortingHat
        deleted_unique_identities += delete_unique_identities(orphan_uuids)

    logger.debug("[identities retention] Total orphan unique identities deleted from SH: {}".format(
                 deleted_unique_identities))
    logger.debug("[identities retention] Total identities in non-active data sources deleted from SH: {}".format(
                 deleted_identities))


def delete_inactive_unique_identities(es, sortinghat_db, before_date):
    """Select the unique identities not seen before `before_date` and
    delete them from SortingHat.

    :param es: ElasticSearchDSL object
    :param sortinghat_db: instance of the SortingHat database
    :param before_date: datetime str to filter the identities
    """
    page = es.search(
        index=IDENTITIES_INDEX,
        scroll="360m",
        size=SIZE_SCROLL_IDENTITIES_INDEX,
        body={
            "query": {
                "range": {
                    "last_seen": {
                        "lte": before_date
                    }
                }
            }
        }
    )

    sid = page['_scroll_id']
    scroll_size = page['hits']['total']

    if scroll_size == 0:
        logging.warning("[identities retention] No inactive identities found in {} after {}!".format(
                        IDENTITIES_INDEX, before_date))
        return

    count = 0

    while scroll_size > 0:
        for item in page['hits']['hits']:
            to_delete = item['_source']['sh_uuid']
            success = SortingHat.remove_unique_identity(sortinghat_db, to_delete)
            # increment the number of deleted identities only if the corresponding command was successful
            count = count + 1 if success else count

        page = es.scroll(scroll_id=sid, scroll='60m')
        sid = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])

    logger.debug("[identities retention] Total inactive identities deleted from SH: {}".format(count))


def retain_identities(retention_time, es_enrichment_url, sortinghat_db, data_source, active_data_sources):
    """Select the unique identities not seen before `retention_time` and
    delete them from SortingHat. Furthermore, it deletes also the orphan unique identities,
    those ones stored in SortingHat but not in IDENTITIES_INDEX.

    :param retention_time: maximum number of minutes wrt the current date to retain the identities
    :param es_enrichment_url: URL of the ElasticSearch where the enriched data is stored
    :param sortinghat_db: instance of the SortingHat database
    :param data_source: target data source (e.g., git, github, slack)
    :param active_data_sources: list of active data sources
    """
    before_date = get_diff_current_date(minutes=retention_time)
    before_date_str = before_date.isoformat()

    es = Elasticsearch([es_enrichment_url], timeout=120, max_retries=20, retry_on_timeout=True, verify_certs=False)

    # delete the unique identities which have not been seen after `before_date`
    delete_inactive_unique_identities(es, sortinghat_db, before_date_str)
    # delete the unique identities for a given data source which are not in the IDENTITIES_INDEX
    delete_orphan_unique_identities(es, sortinghat_db, data_source, active_data_sources)


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


def populate_identities_index(es_enrichment_url, enrich_index):
    """Save the identities currently in use in the index IDENTITIES_INDEX.

    :param es_enrichment_url: url of the ElasticSearch with enriched data
    :param enrich_index: name of the enriched index
    """
    class Mapping(BaseMapping):

        @staticmethod
        def get_elastic_mappings(es_major):
            """Get Elasticsearch mapping.

            :param es_major: major version of Elasticsearch, as string
            :returns:        dictionary with a key, 'items', with the mapping
            """

            mapping = """
            {
                "properties": {
                   "sh_uuid": {
                       "type": "keyword"
                   },
                   "last_seen": {
                       "type": "date"
                   }
                }
            }
            """

            return {"items": mapping}

    # identities index
    mapping_identities_index = Mapping()
    elastic_identities = get_elastic(es_enrichment_url, IDENTITIES_INDEX, mapping=mapping_identities_index)

    # enriched index
    elastic_enrich = get_elastic(es_enrichment_url, enrich_index)
    # collect mapping attributes in enriched index
    attributes = elastic_enrich.all_properties()
    # select attributes coming from SortingHat (*_uuid except git_uuid)
    sh_uuid_attributes = [attr for attr in attributes if attr.endswith('_uuid') and not attr.startswith('git_')]

    enriched_items = ElasticItems(None)
    enriched_items.elastic = elastic_enrich

    logger.debug("[identities-index] Start adding identities to {}".format(IDENTITIES_INDEX))

    identities = []
    for eitem in enriched_items.fetch(ignore_incremental=True):
        for sh_uuid_attr in sh_uuid_attributes:

            if sh_uuid_attr not in eitem:
                continue

            if not eitem[sh_uuid_attr]:
                continue

            identity = {
                'sh_uuid': eitem[sh_uuid_attr],
                'last_seen': datetime_utcnow().isoformat()
            }

            identities.append(identity)

            if len(identities) == elastic_enrich.max_items_bulk:
                elastic_identities.bulk_upload(identities, 'sh_uuid')
                identities = []

    if len(identities) > 0:
        elastic_identities.bulk_upload(identities, 'sh_uuid')

    logger.debug("[identities-index] End adding identities to {}".format(IDENTITIES_INDEX))
