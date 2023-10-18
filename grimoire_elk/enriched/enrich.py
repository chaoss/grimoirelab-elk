# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2023 Bitergia
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
#   Quan Zhou <quan@bitergia.com>
#   Miguel Ángel Fernández <mafesan@bitergia.com>
#
import datetime
import json
import functools
import logging
import requests
import sys
import time

from datetime import timedelta
from dateutil.relativedelta import relativedelta

import pkg_resources
from functools import lru_cache

from elasticsearch import Elasticsearch as ES, RequestsHttpConnection
from geopy.geocoders import Nominatim

from perceval.backend import find_signature_parameters
from grimoirelab_toolkit.datetime import datetime_utcnow, str_to_datetime

from ..elastic import ElasticSearch
from ..elastic_analyzer import Analyzer
from ..elastic_items import (ElasticItems,
                             HEADER_JSON)
from .study_ceres_onion import ESOnionConnector, onion_study
from .sortinghat_gelk import MULTI_ORG_NAMES
from .graal_study_evolution import (get_to_date,
                                    get_unique_repository)
from statsmodels.duration.survfunc import SurvfuncRight

from .utils import grimoire_con, METADATA_FILTER_RAW, REPO_LABELS, anonymize_url
from .. import __version__

logger = logging.getLogger(__name__)

try:
    from sortinghat.cli.client import (SortingHatClient,
                                       SortingHatClientError)
    from sortinghat.utils import generate_uuid

    from .sortinghat_gelk import SortingHat

    SORTINGHAT_LIBS = True
except ImportError:
    logger.info("SortingHat not available")
    SORTINGHAT_LIBS = False

UNKNOWN_PROJECT = 'unknown'
DEFAULT_PROJECT = 'Main'
DEFAULT_DB_USER = 'root'
CUSTOM_META_PREFIX = 'cm'
EXTRA_PREFIX = 'extra'
SH_UNKNOWN_VALUE = 'Unknown'


def metadata(func):
    """Add metadata to an item.

    Decorator that adds metadata to a given item such as
    the gelk revision used.

    """

    @functools.wraps(func)
    def decorator(self, *args, **kwargs):
        eitem = func(self, *args, **kwargs)
        metadata = {
            'metadata__gelk_version': self.gelk_version,
            'metadata__gelk_backend_name': self.__class__.__name__,
            'metadata__enriched_on': datetime_utcnow().isoformat()
        }
        eitem.update(metadata)
        return eitem

    return decorator


class Enrich(ElasticItems):
    analyzer = Analyzer
    sh_db = None
    kibiter_version = None
    RAW_FIELDS_COPY = ["metadata__updated_on", "metadata__timestamp",
                       "offset", "origin", "tag", "uuid"]
    KEYWORD_MAX_LENGTH = 1000  # this control allows to avoid max_bytes_length_exceeded_exception

    ONION_INTERVAL = seconds = 3600 * 24 * 7

    def __init__(self, db_sortinghat=None, json_projects_map=None, db_user='',
                 db_password='', db_host='', insecure=True, db_port=None, db_path=None,
                 db_ssl=False, db_verify_ssl=True, db_tenant=None):

        perceval_backend = None
        super().__init__(perceval_backend, insecure=insecure)
        self._connector_name = None
        self.sortinghat = False
        if db_user == '':
            db_user = DEFAULT_DB_USER
        if db_sortinghat and not SORTINGHAT_LIBS:
            raise RuntimeError("Sorting hat configured but libraries not available.")
        if db_sortinghat:
            if not Enrich.sh_db:
                client = SortingHatClient(host=db_host, port=db_port,
                                          path=db_path, ssl=db_ssl,
                                          verify_ssl=db_verify_ssl,
                                          user=db_user, password=db_password,
                                          tenant=db_tenant)
                client.connect()
                client.gqlc.logger.setLevel(logging.CRITICAL)
                Enrich.sh_db = client

            self.sortinghat = True

        self.prjs_map = None  # mapping beetween repositories and projects
        self.json_projects = None

        if json_projects_map:
            with open(json_projects_map) as data_file:
                self.json_projects = json.load(data_file)
                # If we have JSON projects always use them for mapping
                self.prjs_map = self.__convert_json_to_projects_map(self.json_projects)

        if self.prjs_map and self.json_projects:
            # logger.info("Comparing db and json projects")
            # self.__compare_projects_map(self.prjs_map, self.json_projects)
            pass

        self.studies = []

        self.requests = grimoire_con()
        self.elastic = None
        self.type_name = "items"  # type inside the index to store items enriched

        # To add the gelk version to enriched items
        self.gelk_version = __version__

        # params used to configure the backend
        # in perceval backends managed directly inside the backend
        self.backend_params = None
        # Label used during enrichment for identities without a known affiliation
        self.unaffiliated_group = 'Unknown'
        # Label used during enrichment for identities with no gender info
        self.unknown_gender = 'Unknown'

    def set_elastic_url(self, url):
        """ Elastic URL """
        self.elastic_url = url

    def set_elastic(self, elastic):
        self.elastic = elastic

    def set_params(self, params):
        from ..utils import get_connector_from_name

        self.backend_params = params
        backend_name = self.get_connector_name()
        # We can now create the perceval backend
        if not get_connector_from_name(backend_name):
            raise RuntimeError("Unknown backend {}".format(backend_name))
        connector = get_connector_from_name(backend_name)
        klass = connector[3]  # BackendCmd for the connector
        if not klass:
            # Non perceval backends can not be configured
            return

        backend_cmd = klass(*self.backend_params)
        parsed_args = vars(backend_cmd.parsed_args)
        init_args = find_signature_parameters(backend_cmd.BACKEND,
                                              parsed_args)
        backend_cmd.backend = backend_cmd.BACKEND(**init_args)
        self.perceval_backend = backend_cmd.backend

    def update_items(self, ocean_backend, enrich_backend):
        """Perform update operations over an enriched index, just after the enrichment
        It must be redefined in the enriched connectors"""

        return

    def __convert_json_to_projects_map(self, json):
        """ Convert JSON format to the projects map format
        map[ds][repository] = project
        If a repository is in several projects assign to leaf
        Check that all JSON data is in the database

        :param json: data with the projects to repositories mapping
        :returns: the repositories to projects mapping per data source
        """
        ds_repo_to_prj = {}

        # Sent the unknown project to the end of the list.
        # This change is needed to avoid assigning repositories to
        # the `Main` project when they exist in the `unknown`
        # section and in other sections too.
        project_names = list(json.keys())
        if UNKNOWN_PROJECT in json:
            project_names.remove(UNKNOWN_PROJECT)
            project_names.append(UNKNOWN_PROJECT)

        for project in project_names:
            for ds in json[project]:
                if ds == "meta":
                    continue  # not a real data source
                if ds not in ds_repo_to_prj:
                    if ds not in ds_repo_to_prj:
                        ds_repo_to_prj[ds] = {}
                for repo in json[project][ds]:
                    repo, _ = self.extract_repo_tags(repo)
                    if repo in ds_repo_to_prj[ds]:
                        if project == ds_repo_to_prj[ds][repo]:
                            logger.debug("Duplicated repo: {} {} {}".format(ds, repo, project))
                        else:
                            if len(project.split(".")) > len(ds_repo_to_prj[ds][repo].split(".")):
                                logger.debug("Changed repo project because we found a leaf: {} leaf vs "
                                             "{} ({}, {})".format(project, ds_repo_to_prj[ds][repo], repo, ds))
                                ds_repo_to_prj[ds][repo] = project
                    else:
                        ds_repo_to_prj[ds][repo] = project
        return ds_repo_to_prj

    def __compare_projects_map(self, db, json):
        # Compare the projects coming from db and from a json file in eclipse
        ds_map_db = {}
        ds_map_json = {
            "git": "scm",
            "pipermail": "mls",
            "gerrit": "scr",
            "bugzilla": "its"
        }
        for ds in ds_map_json:
            ds_map_db[ds_map_json[ds]] = ds

        db_projects = []
        dss = db.keys()

        # Check that all db data is in the JSON file
        for ds in dss:
            for repository in db[ds]:
                # A repository could be in more than one project. But we get only one.
                project = db[ds][repository]
                if project not in db_projects:
                    db_projects.append(project)
                if project not in json:
                    logger.error("Project not found in JSON {}".format(project))
                    raise Exception("Project not found in JSON {}".format(project))
                else:
                    if ds == 'mls':
                        repo_mls = repository.split("/")[-1]
                        repo_mls = repo_mls.replace(".mbox", "")
                        repository = 'https://dev.eclipse.org/mailman/listinfo/' + repo_mls
                    if ds_map_db[ds] not in json[project]:
                        logger.error("db repository not found in json {}".format(repository))
                    elif repository not in json[project][ds_map_db[ds]]:
                        logger.error("db repository not found in json {}".format(repository))

        for project in json.keys():
            if project not in db_projects:
                logger.debug("JSON project {} not found in db".format(project))

        # Check that all JSON data is in the database
        for project in json:
            for ds in json[project]:
                if ds not in ds_map_json:
                    # meta
                    continue
                for repo in json[project][ds]:
                    if ds == 'pipermail':
                        repo_mls = repo.split("/")[-1]
                        repo = "/mnt/mailman_archives/%s.mbox/%s.mbox" % (repo_mls, repo_mls)
                    if repo in db[ds_map_json[ds]]:
                        # print("Found ", repo, ds)
                        pass
                    else:
                        logger.debug("Not found repository in db {} {}".format(repo, ds))

        logger.debug("Number of db projects: {}".format(db_projects))
        logger.debug("Number of json projects: {} (>={})".format(json.keys(), db_projects))

    def get_field_unique_id(self):
        """ Field in the raw item with the unique id """
        return "uuid"

    def get_field_event_unique_id(self):
        """ Field in the rich event with the unique id """
        raise NotImplementedError

    @metadata
    def get_rich_item(self, item):
        """ Create a rich item from the raw item """
        raise NotImplementedError

    def get_rich_events(self, item):
        """ Create rich events from the raw item """
        raise NotImplementedError

    def enrich_events(self, items):
        return self.enrich_items(items, events=True)

    def enrich_items(self, ocean_backend, events=False):
        """
        Enrich the items fetched from ocean_backend generator
        generating enriched items/events which are uploaded to the Elasticsearch index for
        this Enricher (self).

        :param ocean_backend: Ocean backend object to fetch the items from
        :param events: enrich items or enrich events
        :return: total number of enriched items/events uploaded to Elasticsearch
        """

        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        items = ocean_backend.fetch()

        url = self.elastic.get_bulk_url()

        logger.debug("Adding items to {} (in {} packs)".format(anonymize_url(url), max_items))

        if events:
            logger.debug("Adding events items")

        for item in items:
            if current >= max_items:
                try:
                    total += self.elastic.safe_put_bulk(url, bulk_json)
                    json_size = sys.getsizeof(bulk_json) / (1024 * 1024)
                    logger.debug("Added {} items to {} ({:.2f} MB)".format(total, anonymize_url(url), json_size))
                except UnicodeEncodeError:
                    # Why is requests encoding the POST data as ascii?
                    logger.error("Unicode error in enriched items")
                    logger.debug(bulk_json)
                    safe_json = str(bulk_json.encode('ascii', 'ignore'), 'ascii')
                    total += self.elastic.safe_put_bulk(url, safe_json)
                bulk_json = ""
                current = 0

            if not events:
                rich_item = self.get_rich_item(item)
                data_json = json.dumps(rich_item)
                bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                             (item[self.get_field_unique_id()])
                bulk_json += data_json + "\n"  # Bulk document
                current += 1
            else:
                rich_events = self.get_rich_events(item)
                for rich_event in rich_events:
                    data_json = json.dumps(rich_event)
                    bulk_json += '{"index" : {"_id" : "%s_%s" } }\n' % \
                                 (item[self.get_field_unique_id()],
                                  rich_event[self.get_field_event_unique_id()])
                    bulk_json += data_json + "\n"  # Bulk document
                    current += 1

        if current > 0:
            total += self.elastic.safe_put_bulk(url, bulk_json)

        return total

    def add_repository_labels(self, eitem):
        """Add labels to the enriched item"""

        eitem[REPO_LABELS] = self.repo_labels

    def add_metadata_filter_raw(self, eitem):
        """Add filter raw information to the enriched item"""

        eitem[METADATA_FILTER_RAW] = self.filter_raw

    def get_connector_name(self):
        """ Find the name for the current connector """
        from ..utils import get_connector_name
        if not self._connector_name:
            self._connector_name = get_connector_name(type(self))
        return self._connector_name

    def get_sh_backend_name(self):
        """Retrieve the backend name for SortingHat identities."""

        return self.get_connector_name()

    def get_field_author(self):
        """ Field with the author information """
        raise NotImplementedError

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "metadata__updated_on"

    def get_identities(self, item):
        """ Return the identities from an item """
        raise NotImplementedError

    def has_identities(self):
        """ Return whether the enriched items contains identities """

        return True

    def get_email_domain(self, email):
        domain = None
        try:
            domain = email.split("@")[1]
        except (IndexError, AttributeError):
            # logger.warning("Bad email format: %s" % (identity['email']))
            pass
        return domain

    def get_identity_domain(self, identity):
        domain = None
        if 'email' in identity and identity['email']:
            domain = self.get_email_domain(identity['email'])
        return domain

    def get_item_id(self, eitem):
        """ Return the item_id linked to this enriched eitem """

        # If possible, enriched_item and item will have the same id
        return eitem["_id"]

    def get_last_update_from_es(self, _filters=[]):

        last_update = self.elastic.get_last_date(self.get_incremental_date(), _filters)

        return last_update

    def get_last_offset_from_es(self, _filters=[]):
        # offset is always the field name from perceval
        last_update = self.elastic.get_last_offset("offset", _filters)

        return last_update

    # def get_elastic_mappings(self):
    #     """ Mappings for enriched indexes """
    #
    #     mapping = '{}'
    #     return {"items": mapping}

    def get_elastic_analyzers(self):
        """ Custom analyzers for our indexes  """

        analyzers = '{}'

        return analyzers

    def get_grimoire_fields(self, creation_date, item_name):
        """ Return common grimoire fields for all data sources """

        grimoire_date = None
        if isinstance(creation_date, datetime.datetime):
            grimoire_date = creation_date.isoformat()
        else:
            try:
                grimoire_date = str_to_datetime(creation_date).isoformat()
            except Exception as ex:
                pass

        name = "is_" + self.get_connector_name() + "_" + item_name

        return {
            "grimoire_creation_date": grimoire_date,
            name: 1
        }

    # Project field enrichment
    def get_project_repository(self, eitem):
        """
            Get the repository name used for mapping to project name from
            the enriched item.
            To be implemented for each data source
        """
        return ''

    @classmethod
    def add_project_levels(cls, project):
        """ Add project sub levels extra items """

        eitem_path = ''
        eitem_project_levels = {}

        if project is not None:
            subprojects = project.split('.')
            for i in range(0, len(subprojects)):
                if i > 0:
                    eitem_path += "."
                eitem_path += subprojects[i]
                eitem_project_levels['project_' + str(i + 1)] = eitem_path

        return eitem_project_levels

    def find_item_project(self, eitem):
        """
        Find the project for a enriched item
        :param eitem: enriched item for which to find the project
        :return: the project entry (a dictionary)
        """
        # get the data source name relying on the cfg section name, if null use the connector name
        ds_name = self.cfg_section_name if self.cfg_section_name else self.get_connector_name()

        try:
            # retrieve the project which includes the repo url in the projects.json,
            # the variable `projects_json_repo` is passed from mordred to ELK when
            # iterating over the repos in the projects.json, (see: param
            # `projects_json_repo` in the functions elk.feed_backend and
            # elk.enrich_backend)
            if self.projects_json_repo:
                project = self.prjs_map[ds_name][self.projects_json_repo]
            # if `projects_json_repo` (e.g., AOC study), use the
            # method `get_project_repository` (defined in each enricher)
            else:
                repository = self.get_project_repository(eitem)
                project = self.prjs_map[ds_name][repository]
        # With the introduction of `projects_json_repo` the code in the
        # except should be unreachable, and could be removed
        except KeyError:
            # logger.warning("Project not found for repository %s (data source: %s)", repository, ds_name)
            project = None

            if self.filter_raw:
                fltr = eitem['origin'] + ' --filter-raw=' + self.filter_raw
                if ds_name in self.prjs_map and fltr in self.prjs_map[ds_name]:
                    project = self.prjs_map[ds_name][fltr]
            elif ds_name in self.prjs_map:
                # this code is executed to retrieve the project of private repositories (in particular Git ones)
                # the URLs in the prjs_map are retrieved, anonymized and compared with the value
                # returned by `get_project_repository`
                repository = self.get_project_repository(eitem)
                for r in self.prjs_map[ds_name]:
                    anonymized_repo = anonymize_url(r)
                    if repository == anonymized_repo:
                        project = self.prjs_map[ds_name][r]
                        break

            if project == UNKNOWN_PROJECT:
                return None
            if project:
                return project

            # Try to use always the origin in any case
            if 'origin' in eitem:
                if ds_name in self.prjs_map and eitem['origin'] in self.prjs_map[ds_name]:
                    project = self.prjs_map[ds_name][eitem['origin']]
                elif ds_name in self.prjs_map:
                    # Try to find origin as part of the keys
                    for ds_repo in self.prjs_map[ds_name]:
                        ds_repo = str(ds_repo)  # discourse has category_id ints
                        if eitem['origin'] in ds_repo:
                            project = self.prjs_map[ds_name][ds_repo]
                            break

        if project == UNKNOWN_PROJECT:
            project = None

        return project

    def get_item_project(self, eitem):
        """
        Get the project name related to the eitem
        :param eitem: enriched item for which to find the project
        :return: a dictionary with the project data
        """
        eitem_project = {}
        project = self.find_item_project(eitem)

        if project is None:
            project = DEFAULT_PROJECT

        eitem_project = {"project": project}
        # Time to add the project levels: eclipse.platform.releng.aggregator
        eitem_project.update(self.add_project_levels(project))

        # And now time to add the metadata
        eitem_project.update(self.get_item_metadata(eitem))

        return eitem_project

    def get_item_metadata(self, eitem):
        """
        In the projects.json file, inside each project, there is a field called "meta" which has a
        dictionary with fields to be added to the enriched items for this project.

        This fields must be added with the prefix cm_ (custom metadata).

        This method fetch the metadata fields for the project in which the eitem is included.

        :param eitem: enriched item to search metadata for
        :return: a dictionary with the metadata fields
        """

        eitem_metadata = {}

        # Get the project entry for the item, which includes the metadata
        project = self.find_item_project(eitem)

        if project and 'meta' in self.json_projects[project]:
            meta_fields = self.json_projects[project]['meta']
            if isinstance(meta_fields, dict):
                eitem_metadata = {CUSTOM_META_PREFIX + "_" + field: value for field, value in meta_fields.items()}

        return eitem_metadata

    # Sorting Hat stuff to be moved to SortingHat class
    def get_sh_identity(self, item, identity_field):
        """ Empty identity. Real implementation in each data source. """
        identity = {}
        for field in ['name', 'email', 'username']:
            identity[field] = None
        return identity

    @staticmethod
    def get_main_enrollments(enrollments):
        """ Get the main enrollment given a list of enrollments.
        If the enrollment contains :: the main one is the first part.

        For example:
        - Enrollment: Chaoss::Eng
        - Main: Chaoss

        If there is more than one, it will return ordered alphabetically.
        """
        main_orgs = list(map(lambda x: x.split("::")[0], enrollments))
        main_orgs = sorted(list(set(main_orgs)))

        return main_orgs

    @staticmethod
    def remove_prefix_enrollments(enrollments):
        """ Remove the prefix `::` of the enrollments.

        :param enrollments: list of enrollments
        :return: list of enrollments without prefix
        """
        enrolls = [enroll.split("::")[1] if "::" in enroll else
                   enroll for enroll in enrollments]
        enrolls_unique = sorted(list(set(enrolls)))

        return enrolls_unique

    def __get_item_sh_fields_empty(self, rol, undefined=False):
        """ Return a SH identity with all fields to empty_field """
        # If empty_field is None, the fields do not appear in index patterns
        empty_field = '' if not undefined else '-- UNDEFINED --'
        return {
            rol + "_id": empty_field,
            rol + "_uuid": empty_field,
            rol + "_name": empty_field,
            rol + "_user_name": empty_field,
            rol + "_domain": empty_field,
            rol + "_gender": empty_field,
            rol + "_gender_acc": None,
            rol + "_org_name": empty_field,
            rol + "_bot": False,
            rol + MULTI_ORG_NAMES: [empty_field]
        }

    def get_item_no_sh_fields(self, identity, rol):
        """ Create an item with reasonable data when SH is not enabled """

        username = identity.get('username', '')
        email = identity.get('email', '')
        name = identity.get('name', '')
        backend_name = self.get_sh_backend_name()

        if not (username or email or name):
            return self.__get_item_sh_fields_empty(rol)

        uuid = self.generate_uuid(backend_name, email=email,
                                  name=name, username=username)
        return {
            rol + "_id": uuid,
            rol + "_uuid": uuid,
            rol + "_name": name,
            rol + "_user_name": username,
            rol + "_domain": self.get_identity_domain(identity),
            rol + "_gender": self.unknown_gender,
            rol + "_gender_acc": None,
            rol + "_org_name": self.unaffiliated_group,
            rol + "_bot": False,
            rol + MULTI_ORG_NAMES: [self.unaffiliated_group]
        }

    def get_individual_fields(self, individual, sh_id=None, item_date=None, rol='author'):
        """ Get standard SH fields from a SH identity """

        eitem_sh = self.__get_item_sh_fields_empty(rol)

        eitem_sh[rol + "_id"] = sh_id
        eitem_sh[rol + "_uuid"] = individual['mk']

        profile = individual['profile']
        eitem_sh[rol + "_name"] = profile.get('name', eitem_sh[rol + "_name"])
        email = profile.get('email', None)
        eitem_sh[rol + "_domain"] = self.get_email_domain(email)
        eitem_sh[rol + "_gender"] = profile.get('gender', self.unknown_gender)
        eitem_sh[rol + "_gender_acc"] = profile.get('genderAcc', 0)
        eitem_sh[rol + "_bot"] = profile.get('isBot', False)

        multi_enrolls = self.get_sh_item_multi_enrollments(individual['enrollments'], item_date)
        main_enrolls = self.get_main_enrollments(multi_enrolls)
        all_enrolls = list(set(main_enrolls + multi_enrolls))
        eitem_sh[rol + MULTI_ORG_NAMES] = self.remove_prefix_enrollments(all_enrolls)
        eitem_sh[rol + "_org_name"] = main_enrolls[0]

        return eitem_sh

    def get_item_sh_fields(self, identity=None, item_date=None, sh_id=None,
                           rol='author'):
        """ Get standard SH fields from a SH identity """

        eitem_sh = self.__get_item_sh_fields_empty(rol)

        if identity:
            sh_item = self.get_sh_item_from_identity(identity, self.get_sh_backend_name())
            eitem_sh[rol + "_id"] = sh_item.get('id', '')
            eitem_sh[rol + "_uuid"] = sh_item.get('uuid', '')
            eitem_sh[rol + "_name"] = identity.get('name', '')
            eitem_sh[rol + "_user_name"] = identity.get('username', '')
            eitem_sh[rol + "_domain"] = self.get_identity_domain(identity)
        elif sh_id:
            # Use the SortingHat id to get the identity
            sh_item = self.get_sh_item_from_id(sh_id)
            eitem_sh[rol + "_id"] = sh_id
            eitem_sh[rol + "_uuid"] = sh_item.get('uuid', '')
        else:
            # No data to get a SH identity. Return an empty one.
            return eitem_sh

        # If the identity does not exist return an empty identity
        if rol + "_uuid" not in eitem_sh or not eitem_sh[rol + "_uuid"]:
            return self.__get_item_sh_fields_empty(rol, undefined=True)

        if 'profile' in sh_item and sh_item['profile']:
            profile = sh_item['profile']
            # If name not in profile, keep its old value (should be empty or identity's name field value)
            eitem_sh[rol + "_name"] = profile.get('name', eitem_sh[rol + "_name"])

            email = profile.get('email', None)
            eitem_sh[rol + "_domain"] = self.get_email_domain(email)

            eitem_sh[rol + "_gender"] = profile.get('gender', self.unknown_gender)
            eitem_sh[rol + "_gender_acc"] = profile.get('genderAcc', 0)
            eitem_sh[rol + "_bot"] = profile.get('isBot', False)

        # Ensure we always write gender fields
        if not eitem_sh.get(rol + "_gender"):
            eitem_sh[rol + "_gender"] = self.unknown_gender
            eitem_sh[rol + "_gender_acc"] = 0

        multi_enrolls = self.get_sh_item_multi_enrollments(sh_item['enrollments'], item_date)
        main_enrolls = self.get_main_enrollments(multi_enrolls)
        all_enrolls = list(set(main_enrolls + multi_enrolls))
        eitem_sh[rol + MULTI_ORG_NAMES] = self.remove_prefix_enrollments(all_enrolls)
        eitem_sh[rol + "_org_name"] = main_enrolls[0]

        return eitem_sh

    @lru_cache(4096)
    def get_sh_item_from_id(self, sh_id):
        """Get all the identity information from SortingHat using the individual id"""

        sh_item = {}

        try:
            individual = self.get_entity(sh_id)
            if not individual:
                msg = "Individual not found given the following id: {}".format(sh_id)
                logger.debug(msg)
                return sh_item
            uuid = individual['mk']
        except Exception as ex:
            msg = "Error getting individual {} from SortingHat: {}".format(sh_id, ex)
            logger.error(msg)
            return sh_item

        # Fill the information needed with the identity, individual and profile
        sh_item['id'] = sh_id
        sh_item['uuid'] = uuid
        sh_item['profile'] = individual['profile']
        sh_item['enrollments'] = individual['enrollments']

        return sh_item

    def get_sh_item_from_identity(self, identity, backend_name):
        identity_tuple = tuple(identity.items())
        sh_item = self.get_sh_item_from_identity_cache(identity_tuple, backend_name)
        return sh_item

    @lru_cache(4096)
    def get_sh_item_from_identity_cache(self, identity_tuple, backend_name):
        """Get a SortingHat item with all the information related with an identity"""
        sh_item = {}
        iden = {}

        # Convert the identity to dict again
        identity = dict((x, y) for x, y in identity_tuple)

        for field in ['email', 'name', 'username']:
            iden[field] = identity.get(field)

        if not iden['name'] and not iden['email'] and not iden['username']:
            logger.warning("Name, email and username are none in {}".format(backend_name))
            return sh_item

        identity_id = self.generate_uuid(backend_name,
                                         email=iden['email'],
                                         name=iden['name'],
                                         username=iden['username'])

        try:
            individual = self.get_entity(identity_id)
            if not individual:
                msg = "Individual not found given the following identity: {}".format(identity_id)
                logger.debug(msg)
                return sh_item

            for indv_identity in individual['identities']:
                if indv_identity['uuid'] == identity_id:
                    identity_sh = indv_identity
                    break
            else:
                msg = "Identity {} not found in individual returned by SortingHat.".format(identity)
                logger.error(msg)
                return sh_item
        except SortingHatClientError:
            msg = "None Identity found {}, identity: {}".format(backend_name, identity)
            logger.debug(msg)
            return sh_item
        except UnicodeEncodeError:
            msg = "UnicodeEncodeError {}, identity: {}".format(backend_name, identity)
            logger.error(msg)
            return sh_item
        except Exception as ex:
            msg = "Unknown error getting identity from SortingHat, {}, {}, {}".format(ex, backend_name, identity)
            logger.error(msg)
            return sh_item

        # Fill the information needed with the identity, individual and profile
        sh_item['id'] = identity_sh['uuid']
        sh_item['uuid'] = individual['mk']
        sh_item['name'] = identity_sh['name']
        sh_item['username'] = identity_sh['username']
        sh_item['email'] = identity_sh['email']
        sh_item['profile'] = individual['profile']
        sh_item['enrollments'] = individual['enrollments']

        return sh_item

    def get_sh_item_multi_enrollments(self, enrollments, item_date_str):
        """ Get the enrollments for the uuid when the item was done """

        enrolls = []
        enrollments = enrollments if enrollments else []

        if enrollments:
            if item_date_str:
                item_date = str_to_datetime(item_date_str)
            else:
                item_date = None

            # item_date must be offset-naive (utc)
            if item_date and item_date.tzinfo:
                item_date = (item_date - item_date.utcoffset()).replace(tzinfo=None)

        for enrollment in enrollments:
            group = enrollment['group']
            if not item_date:
                if group['type'] == 'team' and group['parentOrg']:
                    name = "{}::{}".format(group['parentOrg']['name'], group['name'])
                else:
                    name = group['name']
                enrolls.append(name)
            elif str_to_datetime(enrollment['start']).isoformat() <= item_date.isoformat() \
                    <= str_to_datetime(enrollment['end']).isoformat():
                if group['type'] == 'team' and group['parentOrg']:
                    name = "{}::{}".format(group['parentOrg']['name'], group['name'])
                else:
                    name = group['name']
                enrolls.append(name)
        if not enrolls:
            enrolls.append(self.unaffiliated_group)

        return enrolls

    def get_item_sh_from_id(self, eitem, roles=None, individuals=None):
        # Get the SH fields from the data in the enriched item

        eitem_sh = {}  # Item enriched

        author_field = self.get_field_author()
        if not author_field:
            return eitem_sh
        sh_id_author = None

        if not roles:
            roles = [author_field]

        date = eitem[self.get_field_date()]
        for rol in roles:
            if rol + "_id" not in eitem:
                # For example assignee in github it is usual that it does not appears
                logger.debug("Enriched index does not include SH ids for {}_id. Can not refresh it.".format(rol))
                continue
            sh_id = eitem[rol + "_id"]
            if not sh_id:
                logger.debug("{}_id is None".format(rol))
                continue
            if rol == author_field:
                sh_id_author = sh_id
            individual = self.find_individual(individuals, sh_id)
            if not individual:
                logger.debug(f"Individual {sh_id} not found.")
                continue
            eitem_sh.update(self.get_individual_fields(individual=individual,
                                                       sh_id=sh_id,
                                                       item_date=date,
                                                       rol=rol))

        # Add the author field common in all data sources
        rol_author = 'author'
        if sh_id_author and author_field != rol_author:
            individual = self.find_individual(individuals, sh_id_author)
            if individual:
                eitem_sh.update(self.get_individual_fields(individual=individual,
                                                           sh_id=sh_id_author,
                                                           item_date=date,
                                                           rol=rol_author))
            else:
                logger.debug(f"Individual {sh_id_author} not found.")

        return eitem_sh

    def get_item_sh_meta_fields(self, eitem, roles=None, suffixes=None, non_authored_prefix=None, individuals=None):
        """Get the SH meta fields from the data in the enriched item."""

        eitem_meta_sh = {}  # Item enriched

        date = eitem[self.get_field_date()]

        for rol in roles:
            if rol + "_uuids" not in eitem:
                continue
            sh_uuids = eitem[rol + "_uuids"]
            if not sh_uuids:
                logger.debug("{}_uuids is None".format(rol))
                continue

            for sh_uuid in sh_uuids:
                individual = self.find_individual(individuals, sh_uuid)
                if not individual:
                    logger.debug(f"Individual {sh_uuid} not found.")
                    continue
                sh_fields = self.get_individual_fields(individual=individual, sh_id=sh_uuid, item_date=date, rol=rol)

                self.add_meta_fields(eitem, eitem_meta_sh, sh_fields, rol, sh_uuid, suffixes, non_authored_prefix)

        return eitem_meta_sh

    @staticmethod
    def find_individual(individuals, sh_id):
        if not individuals:
            return None
        for indiv in individuals:
            if sh_id == indiv['mk']:
                return indiv
            for identity in indiv['identities']:
                if sh_id == identity['uuid']:
                    return indiv
        return None

    def add_meta_fields(self, eitem, meta_eitem, sh_fields, rol, uuid, suffixes, non_authored_prefix):
        def add_non_authored_fields(author_uuid, uuid, new_eitem, new_list, non_authored_field):
            if author_uuid == uuid:
                non_authored = []
            else:
                non_authored = new_list
            new_eitem[non_authored_field] = non_authored

        for suffix in suffixes:
            field = rol + suffix[:-1]
            if suffix == "_org_names":
                field = rol + "_multi" + suffix

            new_list = sh_fields[field]
            if type(new_list) != list:
                new_list = [new_list]

            try:
                meta_eitem[rol + suffix] += new_list
            except KeyError:
                meta_eitem[rol + suffix] = new_list

            if non_authored_prefix:
                non_authored_field = non_authored_prefix + rol + suffix
                add_non_authored_fields(eitem['author_uuid'], uuid, meta_eitem, new_list,
                                        non_authored_field)
        return meta_eitem

    def get_users_data(self, item):
        """ If user fields are inside the global item dict """
        if 'data' in item:
            users_data = item['data']
        else:
            # the item is directly the data (kitsune answer)
            users_data = item

        return users_data

    def get_item_sh(self, item, roles=None, date_field=None):
        """
        Add sorting hat enrichment fields for different roles

        If there are no roles, just add the author fields.

        """
        eitem_sh = {}  # Item enriched

        author_field = self.get_field_author()

        if not roles:
            roles = [author_field]

        if not date_field:
            item_date = item[self.get_field_date()]
        else:
            item_date = item[date_field]

        users_data = self.get_users_data(item)

        for rol in roles:
            if rol in users_data:
                identity = self.get_sh_identity(item, rol)
                if self.sortinghat:
                    sh_fields = self.get_item_sh_fields(identity, item_date, rol=rol)
                else:
                    sh_fields = self.get_item_no_sh_fields(identity, rol)

                eitem_sh.update(sh_fields)

                if not eitem_sh[rol + '_org_name']:
                    eitem_sh[rol + '_org_name'] = SH_UNKNOWN_VALUE

                if not eitem_sh[rol + '_name']:
                    eitem_sh[rol + '_name'] = SH_UNKNOWN_VALUE

                if not eitem_sh[rol + '_user_name']:
                    eitem_sh[rol + '_user_name'] = SH_UNKNOWN_VALUE

        # Add the author field common in all data sources
        rol_author = 'author'
        if author_field in users_data and author_field != rol_author:
            identity = self.get_sh_identity(item, author_field)
            if self.sortinghat:
                sh_fields = self.get_item_sh_fields(identity, item_date, rol=rol_author)
            else:
                sh_fields = self.get_item_no_sh_fields(identity, rol_author)
            eitem_sh.update(sh_fields)

            if not eitem_sh['author_org_name']:
                eitem_sh['author_org_name'] = SH_UNKNOWN_VALUE

            if not eitem_sh['author_name']:
                eitem_sh['author_name'] = SH_UNKNOWN_VALUE

            if not eitem_sh['author_user_name']:
                eitem_sh['author_user_name'] = SH_UNKNOWN_VALUE

        return eitem_sh

    def generate_uuid(self, source, email=None, name=None, username=None):
        """
        Generate UUID from identity fields.
        Force empty fields to None, the same way add_identity works.
        """
        args = {
            "email": email,
            "name": name,
            "source": source,
            "username": username
        }
        args_without_empty = {k: v for k, v in args.items() if v}
        return generate_uuid(**args_without_empty)

    @lru_cache(4096)
    def get_entity(self, id):
        return SortingHat.get_entity(self.sh_db, id)

    @lru_cache(4096)
    def is_bot(self, uuid):
        return SortingHat.is_bot(self.sh_db, uuid)

    @lru_cache(4096)
    def get_enrollments(self, uuid):
        return SortingHat.get_enrollments(self.sh_db, uuid)

    @lru_cache(4096)
    def get_unique_identity(self, uuid):
        return SortingHat.get_unique_identity(self.sh_db, uuid)

    @lru_cache(4096)
    def get_uuid_from_id(self, sh_id):
        """ Get the SH identity uuid from the id """
        return SortingHat.get_uuid_from_id(self.sh_db, sh_id)

    def add_sh_identities(self, identities):
        SortingHat.add_identities(self.sh_db, identities,
                                  self.get_sh_backend_name())

    @lru_cache(4096)
    def add_sh_identity_cache(self, identity_tuple):
        """Cache add_sh_identity calls. Identity must be in tuple format"""

        identity = dict((x, y) for x, y in identity_tuple)
        self.add_sh_identity(identity)

    def add_sh_identity(self, identity):
        SortingHat.add_identity(self.sh_db, identity,
                                self.get_sh_backend_name())

    def copy_raw_fields(self, copy_fields, source, target):
        """Copy fields from item to enriched item."""

        for f in copy_fields:
            if f in source:
                target[f] = source[f]
            else:
                target[f] = None

    def enrich_onion(self, enrich_backend, alias, in_index, out_index, data_source,
                     contribs_field, timeframe_field, sort_on_field,
                     seconds=ONION_INTERVAL, no_incremental=False):

        log_prefix = "[" + data_source + "] study onion"

        logger.info("{}  starting study - Input: {} Output: {}".format(log_prefix, in_index, out_index))

        # Creating connections
        es = ES([enrich_backend.elastic.url], retry_on_timeout=True, timeout=100,
                verify_certs=self.elastic.requests.verify, connection_class=RequestsHttpConnection)

        in_conn = ESOnionConnector(es_conn=es, es_index=in_index,
                                   contribs_field=contribs_field,
                                   timeframe_field=timeframe_field,
                                   sort_on_field=sort_on_field)
        out_conn = ESOnionConnector(es_conn=es, es_index=out_index,
                                    contribs_field=contribs_field,
                                    timeframe_field=timeframe_field,
                                    sort_on_field=sort_on_field,
                                    read_only=False)

        if not in_conn.exists():
            logger.info("{} missing index {}".format(log_prefix, in_index))
            return

        # Check last execution date
        latest_date = None
        if out_conn.exists():
            latest_date = out_conn.latest_enrichment_date()

        if latest_date:
            logger.info("{} Latest enrichment date: {}".format(log_prefix, latest_date.isoformat()))
            update_after = latest_date + timedelta(seconds=seconds)
            logger.info("{} update after date: {}".format(log_prefix, update_after.isoformat()))
            if update_after >= datetime_utcnow():
                logger.info("{} too soon to update. Next update will be at {}".format(
                            log_prefix, update_after.isoformat()))
                return

        # Onion currently does not support incremental option
        logger.info("{} Creating out ES index".format(log_prefix))
        # Initialize out index
        if not self.elastic.is_legacy():
            filename = pkg_resources.resource_filename('grimoire_elk', 'enriched/mappings/onion_es7.json')
        else:
            filename = pkg_resources.resource_filename('grimoire_elk', 'enriched/mappings/onion.json')

        out_conn.create_index(filename, delete=out_conn.exists())

        onion_study(in_conn=in_conn, out_conn=out_conn, data_source=data_source)

        # Create alias if output index exists (index is always created from scratch, so
        # alias need to be created each time)
        if out_conn.exists() and not out_conn.exists_alias(out_index, alias):
            logger.info("{} Creating alias: {}".format(log_prefix, alias))
            out_conn.create_alias(alias)

        logger.info("{} end".format(log_prefix))

    def enrich_extra_data(self, ocean_backend, enrich_backend, json_url, target_index=None):
        """
        This study enables setting/removing extra fields on/from a target index. For example if a use case
        requires tagging specific documents in an index with extra fields, like tagging all kernel
        maintainers with an extra attribute such as "maintainer = True".

        The extra data to be added/removed is passed via a JSON (`json_url`) and contains:
         - a list of AND `conditions`, which must be all True to select a document. Each condition
           is defined by a `field` present in the document and a `value`.
         - an optional `date_range`, used to apply the conditions on a time span. Date range is defined by
           a datetime `field`, a `start` and optional an `end` date.
         - a list of `set_extra_fields`, which will be set (i.e., added or modified) on the target index. An
           `add_extra_field` is defined by a `field` and its `value`. Field names are automatically prepended
           with the word `extra`. This is needed to separate the original fields with the extra ones.
         - a list of `remove_extra_fields`, which will be removed from the target index if
           exist. A `remove_extra_field` is defined by a `field`, which automatically prepended with the word `extra`.
           This is needed to avoid deleting original fields.
           Note that removals are executed after additions.

        An example of JSON is provided below:
        ```
        [
            {
                "conditions": [
                    {
                        "field": "author_name",
                        "value": "Mister X"
                    }
                ],
                "set_extra_fields": [
                    {
                        "field": "maintainer",
                        "value": "true"
                    }
                ]
            },
            {
                "conditions": [
                    {
                        "field": "author_name",
                        "value": "Mister X"
                    }
                 ],
                 "date_range": {
                    "field": "grimoire_creation_date",
                    "start": "2018-01-01",
                    "end": "2019-01-01"
                },
                "remove_extra_fields": [
                    {
                        "field": "maintainer",
                        "value": "true"
                    }
                ]
            }
        ]
        ```

        :param ocean_backend: backend from which to read the raw items
        :param enrich_backend:  backend from which to read the enriched items
        :param json_url: url to json file that containing the target documents and the extra fields to be added
        :param target_index: an optional target index to be enriched (e.g., an enriched or study index). If not
                             declared it will be the index defined in the enrich_backend.
        """
        index_url = "{}/{}".format(enrich_backend.elastic_url,
                                   target_index) if target_index else enrich_backend.elastic.index_url
        url = "{}/_update_by_query?wait_for_completion=true&conflicts=proceed".format(index_url)

        res = self.requests.get(index_url)
        if res.status_code != 200:
            logger.error("[enrich-extra-data] Target index {} doesn't exists, "
                         "study finished".format(anonymize_url(url)))
            return

        res = self.requests.get(json_url)
        res.raise_for_status()
        extras = res.json()

        for extra in extras:
            conds = []
            fltrs = []
            stmts = []

            # create AND conditions
            conditions = extra.get('conditions', [])
            for c in conditions:
                c_field = c['field']
                c_value = c['value']
                cond = {
                    "term": {
                        c_field: c_value
                    }
                }
                conds.append(cond)

            # create date filter
            date_range = extra.get('date_range', [])
            if date_range:
                gte = date_range.get("start", None)
                lte = date_range.get("end", None)
                # handle empty values
                lte = "now" if not lte else lte

                date_fltr = {
                    "range": {
                        date_range['field']: {
                            "gte": gte,
                            "lte": lte
                        }
                    }
                }

                fltrs.append(date_fltr)

            # populate painless, add/modify statements
            add_fields = extra.get('set_extra_fields', [])
            for a in add_fields:
                a_field = "{}_{}".format(EXTRA_PREFIX, a['field'])
                a_value = a['value']

                if isinstance(a_value, int) or isinstance(a_value, float):
                    if isinstance(a_value, bool):
                        stmt = "ctx._source.{} = {}".format(a_field, str(a_value).lower())
                    else:
                        stmt = "ctx._source.{} = {}".format(a_field, a_value)
                else:
                    stmt = "ctx._source.{} = '{}'".format(a_field, a_value)
                stmts.append(stmt)

            # populate painless, remove statements
            remove_fields = extra.get('remove_extra_fields', [])
            for r in remove_fields:
                r_field = "{}_{}".format(EXTRA_PREFIX, r['field'])

                stmt = "ctx._source.remove('{}')".format(r_field)

                stmts.append(stmt)

            es_query = '''
                    {
                      "script": {
                        "source":
                        "%s",
                        "lang": "painless"
                      },
                      "query": {
                        "bool": {
                          "must": %s,
                          "filter": %s
                        }
                      }
                    }
                    ''' % (";".join(stmts), json.dumps(conds), json.dumps(fltrs))

            try:
                r = self.requests.post(url, data=es_query, headers=HEADER_JSON, verify=False)
            except requests.exceptions.RetryError:
                logger.warning("[enrich-extra-data] Retry exceeded while executing study. "
                               "The following query is skipped {}.".format(es_query))
                continue

            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                logger.error("[enrich-extra-data] Error while executing study. Study aborted.")
                logger.error(ex)
                return

            logger.info("[enrich-extra-data] Target index {} updated with data from {}".format(
                        anonymize_url(url), json_url))

    def find_geo_point_in_index(self, es_in, in_index, location_field, location_value, geolocation_field):
        """Look for a geo point in the `in_index` based on the `location_field` and `location_value`

        :param es_in: Elastichsearch obj
        :param in_index: target index
        :param location_field: field including location info (e.g., user_location)
        :param location_value: value of the location field (e.g., Madrid, Spain)
        :param geolocation_field: field including geolocation info (e.g., user_geo_location)
        :return: geolocation coordinates
        """
        query_location_geo_point = """
        {
          "_source": {
            "includes": ["%s"]
            },
          "size": 1,
          "query": {
            "bool": {
              "must": [
                {
                  "exists": {
                    "field": "%s"
                  }
                }
              ],
              "filter": [
                {"term" : { "%s" : "%s" }}
              ]
            }
          }
        }
        """ % (geolocation_field, geolocation_field, location_field, location_value)

        location_geo_point = es_in.search(index=in_index, body=query_location_geo_point)['hits']['hits']
        geo_point_found = location_geo_point[0]['_source'][geolocation_field] if location_geo_point else None

        return geo_point_found

    def add_geo_point_in_index(self, enrich_backend, geolocation_field, loc_lat, loc_lon, location_field, location):
        """Add geo point information (`loc_lat` and `loc_lon`) to the `in_index` in `geolocation_field` based
        on the `location_field` and `location_value`.

        :param enrich_backend: Enrich backend obj
        :param geolocation_field: field including geolocation info (e.g., user_geo_location)
        :param loc_lat: latitude value
        :param loc_lon: longitude value
        :param location_field: field including location info (e.g., user_location)
        :param location: value of the location field (e.g., Madrid, Spain)
        """
        es_query = """
            {
              "script": {
                "source":
                "ctx._source.%s = [:];ctx._source.%s.lat = params.geo_lat;ctx._source.%s.lon = params.geo_lon;",
                "lang": "painless",
                "params": {
                    "geo_lat": "%s",
                    "geo_lon": "%s"
                }
              },
              "query": {
                "term": {
                  "%s": "%s"
                }
              }
            }
        """ % (geolocation_field, geolocation_field, geolocation_field, loc_lat, loc_lon,
               location_field, location)

        r = self.requests.post(
            enrich_backend.elastic.index_url + "/_update_by_query?wait_for_completion=true&conflicts=proceed",
            data=es_query.encode('utf-8'), headers=HEADER_JSON,
            verify=False
        )

        r.raise_for_status()

    def enrich_geolocation(self, ocean_backend, enrich_backend, location_field, geolocation_field):
        """
        This study includes geo points information (latitude and longitude) based on the value of
        the `location_field`. The coordinates are retrieved using Nominatim through the geopy package, and
        saved in the `geolocation_field`.

        All locations included in the `location_field` are retrieved from the enriched index. For each location,
        the geo points are retrieved. First the geo points already stored in the index are used to resolve the
        new geo points. If they are not present in the enriched index, Nominatim is employed to obtain the new geo
        points. The geo points are then saved to the `geolocation_field`. In case the geo points are not found for
        a given location, they are set to lat:0 lon:0, which point to the Null Island.

        The example below shows how to activate the study by modifying the setup.cfg. The study
        `enrich_geolocation:user` retrieves location data from `user_location` and stores the geo points
        to `user_geolocation`. In a similar manner, `enrich_geolocation:assignee` takes the data from
        `assignee_location` and stores the geo points to `assignee_geolocation`.

        ```
        [github]
        raw_index = github_issues_chaoss
        enriched_index = github_issues_chaoss_enriched
        api-token = ...
        ...
        studies = [enrich_geolocation:user, enrich_geolocation:assignee]

        [enrich_geolocation:user]
        location_field = user_location
        geolocation_field = user_geolocation

        [enrich_geolocation:assignee]
        location_field = assignee_location
        geolocation_field = assignee_geolocation
        ```

        :param ocean_backend: backend from which to read the raw items
        :param enrich_backend:  backend from which to read the enriched items
        :param location_field: field in the enriched index including location info (e.g., Madrid, Spain)
        :param geolocation_field: enriched field where latitude and longitude will be stored.

        :return: None
        """
        data_source = enrich_backend.__class__.__name__.split("Enrich")[0].lower()
        log_prefix = "[{}] Geolocation".format(data_source)
        logger.info("{} starting study {}".format(log_prefix, anonymize_url(self.elastic.index_url)))

        es_in = ES([enrich_backend.elastic_url], retry_on_timeout=True, timeout=100,
                   verify_certs=self.elastic.requests.verify, connection_class=RequestsHttpConnection)
        in_index = enrich_backend.elastic.index

        query_locations_no_geo_points = """
        {
          "size": 0,
          "aggs": {
            "locations": {
              "terms": {
                "field": "%s",
                "size": 5000,
                "order": {
                  "_count": "desc"
                }
              }
            }
          },
          "query": {
            "bool": {
              "must_not": [
                {
                  "exists": {
                    "field": "%s"
                  }
                }
              ]
            }
          }
        }
        """ % (location_field, geolocation_field)

        locations_no_geo_points = es_in.search(index=in_index, body=query_locations_no_geo_points)
        locations = [loc['key'] for loc in locations_no_geo_points['aggregations']['locations'].get('buckets', [])]
        geolocator = Nominatim(user_agent='grimoirelab-elk')

        for location in locations:
            # Default lat and lon coordinates point to the Null Island https://en.wikipedia.org/wiki/Null_Island
            loc_lat = 0
            loc_lon = 0

            # look for the geo point in the current index
            loc_info = self.find_geo_point_in_index(es_in, in_index, location_field, location, geolocation_field)
            if loc_info:
                loc_lat = loc_info['lat']
                loc_lon = loc_info['lon']
            else:
                try:
                    loc_info = geolocator.geocode(location)
                except Exception as ex:
                    logger.debug("{} Location {} not found for {}. {}".format(
                        log_prefix, location, anonymize_url(enrich_backend.elastic.index_url), ex)
                    )
                    continue

                # The geolocator may return a None value
                if loc_info:
                    loc_lat = loc_info.latitude
                    loc_lon = loc_info.longitude

            try:
                self.add_geo_point_in_index(enrich_backend, geolocation_field, loc_lat, loc_lon,
                                            location_field, location)
            except requests.exceptions.HTTPError as ex:
                logger.error("{} error executing study for {}. {}".format(
                    log_prefix, anonymize_url(enrich_backend.elastic.index_url), ex)
                )

        logger.info("{} end {}".format(log_prefix, anonymize_url(self.elastic.index_url)))

    def enrich_forecast_activity(self, ocean_backend, enrich_backend, out_index,
                                 observations=20, probabilities=[0.5, 0.7, 0.9], interval_months=6,
                                 date_field="metadata__updated_on"):
        """
        The goal of this study is to forecast the contributor activity based on their past contributions. The idea
        behind this study is that abandonment of active developers poses a significant risk for open source
        software projects, and this risk can be reduced by forecasting the future activity of contributors
        involved in such projects and taking necessary countermeasures. The logic of this study is based
        on the tool: https://github.com/AlexandreDecan/gap

        :param ocean_backend: backend from which to read the raw items
        :param enrich_backend:  backend from which to read the enriched items
        :param observations: number of observations to consider
        :param probabilities: probabilities of the next contributor's activity
        :param interval_months: number of months to consider a contributor active on the repo
        :param date_field: field used to find the author's activity

        Example of the setup.cfg

        [git]
        raw_index = git_gap_raw
        enriched_index = git_gap_enriched
        latest-items = true
        category = commit
        studies = [enrich_forecast_activity]

        [enrich_forecast_activity]
        out_index = git_study_forecast_activity
        observations = 40
        probability = [0.5, 0.7]
        date_field = author_date
        """
        logger.info("[enrich-forecast-activity] Start study")

        es_in = ES([enrich_backend.elastic_url], retry_on_timeout=True, timeout=100,
                   verify_certs=self.elastic.requests.verify, connection_class=RequestsHttpConnection)
        in_index = enrich_backend.elastic.index

        unique_repos = es_in.search(
            index=in_index,
            body=get_unique_repository())

        repositories = [repo['key'] for repo in unique_repos['aggregations']['unique_repos'].get('buckets', [])]
        current_month = datetime_utcnow().replace(day=1, hour=0, minute=0, second=0)

        logger.info("[enrich-forecast-activity] {} repositories to process".format(len(repositories)))
        es_out = ElasticSearch(enrich_backend.elastic.url, out_index)
        es_out.add_alias("forecast_activity_study")

        num_items = 0
        ins_items = 0

        survided_authors = []
        # iterate over the repositories
        for repository_url in repositories:
            logger.debug("[enrich-forecast-activity] Start analysis for {}".format(repository_url))
            from_month = get_to_date(es_in, in_index, out_index, repository_url, interval_months)
            to_month = from_month.replace(month=int(interval_months), day=1, hour=0, minute=0, second=0)

            # analyse the repository on a given time frame
            while to_month < current_month:

                from_month_iso = from_month.isoformat()
                to_month_iso = to_month.isoformat()

                # get authors
                authors = es_in.search(
                    index=in_index,
                    body=self.authors_between_dates(repository_url, from_month_iso, to_month_iso,
                                                    date_field=date_field)
                )['aggregations']['authors'].get("buckets", [])

                # get author activity
                for author in authors:
                    author_uuid = author['key']
                    activities = es_in.search(index=in_index,
                                              body=self.author_activity(repository_url, from_month_iso, to_month_iso,
                                                                        author_uuid, date_field=date_field)
                                              )['hits']['hits']

                    dates = [str_to_datetime(a['_source'][date_field]) for a in activities]
                    durations = self.dates_to_duration(dates, window_size=observations)

                    if len(durations) < observations:
                        continue

                    repository_name = repository_url.split("/")[-1]
                    author_name = activities[0]['_source'].get('author_name', None)
                    author_user_name = activities[0]['_source'].get('author_user_name', None)
                    author_org_name = activities[0]['_source'].get('author_org_name', None)
                    author_domain = activities[0]['_source'].get('author_domain', None)
                    author_bot = activities[0]['_source'].get('author_bot', None)
                    to_month_iso = to_month.isoformat()
                    survided_author = {
                        "uuid": "{}_{}_{}_{}".format(to_month_iso, repository_name, interval_months, author_uuid),
                        "origin": repository_url,
                        "repository": repository_name,
                        "interval_months": interval_months,
                        "from_date": from_month_iso,
                        "to_date": to_month_iso,
                        "study_creation_date": from_month_iso,
                        "author_uuid": author_uuid,
                        "author_name": author_name,
                        "author_bot": author_bot,
                        "author_user_name": author_user_name,
                        "author_org_name": author_org_name,
                        "author_domain": author_domain,
                        'metadata__gelk_version': self.gelk_version,
                        'metadata__gelk_backend_name': self.__class__.__name__,
                        'metadata__enriched_on': datetime_utcnow().isoformat()
                    }

                    survided_author.update(self.get_grimoire_fields(survided_author["study_creation_date"], "survived"))

                    last_activity = dates[-1]
                    surv = SurvfuncRight(durations, [1] * len(durations))
                    for prob in probabilities:
                        pred = surv.quantile(float(prob))
                        pred_field = "prediction_{}".format(str(prob).replace('.', ''))

                        survided_author[pred_field] = int(pred)
                        next_activity_field = "next_activity_{}".format(str(prob).replace('.', ''))
                        survided_author[next_activity_field] = (last_activity + timedelta(days=int(pred))).isoformat()

                    survided_authors.append(survided_author)

                    if len(survided_authors) >= self.elastic.max_items_bulk:
                        num_items += len(survided_authors)
                        ins_items += es_out.bulk_upload(survided_authors, self.get_field_unique_id())
                        survided_authors = []

                from_month = to_month
                to_month = to_month + relativedelta(months=+interval_months)

                logger.debug("[enrich-forecast-activity] End analysis for {}".format(repository_url))

        if len(survided_authors) > 0:
            num_items += len(survided_authors)
            ins_items += es_out.bulk_upload(survided_authors, self.get_field_unique_id())

        logger.info("[enrich-forecast-activity] End study")

    def dates_to_duration(self, dates, *, window_size=20):
        """
        Convert a list of dates into a list of durations
        (between consecutive dates). The resulting list is composed of
        'window_size' durations.
        """
        dates = sorted(set(dates))
        kept = dates[-window_size - 1:]  # -1 because intervals vs. bounds
        durations = []
        for first, second in zip(kept[:-1], kept[1:]):
            duration = (second - first).days
            durations.append(duration)

        return durations

    @staticmethod
    def authors_between_dates(repository_url, min_date, max_date,
                              author_field="author_uuid", date_field="metadata__updated_on"):
        """
        Get all authors between a min and max date

        :param repository_url: url of the repository
        :param min_date: min date to retrieve the authors' activities
        :param max_date: max date to retrieve the authors' activities
        :param author_field: field of the author
        :param date_field: field used to find the authors active in a given timeframe

        :return: the query to be executed to get the authors between two dates
        """
        es_query = """
            {
              "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "origin": "%s"
                            }
                        },
                        {
                            "range": {
                                "%s": {
                                    "gte": "%s",
                                    "lte": "%s"
                                }
                            }
                        }
                    ]
                }
              },
              "size": 0,
              "aggs": {
                "authors": {
                    "terms": {
                        "field": "%s",
                        "order": {
                            "_key": "asc"
                        }
                    }
                }
              }
            }
            """ % (repository_url, date_field, min_date, max_date, author_field)

        return es_query

    @staticmethod
    def author_activity(repository_url, min_date, max_date, author_value,
                        author_field="author_uuid", date_field="metadata__updated_on"):
        """
        Get the author's activity between two dates

        :param repository_url: url of the repository
        :param min_date: min date to retrieve the authors' activities
        :param max_date: max date to retrieve the authors' activities
        :param author_value: target author
        :param date_field: field used to find the the authors' activities
        :param author_field: field of the author

        :return: the query to be executed to get the authors' activities for a given repository
        """
        es_query = """
                {
                  "_source": ["%s", "author_name", "author_org_name", "author_bot",
                              "author_user_name", "author_domain"],
                  "size": 5000,
                  "query": {
                    "bool": {
                        "filter": [
                            {
                                "term": {
                                    "origin": "%s"
                                }
                            },
                            {
                                "range": {
                                    "%s": {
                                        "gte": "%s",
                                        "lte": "%s"
                                    }
                                }
                            },
                            {
                                "term": {
                                    "%s": "%s"
                                }
                            }
                        ]
                    }
                  },
                  "sort": [
                        {
                            "%s": {
                                "order": "asc"
                            }
                        }
                    ]
                }
                """ % (date_field, repository_url, date_field, min_date, max_date,
                       author_field, author_value, date_field)

        return es_query

    def enrich_demography_contribution(self, ocean_backend, enrich_backend, alias, date_field="grimoire_creation_date",
                                       author_field="author_uuid"):
        """
        Run demography study for the different types of the author activities and add the resulting enriched items.

        The resulting min and max dates are updated in all items including attributes following the pattern
        <contribution_type>_min_date and <contribution_type>_max_date.

        The different contribution types available are obtained with a query asking for unique "type" elements
        within the contributions from all authors.

        :param ocean_backend: backend from which to read the raw items
        :param enrich_backend:  backend from which to read the enriched items
        :poram alias: name of the study alias
        :param date_field: field used to find the mix and max dates for the author's activity
        :param author_field: field of the author

        :return: None
        """
        data_source = enrich_backend.__class__.__name__.split("Enrich")[0].lower()
        log_prefix = "[{}] Demography Contribution".format(data_source)
        logger.info("{} starting study {}".format(log_prefix, anonymize_url(self.elastic.index_url)))

        es_query = Enrich.fetch_contribution_types()
        r = self.requests.post(self.elastic.index_url + "/_search",
                               data=es_query, headers=HEADER_JSON,
                               verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            logger.error("{} error getting contribution types. Aborted.".format(log_prefix))
            logger.error(ex)
            return

        # Obtain the list of contribution types
        type_fields = []
        for type_field in r.json()['aggregations']['uniq_gender']['buckets']:
            type_fields.append(type_field['key'])

        # Run demography study for each contribution type
        type_fields.sort()
        for field in type_fields:
            Enrich.run_demography(self, date_field, author_field, log_prefix, contribution_type=field)

        if not self.elastic.alias_in_use(alias):
            logger.info("{} Creating alias: {}".format(log_prefix, alias))
            self.elastic.add_alias(alias)

        logger.info("{} end {}".format(log_prefix, anonymize_url(self.elastic.index_url)))

    def enrich_demography(self, ocean_backend, enrich_backend, alias, date_field="grimoire_creation_date",
                          author_field="author_uuid"):
        """
        Run demography study for all of the author activities and add the resulting enriched items.

        :param ocean_backend: backend from which to read the raw items
        :param enrich_backend:  backend from which to read the enriched items
        :poram alias: name of the study alias
        :param date_field: field used to find the mix and max dates for the author's activity
        :param author_field: field of the author

        :return: None
        """
        data_source = enrich_backend.__class__.__name__.split("Enrich")[0].lower()
        log_prefix = "[{}] Demography".format(data_source)
        logger.info("{} starting study {}".format(log_prefix, anonymize_url(self.elastic.index_url)))

        Enrich.run_demography(self, date_field, author_field, log_prefix)

        if not self.elastic.alias_in_use(alias):
            logger.info("{} Creating alias: {}".format(log_prefix, alias))
            self.elastic.add_alias(alias)

        logger.info("{} end {}".format(log_prefix, anonymize_url(self.elastic.index_url)))

    def run_demography(self, date_field, author_field, log_prefix, contribution_type=None):
        """
        The goal of the algorithm is to add to all enriched items the first and last date
        of all the activities or an specific contribution type of the author activities.

        In case there is no specific contribution type, by default all contributions will be considered.

        In order to implement the algorithm first, the min and max dates (based on the date_field attribute)
        are retrieved for all authors, including the contribution type in the corresponding query. Then,
        the resulting min and max dates are updated in all items including attributes following the pattern
        <contribution_type>_min_date and <contribution_type>_max_date. In case no contribution type is specified,
        the default fields are `demography_min_date` and `demography_max_date`.

        :param date_field: field used to find the mix and max dates for the author's activity
        :param author_field: field of the author
        :param log_prefix: log prefix used on logger
        :param contribution_type: name of the contribution type (if any) which the dates are computed for.
            In case there is no specific contribution type, by default all contributions will be considered.
        """
        # The first step is to find the current min and max date for all the authors
        authors_min_max_data = self.fetch_authors_min_max_dates(log_prefix, author_field, contribution_type, date_field)

        # Then we update the min max dates of all authors
        for author in authors_min_max_data:
            author_min_date = author['min']['value_as_string']
            author_max_date = author['max']['value_as_string']
            author_key = author['key']['author_uuid']
            field_name = contribution_type if contribution_type else 'demography'
            es_update = Enrich.update_author_min_max_date(author_min_date, author_max_date,
                                                          author_key, field_name, author_field=author_field)

            try:
                r = self.requests.post(
                    self.elastic.index_url + "/_update_by_query?wait_for_completion=true&conflicts=proceed",
                    data=es_update, headers=HEADER_JSON,
                    verify=False
                )
                self.check_version_conflicts(es_update, r.json().get('version_conflicts', None), log_prefix)

            except requests.exceptions.RetryError:
                logger.warning("{} retry exceeded while executing demography."
                               " The following query is skipped {}".format(log_prefix, es_update))
                continue

            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                logger.error("{} error updating mix and max date for author {}. Aborted.".format(
                             log_prefix, author_key))
                logger.error(ex)
                return

    def fetch_authors_min_max_dates(self, log_prefix, author_field, contribution_type, date_field):
        """ Fetch all authors with their first and last date of activity.

        :param log_prefix: log prefix used on logger.
        :param author_field: field of the author.
        :param contribution_type: name of the contribution type (if any) which the dates are computed for.
            In case there is no specific contribution type, by default all contributions will be considered.
        :param date_field: field used to find the mix and max dates for the author's activity.

        :return: dictionary of authors with min and max dates.
        """
        after = None

        while True:
            es_query = Enrich.authors_min_max_dates(date_field,
                                                    author_field=author_field,
                                                    contribution_type=contribution_type,
                                                    after=after)
            r = self.requests.post(self.elastic.index_url + "/_search",
                                   data=es_query, headers=HEADER_JSON,
                                   verify=False)
            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                logger.error("{} error getting authors mix and max date. Aborted.".format(log_prefix))
                logger.error(ex)
                return

            aggregations_author = r.json()['aggregations']['author']

            # When there are no more elements, it will return an empty list of buckets
            if not aggregations_author['buckets']:
                return

            after = aggregations_author['after_key'][author_field]

            for author in aggregations_author['buckets']:
                yield author

    def check_version_conflicts(self, es_update, version_conflicts, log_prefix, max_retries=5):
        """
        Check if there are version conflicts within a query response and retries the request.
        The time between requests is 0.5 second. This method will perform the retries until there are no
        more version conflicts.

        :param es_update: ES update query
        :param version_conflicts: number of version conflicts from a query response
        :param log_prefix: log prefix used on logger
        :param max_retries: max number of retries to perform the query again when version conflicts are found
        """
        if version_conflicts == 0 or max_retries == 0:
            return

        logger.debug("{}: Found version_conflicts: {}, retries left: {}, retry query: {}".format(log_prefix,
                                                                                                 version_conflicts,
                                                                                                 max_retries,
                                                                                                 es_update))
        time.sleep(0.5)  # Wait 0.5 second between requests
        r = self.requests.post(
            self.elastic.index_url + "/_update_by_query?wait_for_completion=true&conflicts=proceed",
            data=es_update, headers=HEADER_JSON,
            verify=False
        )
        r.raise_for_status()
        retries = max_retries - 1
        self.check_version_conflicts(es_update, r.json().get('version_conflicts', None), log_prefix, max_retries=retries)

    @staticmethod
    def authors_min_max_dates(date_field, author_field="author_uuid", contribution_type=None, after=None):
        """
        Get the aggregation of author with their min and max activity dates

        :param date_field: field used to find the mix and max dates for the author's activity
        :param author_field: field of the author
        :param contribution_type: name of the contribution type (if any) which the dates are computed for.
            In case there is no specific contribution type, by default all contributions will be considered.
        :param after: value used for pagination

        :return: the query to be executed to get the authors min and max aggregation data
        """

        # Limit aggregations:
        # - OpenSearch: 10000
        #   - https://opensearch.org/docs/latest/opensearch/bucket-agg/
        # - ElasticSearch: 10000
        #   - https://discuss.elastic.co/t/increasing-max-buckets-for-specific-visualizations/187390/4
        #   - When you try to fetch more than 10000 it will return this error message:
        #     {
        #       "type": "too_many_buckets_exception",
        #       "reason": "Trying to create too many buckets. Must be less than or equal to: [10000] but was [20000].
        #                 This limit can be set by changing the [search.max_buckets] cluster level setting.",
        #       "max_buckets": 10000
        #     }

        query_type = ""
        if contribution_type:
            query_type = """"query": {
            "bool" : {
              "must" : {
                "term" : {
                  "type" : "%s"
                }
              }
            }
          },""" % contribution_type

        query_after = ""
        if after:
            query_after = """"after": {
                  "%s": "%s"
                },""" % (author_field, after)

        es_query = """
        {
          "size": 0,
          %s
          "aggs": {
            "author": {
              "composite": {
                "sources": [
                  {
                    "%s": {
                      "terms": {
                        "field": "%s"
                      }
                    }
                  }
                ],
                %s
                "size": 10000
              },
              "aggs": {
                "min": {
                  "min": {
                    "field": "%s"
                  }
                },
                "max": {
                  "max": {
                    "field": "%s"
                  }
                }
              }
            }
          }
        }
        """ % (query_type, author_field, author_field, query_after, date_field, date_field)

        return es_query

    @staticmethod
    def fetch_contribution_types():
        query = '''
        {
            "size":"0",
            "aggs" : {
                "uniq_gender" : {
                    "terms" : { "field" : "type" }
                }
            }
        }
        '''
        return query

    @staticmethod
    def update_author_min_max_date(min_date, max_date, target_author, field, author_field="author_uuid"):
        """
        Get the query to update demography_min_date and demography_max_date of a given author

        :param min_date: new <field>_min_date
        :param max_date: new <field>_max_date
        :param target_author: target author to be updated
        :param field: enriched field name
        :param author_field: author field

        :return: the query to be executed to update demography data of an author
        """

        es_query = '''
        {
          "script": {
            "source":
            "ctx._source.%s_min_date = params.min_date;ctx._source.%s_max_date = params.max_date;",
            "lang": "painless",
            "params": {
                "min_date": "%s",
                "max_date": "%s"
            }
          },
          "query": {
            "term": {
              "%s": "%s"
            }
          }
        }
        ''' % (field, field, min_date, max_date, author_field, target_author)

        return es_query

    def enrich_feelings(self, ocean_backend, enrich_backend, attributes, nlp_rest_url,
                        no_incremental=False, uuid_field='id', date_field="grimoire_creation_date"):
        """
        This study allows to add sentiment and emotion data to a target enriched index. All documents in the enriched
        index not containing the attributes `has_sentiment` or `has_emotion` are retrieved. Then, each attribute
        listed in the param `attributes` is searched in the document. If the attribute is found, its text is
        retrieved and sent to the NLP tool available at `nlp_rest_url`, which returns sentiment and emotion
        information. Such a data is stored in the attributes `feeling_sentiment` and `feeling_emotion` using
        the `update_by_query` endpoint.

        :param ocean_backend: backend from which to read the raw items
        :param enrich_backend:  backend from which to read the enriched items
        :param attributes: list of attributes in the JSON documents from where the
            sentiment/emotion data must be extracted.
        :param nlp_rest_url: URL of the NLP tool
        :param no_incremental: if `True` the incremental enrichment is ignored.
        :param uuid_field: field storing the UUID of the documents
        :param date_field: field used to order the documents
        """
        es_query = """
            {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "bool": {
                                    "must_not": {
                                        "exists": {
                                            "field": "has_sentiment"
                                        }
                                    }
                                }
                            },
                            {
                                "bool": {
                                    "must_not": {
                                        "exists": {
                                            "field": "has_emotion"
                                        }
                                    }
                                }
                            }
                        ]
                    }
                },
                "sort": [
                    {
                        "%s": {
                           "order": "asc"
                        }
                    }
                ]
            }
            """ % date_field

        logger.info("[enrich-feelings] Start study on {} with data from {}".format(
            anonymize_url(self.elastic.index_url), nlp_rest_url))

        es = ES([self.elastic_url], timeout=3600, max_retries=50, retry_on_timeout=True,
                verify_certs=self.elastic.requests.verify, connection_class=RequestsHttpConnection)
        search_fields = [attr for attr in attributes]
        search_fields.extend([uuid_field])
        page = es.search(index=enrich_backend.elastic.index,
                         scroll="1m",
                         _source=search_fields,
                         size=100,
                         body=json.loads(es_query))

        scroll_id = page["_scroll_id"]
        total = page['hits']['total']
        if isinstance(total, dict):
            scroll_size = total['value']
        else:
            scroll_size = total

        if scroll_size == 0:
            logging.warning("No data found!")
            return

        total = 0
        sentiments_data = {}
        emotions_data = {}
        while scroll_size > 0:

            for hit in page['hits']['hits']:
                source = hit['_source']
                source_uuid = str(source[uuid_field])
                total += 1

                for attr in attributes:
                    found = source.get(attr, None)
                    if not found:
                        continue
                    else:
                        found = found.encode('utf-8')

                    sentiment_label, emotion_label = self.get_feelings(found, nlp_rest_url)
                    self.__update_feelings_data(sentiments_data, sentiment_label, source_uuid)
                    self.__update_feelings_data(emotions_data, emotion_label, source_uuid)

            if sentiments_data:
                self.__add_feelings_to_index('sentiment', sentiments_data, uuid_field)
                sentiments_data = {}
            if emotions_data:
                self.__add_feelings_to_index('emotion', emotions_data, uuid_field)
                emotions_data = {}

            page = es.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_id = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])

        if sentiments_data:
            self.__add_feelings_to_index('sentiment', sentiments_data, uuid_field)
        if emotions_data:
            self.__add_feelings_to_index('emotion', emotions_data, uuid_field)

        logger.info("[enrich-feelings] End study. Index {} updated with data from {}".format(
            anonymize_url(self.elastic.index_url), nlp_rest_url))

    def get_feelings(self, text, nlp_rest_url):
        """This method wraps the calls to the NLP rest service. First the text is converted as plain text,
        then the code is stripped and finally the resulting text is process to extract sentiment and emotion
        information.

        :param text: text to analyze
        :param nlp_rest_url: URL of the NLP rest tool
        :return: a tuple composed of the sentiment and emotion labels
        """
        sentiment = None
        emotion = None

        headers = {
            'Content-Type': 'text/plain'
        }
        plain_text_url = nlp_rest_url + '/plainTextBugTrackerMarkdown'
        r = self.requests.post(plain_text_url, data=text, headers=headers)
        r.raise_for_status()
        plain_text_json = r.json()

        headers = {
            'Content-Type': 'application/json'
        }
        code_url = nlp_rest_url + '/code'
        r = self.requests.post(code_url, data=json.dumps(plain_text_json), headers=headers)
        r.raise_for_status()
        code_json = r.json()

        texts = [c['text'] for c in code_json if c['label'] != '__label__Code']
        message = '.'.join(texts)
        message_dump = json.dumps([message])

        if not message:
            logger.debug("[enrich-feelings] No feelings detected after processing on {} in index {}".format(
                text, anonymize_url(self.elastic.index_url)))
            return sentiment, emotion

        sentiment_url = nlp_rest_url + '/sentiment'
        headers = {
            'Content-Type': 'application/json'
        }
        r = self.requests.post(sentiment_url, data=message_dump, headers=headers)
        r.raise_for_status()
        sentiment_json = r.json()[0]
        sentiment = sentiment_json['label']

        emotion_url = nlp_rest_url + '/emotion'
        headers = {
            'Content-Type': 'application/json'
        }
        r = self.requests.post(emotion_url, data=message_dump, headers=headers)
        r.raise_for_status()
        emotion_json = r.json()[0]

        emotion = emotion_json['labels'][0] if len(emotion_json.get('labels', [])) > 0 else None
        return sentiment, emotion

    def __update_feelings_data(self, data, label, source_uuid):
        if not label:
            entry = data.get('__label__unknown', None)
            if not entry:
                data.update({'__label__unknown': [source_uuid]})
            else:
                entry.append(source_uuid)
        else:
            entry = data.get(label, None)
            if not entry:
                data.update({label: [source_uuid]})
            else:
                entry.append(source_uuid)

    def __add_feelings_to_index(self, feeling_type, feeling_data, uuid_field):
        url = "{}/_update_by_query?wait_for_completion=true".format(self.elastic.index_url)
        for fd in feeling_data:
            uuids = json.dumps(feeling_data[fd])
            es_query = """
                {
                  "script": {
                    "source": "ctx._source.feeling_%s = '%s';ctx._source.has_%s = 1",
                    "lang": "painless"
                  },
                  "query": {
                    "bool": {
                      "filter": {
                        "terms": {
                            "%s": %s
                        }
                      }
                    }
                  }
                }
                """ % (feeling_type, fd, feeling_type, uuid_field, uuids)

            r = self.requests.post(url, data=es_query, headers=HEADER_JSON, verify=False)
            try:
                r.raise_for_status()
                logger.debug("[enrich-feelings] Adding {} on uuids {} in {}".format(
                    fd, uuids, anonymize_url(self.elastic.index_url)))
            except requests.exceptions.HTTPError as ex:
                logger.error("[enrich-feelings] Error while executing study. Study aborted.")
                logger.error(ex)
                return
