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

import json
import functools
import logging
import requests
import sys

from datetime import timedelta

import pkg_resources
from functools import lru_cache

from elasticsearch import Elasticsearch, RequestsHttpConnection

from perceval.backend import find_signature_parameters
from grimoirelab_toolkit.datetime import (datetime_utcnow, str_to_datetime)

from ..elastic_items import (ElasticItems,
                             HEADER_JSON)
from .study_ceres_onion import ESOnionConnector, onion_study

from .utils import grimoire_con, METADATA_FILTER_RAW, REPO_LABELS
from .. import __version__

logger = logging.getLogger(__name__)

try:
    import pymysql
    MYSQL_LIBS = True
except ImportError:
    logger.info("MySQL not available")
    MYSQL_LIBS = False

try:
    from sortinghat.db.database import Database
    from sortinghat import api, utils
    from sortinghat.exceptions import NotFoundError, InvalidValueError

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
DEMOGRAPHICS_ALIAS = 'demographics'
ONION_ALIAS = 'all_onion'


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

    sh_db = None
    kibiter_version = None
    RAW_FIELDS_COPY = ["metadata__updated_on", "metadata__timestamp",
                       "offset", "origin", "tag", "uuid"]
    KEYWORD_MAX_LENGTH = 1000  # this control allows to avoid max_bytes_length_exceeded_exception

    ONION_INTERVAL = seconds = 3600 * 24 * 7

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', insecure=True):

        perceval_backend = None
        super().__init__(perceval_backend, insecure=insecure)

        self.sortinghat = False
        if db_user == '':
            db_user = DEFAULT_DB_USER
        if db_sortinghat and not SORTINGHAT_LIBS:
            raise RuntimeError("Sorting hat configured but libraries not available.")
        if db_sortinghat:
            # self.sh_db = Database("root", "", db_sortinghat, "mariadb")
            if not Enrich.sh_db:
                Enrich.sh_db = Database(db_user, db_password, db_sortinghat, db_host)
            self.sortinghat = True

        self.prjs_map = None  # mapping beetween repositories and projects
        self.json_projects = None

        if json_projects_map:
            with open(json_projects_map) as data_file:
                self.json_projects = json.load(data_file)
                # If we have JSON projects always use them for mapping
                self.prjs_map = self.__convert_json_to_projects_map(self.json_projects)
        if not self.json_projects:
            if db_projects_map and not MYSQL_LIBS:
                raise RuntimeError("Projects configured but MySQL libraries not available.")
            if db_projects_map and not self.json_projects:
                self.prjs_map = self.__get_projects_map(db_projects_map,
                                                        db_user, db_password,
                                                        db_host)

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
        # in twitter and others managed in arthur logic
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
            raise RuntimeError("Unknown backend %s" % backend_name)
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
                    repo, _ = self.extract_repo_labels(repo)
                    if repo in ds_repo_to_prj[ds]:
                        if project == ds_repo_to_prj[ds][repo]:
                            logger.debug("Duplicated repo: %s %s %s", ds, repo, project)
                        else:
                            if len(project.split(".")) > len(ds_repo_to_prj[ds][repo].split(".")):
                                logger.debug("Changed repo project because we found a leaf: %s leaf vs %s (%s, %s)",
                                             project, ds_repo_to_prj[ds][repo], repo, ds)
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
                    logger.error("Project not found in JSON ", project)
                    raise NotFoundError("Project not found in JSON " + project)
                else:
                    if ds == 'mls':
                        repo_mls = repository.split("/")[-1]
                        repo_mls = repo_mls.replace(".mbox", "")
                        repository = 'https://dev.eclipse.org/mailman/listinfo/' + repo_mls
                    if ds_map_db[ds] not in json[project]:
                        logger.error("db repository not found in json %s", repository)
                    elif repository not in json[project][ds_map_db[ds]]:
                        logger.error("db repository not found in json %s", repository)

        for project in json.keys():
            if project not in db_projects:
                logger.debug("JSON project %s not found in db" % project)

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
                        logger.debug("Not found repository in db %s %s", repo, ds)

        logger.debug("Number of db projects: %i", len(db_projects))
        logger.debug("Number of json projects: %i (>=%i)", len(json.keys()), len(db_projects))

    def __get_projects_map(self, db_projects_map, db_user=None, db_password=None, db_host=None):
        # Read the repo to project mapping from a database
        ds_repo_to_prj = {}

        db = pymysql.connect(user=db_user, passwd=db_password, host=db_host,
                             db=db_projects_map)
        cursor = db.cursor()

        query = """
            SELECT data_source, p.id, pr.repository_name
            FROM projects p
            JOIN project_repositories pr ON p.project_id=pr.project_id
        """

        res = int(cursor.execute(query))
        if res > 0:
            rows = cursor.fetchall()
            for row in rows:
                [ds, name, repo] = row
                if ds not in ds_repo_to_prj:
                    ds_repo_to_prj[ds] = {}
                ds_repo_to_prj[ds][repo] = name
        else:
            raise RuntimeError("Can't find projects mapping in %s" % (db_projects_map))
        return ds_repo_to_prj

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

        url = self.elastic.index_url + '/items/_bulk'

        logger.debug("Adding items to %s (in %i packs)", self.elastic.anonymize_url(url), max_items)

        if events:
            logger.debug("Adding events items")

        for item in items:
            if current >= max_items:
                try:
                    total += self.elastic.safe_put_bulk(url, bulk_json)
                    json_size = sys.getsizeof(bulk_json) / (1024 * 1024)
                    logger.debug("Added %i items to %s (%0.2f MB)", total, self.elastic.anonymize_url(url), json_size)
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
        return get_connector_name(type(self))

    def get_field_author(self):
        """ Field with the author information """
        raise NotImplementedError

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "metadata__updated_on"

    def get_fields_uuid(self):
        """ Fields with unique identities in the JSON enriched items """
        raise NotImplementedError

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
        except IndexError:
            # logger.warning("Bad email format: %s" % (identity['email']))
            pass
        return domain

    def get_identity_domain(self, identity):
        domain = None
        if identity['email']:
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

#    def get_elastic_mappings(self):
#        """ Mappings for enriched indexes """
#
#        mapping = '{}'
#        return {"items": mapping}

    def get_elastic_analyzers(self):
        """ Custom analyzers for our indexes  """

        analyzers = '''
        {
                "analysis" : {
                    "tokenizer" : {
                        "comma" : {
                            "type" : "pattern",
                            "pattern" : ","
                        }
                    },
                    "analyzer" : {
                        "comma" : {
                            "type" : "custom",
                            "tokenizer" : "comma"
                        }
                    }
                }
        }
        '''

        return analyzers

    def get_grimoire_fields(self, creation_date, item_name):
        """ Return common grimoire fields for all data sources """

        grimoire_date = None
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
            # if `projects_json_repo`, which shouldn't never happen, use the
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

    def get_domain(self, identity):
        """ Get the domain from a SH identity """
        domain = None
        if identity['email']:
            try:
                domain = identity['email'].split("@")[1]
            except IndexError:
                # logger.warning("Bad email format: %s" % (identity['email']))
                pass
        return domain

    def is_bot(self, uuid):
        bot = False
        u = self.get_unique_identity(uuid)
        if u.profile:
            bot = u.profile.is_bot
        return bot

    def get_enrollment(self, uuid, item_date):
        """ Get the enrollment for the uuid when the item was done """
        # item_date must be offset-naive (utc)
        if item_date and item_date.tzinfo:
            item_date = (item_date - item_date.utcoffset()).replace(tzinfo=None)

        enrollments = self.get_enrollments(uuid)
        enroll = self.unaffiliated_group
        if enrollments:
            for enrollment in enrollments:
                if not item_date:
                    enroll = enrollment.organization.name
                    break
                elif item_date >= enrollment.start and item_date <= enrollment.end:
                    enroll = enrollment.organization.name
                    break
        return enroll

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
            rol + "_bot": False
        }

    def get_item_sh_fields(self, identity=None, item_date=None, sh_id=None,
                           rol='author'):
        """ Get standard SH fields from a SH identity """
        eitem_sh = self.__get_item_sh_fields_empty(rol)

        if identity:
            # Use the identity to get the SortingHat identity
            sh_ids = self.get_sh_ids(identity, self.get_connector_name())
            eitem_sh[rol + "_id"] = sh_ids.get('id', '')
            eitem_sh[rol + "_uuid"] = sh_ids.get('uuid', '')
            eitem_sh[rol + "_name"] = identity.get('name', '')
            eitem_sh[rol + "_user_name"] = identity.get('username', '')
            eitem_sh[rol + "_domain"] = self.get_identity_domain(identity)
        elif sh_id:
            # Use the SortingHat id to get the identity
            eitem_sh[rol + "_id"] = sh_id
            eitem_sh[rol + "_uuid"] = self.get_uuid_from_id(sh_id)
        else:
            # No data to get a SH identity. Return an empty one.
            return eitem_sh

        # If the identity does not exists return and empty identity
        if rol + "_uuid" not in eitem_sh or not eitem_sh[rol + "_uuid"]:
            return self.__get_item_sh_fields_empty(rol, undefined=True)

        # Get the SH profile to use first this data
        profile = self.get_profile_sh(eitem_sh[rol + "_uuid"])

        if profile:
            # If name not in profile, keep its old value (should be empty or identity's name field value)
            eitem_sh[rol + "_name"] = profile.get('name', eitem_sh[rol + "_name"])

            email = profile.get('email', None)
            if email:
                eitem_sh[rol + "_domain"] = self.get_email_domain(email)

            eitem_sh[rol + "_gender"] = profile.get('gender', self.unknown_gender)
            eitem_sh[rol + "_gender_acc"] = profile.get('gender_acc', 0)

        elif not profile and sh_id:
            logger.warning("Can't find SH identity profile: %s", sh_id)

        # Ensure we always write gender fields
        if not eitem_sh.get(rol + "_gender"):
            eitem_sh[rol + "_gender"] = self.unknown_gender
            eitem_sh[rol + "_gender_acc"] = 0

        eitem_sh[rol + "_org_name"] = self.get_enrollment(eitem_sh[rol + "_uuid"], item_date)
        eitem_sh[rol + "_bot"] = self.is_bot(eitem_sh[rol + '_uuid'])
        return eitem_sh

    def get_profile_sh(self, uuid):
        profile = {}

        u = self.get_unique_identity(uuid)
        if u.profile:
            profile['name'] = u.profile.name
            profile['email'] = u.profile.email
            profile['gender'] = u.profile.gender
            profile['gender_acc'] = u.profile.gender_acc

        return profile

    def get_item_sh_from_id(self, eitem, roles=None):
        # Get the SH fields from the data in the enriched item

        eitem_sh = {}  # Item enriched

        author_field = self.get_field_author()
        if not author_field:
            return eitem_sh
        sh_id_author = None

        if not roles:
            roles = [author_field]

        date = str_to_datetime(eitem[self.get_field_date()])

        for rol in roles:
            if rol + "_id" not in eitem:
                # For example assignee in github it is usual that it does not appears
                logger.debug("Enriched index does not include SH ids for %s. Can not refresh it.", rol + "_id")
                continue
            sh_id = eitem[rol + "_id"]
            if not sh_id:
                logger.debug("%s_id is None", rol)
                continue
            if rol == author_field:
                sh_id_author = sh_id
            eitem_sh.update(self.get_item_sh_fields(sh_id=sh_id, item_date=date,
                                                    rol=rol))

        # Add the author field common in all data sources
        rol_author = 'author'
        if sh_id_author and author_field != rol_author:
            eitem_sh.update(self.get_item_sh_fields(sh_id=sh_id_author,
                                                    item_date=date, rol=rol_author))
        return eitem_sh

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
            item_date = str_to_datetime(item[self.get_field_date()])
        else:
            item_date = str_to_datetime(item[date_field])

        users_data = self.get_users_data(item)

        for rol in roles:
            if rol in users_data:
                identity = self.get_sh_identity(item, rol)
                eitem_sh.update(self.get_item_sh_fields(identity, item_date, rol=rol))

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
            eitem_sh.update(self.get_item_sh_fields(identity, item_date, rol=rol_author))

            if not eitem_sh['author_org_name']:
                eitem_sh['author_org_name'] = SH_UNKNOWN_VALUE

            if not eitem_sh['author_name']:
                eitem_sh['author_name'] = SH_UNKNOWN_VALUE

            if not eitem_sh['author_user_name']:
                eitem_sh['author_user_name'] = SH_UNKNOWN_VALUE

        return eitem_sh

    @lru_cache()
    def get_enrollments(self, uuid):
        return api.enrollments(self.sh_db, uuid)

    @lru_cache()
    def get_unique_identity(self, uuid):
        return api.unique_identities(self.sh_db, uuid)[0]

    @lru_cache()
    def get_uuid_from_id(self, sh_id):
        """ Get the SH identity uuid from the id """
        return SortingHat.get_uuid_from_id(self.sh_db, sh_id)

    def get_sh_ids(self, identity, backend_name):
        """ Return the Sorting Hat id and uuid for an identity """
        # Convert the dict to tuple so it is hashable
        identity_tuple = tuple(identity.items())
        sh_ids = self.__get_sh_ids_cache(identity_tuple, backend_name)
        return sh_ids

    @lru_cache()
    def __get_sh_ids_cache(self, identity_tuple, backend_name):

        # Convert tuple to the original dict
        identity = dict((x, y) for x, y in identity_tuple)

        if not self.sortinghat:
            raise RuntimeError("Sorting Hat not active during enrich")

        iden = {}
        sh_ids = {"id": None, "uuid": None}

        for field in ['email', 'name', 'username']:
            iden[field] = None
            if field in identity:
                iden[field] = identity[field]

        if not iden['name'] and not iden['email'] and not iden['username']:
            logger.warning("Name, email and username are none in %s", backend_name)
            return sh_ids

        try:
            # Find the uuid for a given id.
            id = utils.uuid(backend_name, email=iden['email'],
                            name=iden['name'], username=iden['username'])

            if not id:
                logger.warning("Id not found in SortingHat for name: %s, email: %s and username: %s in %s",
                               str(iden['name']), str(iden['email']), str(iden['username']), backend_name)
                return sh_ids

            with self.sh_db.connect() as session:
                identity_found = api.find_identity(session, id)

                if not identity_found:
                    return sh_ids

                sh_ids['id'] = identity_found.id
                sh_ids['uuid'] = identity_found.uuid
        except InvalidValueError:
            msg = "None Identity found {}".format(backend_name)
            logger.debug(msg)
        except NotFoundError:
            msg = "Identity not found in SortingHat {}".format(backend_name)
            logger.debug(msg)
        except UnicodeEncodeError:
            msg = "UnicodeEncodeError {}, identity: {}".format(backend_name, identity)
            logger.error(msg)
        except Exception as ex:
            msg = "Unknown error adding identity in SortingHat, {}, {}, {}".format(ex, backend_name, identity)
            logger.error(msg)

        return sh_ids

    def enrich_onion(self, enrich_backend, in_index, out_index, data_source,
                     contribs_field, timeframe_field, sort_on_field,
                     seconds=ONION_INTERVAL, no_incremental=False):

        log_prefix = "[" + data_source + "] study onion"

        logger.info("%s  starting study - Input: %s Output: %s", log_prefix, in_index, out_index)

        # Creating connections
        es = Elasticsearch([enrich_backend.elastic.url], retry_on_timeout=True, timeout=100,
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
            logger.info("%s missing index %s", log_prefix, in_index)
            return

        # Check last execution date
        latest_date = None
        if out_conn.exists():
            latest_date = out_conn.latest_enrichment_date()

        if latest_date:
            logger.info(log_prefix + " Latest enrichment date: " + latest_date.isoformat())
            update_after = latest_date + timedelta(seconds=seconds)
            logger.info("%s update after date: %s", log_prefix, update_after.isoformat())
            if update_after >= datetime_utcnow():
                logger.info("%s too soon to update. Next update will be at %s",
                            log_prefix, update_after.isoformat())
                return

        # Onion currently does not support incremental option
        logger.info("%s Creating out ES index", log_prefix)
        # Initialize out index
        filename = pkg_resources.resource_filename('grimoire_elk', 'enriched/mappings/onion.json')
        out_conn.create_index(filename, delete=out_conn.exists())

        onion_study(in_conn=in_conn, out_conn=out_conn, data_source=data_source)

        # Create alias if output index exists (index is always created from scratch, so
        # alias need to be created each time)
        if out_conn.exists() and not out_conn.exists_alias(out_index, ONION_ALIAS):
            logger.info("%s Creating alias: %s", log_prefix, ONION_ALIAS)
            out_conn.create_alias(ONION_ALIAS)

        logger.info("%s end", log_prefix)

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
            logger.error("[enrich-extra-data] Target index %s doesn't exists, "
                         "study finished", self.elastic.anonymize_url(url))
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
                               "The following query is skipped %s.", es_query)
                continue

            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                logger.error("[enrich-extra-data] Error while executing study. Study aborted.")
                logger.error(ex)
                return

            logger.info("[enrich-extra-data] Target index %s "
                        "updated with data from %s", self.elastic.anonymize_url(url), json_url)

    def enrich_demography(self, ocean_backend, enrich_backend, date_field="grimoire_creation_date",
                          author_field="author_uuid"):
        """
        The goal of the algorithm is to add to all enriched items the first and last date
        (i.e., demography_min_date, demography_max_date) of the author activities.

        In order to implement the algorithm first, the min and max dates (based on the date_field attribute)
        are retrieved for all authors. Then, demography_min_date and demography_max_date attributes are
        updated in all items.

        :param ocean_backend: backend from which to read the raw items
        :param enrich_backend:  backend from which to read the enriched items
        :param date_field: field used to find the mix and max dates for the author's activity
        :param author_field: field of the author

        :return: None
        """
        data_source = enrich_backend.__class__.__name__.split("Enrich")[0].lower()
        log_prefix = "[" + data_source + "] Demography"
        logger.info("%s starting study %s", log_prefix, self.elastic.anonymize_url(self.elastic.index_url))

        # The first step is to find the current min and max date for all the authors
        authors_min_max_data = {}

        es_query = Enrich.authors_min_max_dates(date_field, author_field=author_field)
        r = self.requests.post(self.elastic.index_url + "/_search",
                               data=es_query, headers=HEADER_JSON,
                               verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            logger.error("%s error getting authors mix and max date. Aborted.", log_prefix)
            logger.error(ex)
            return

        for author in r.json()['aggregations']['author']['buckets']:
            authors_min_max_data[author['key']] = author

        # Then we update the min max dates of all authors
        for author_key in authors_min_max_data:
            author_min_date = authors_min_max_data[author_key]['min']['value_as_string']
            author_max_date = authors_min_max_data[author_key]['max']['value_as_string']

            es_update = Enrich.update_author_min_max_date(author_min_date, author_max_date,
                                                          author_key, author_field=author_field)

            try:
                r = self.requests.post(
                    self.elastic.index_url + "/_update_by_query?wait_for_completion=true&conflicts=proceed",
                    data=es_update, headers=HEADER_JSON,
                    verify=False
                )
            except requests.exceptions.RetryError:
                logger.warning("%s retry execeeded while executing demography."
                               " The following query is skipped %s", log_prefix, es_update)
                continue

            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                logger.error("%s error updating mix and max date for author %s. Aborted.",
                             log_prefix, author_key)
                logger.error(ex)
                return

        if not self.elastic.alias_in_use(DEMOGRAPHICS_ALIAS):
            logger.info("%s Creating alias: %s", log_prefix, DEMOGRAPHICS_ALIAS)
            self.elastic.add_alias(DEMOGRAPHICS_ALIAS)

        logger.info("%s end %s", log_prefix, self.elastic.anonymize_url(self.elastic.index_url))

    @staticmethod
    def authors_min_max_dates(date_field, author_field="author_uuid"):
        """
        Get the aggregation of author with their min and max activity dates

        :param date_field: field used to find the mix and max dates for the author's activity
        :param author_field: field of the author

        :return: the query to be executed to get the authors min and max aggregation data
        """

        # Limit aggregations: https://github.com/elastic/elasticsearch/issues/18838
        # 30000 seems to be a sensible number of the number of people in git

        es_query = """
        {
          "size": 0,
          "aggs": {
            "author": {
              "terms": {
                "field": "%s",
                "size": 30000
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
        """ % (author_field, date_field, date_field)

        return es_query

    @staticmethod
    def update_author_min_max_date(min_date, max_date, target_author, author_field="author_uuid"):
        """
        Get the query to update demography_min_date and demography_max_date of a given author

        :param min_date: new demography_min_date
        :param max_date: new demography_max_date
        :param target_author: target author to be updated
        :param author_field: author field

        :return: the query to be executed to update demography data of an author
        """

        es_query = '''
        {
          "script": {
            "source":
            "ctx._source.demography_min_date = params.min_date;ctx._source.demography_max_date = params.max_date;",
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
        ''' % (min_date, max_date, author_field, target_author)

        return es_query
