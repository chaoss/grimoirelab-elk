#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Elastic Enrichment API
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

import json
import functools
import logging
import subprocess

import requests

from datetime import datetime as dt
from os import path

from dateutil import parser
from functools import lru_cache

logger = logging.getLogger(__name__)

try:
    import MySQLdb
    MYSQL_LIBS = True
except ImportError:
    logger.info("MySQL not available")
    MYSQL_LIBS = False

try:
    from sortinghat.db.database import Database
    from sortinghat import api, utils
    from sortinghat.exceptions import AlreadyExistsError, NotFoundError, WrappedValueError

    from .sortinghat import SortingHat

    SORTINGHAT_LIBS = True
except ImportError:
    logger.info("SortingHat not available")
    SORTINGHAT_LIBS = False

DEFAULT_PROJECT = 'Main'
DEFAULT_DB_USER = 'root'

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
            'metadata__gelk_backend_name' : self.__class__.__name__,
            'metadata__enriched_on' : dt.utcnow().isoformat()
        }
        eitem.update(metadata)
        return eitem
    return decorator


class Enrich(object):

    sh_db = None

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', insecure=True):
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
        json_projects = None

        if json_projects_map:
            with open(json_projects_map) as data_file:
                json_projects = json.load(data_file)
                # If we have JSON projects always use them for mapping
                self.prjs_map = self.__convert_json_to_projects_map(json_projects)
        if not json_projects:
            if db_projects_map and not MYSQL_LIBS:
                raise RuntimeError("Projects configured but MySQL libraries not available.")
            if  db_projects_map and not json_projects:
                self.prjs_map = self.__get_projects_map(db_projects_map,
                                                        db_user, db_password,
                                                        db_host)

        if self.prjs_map and json_projects:
            # logging.info("Comparing db and json projects")
            # self.__compare_projects_map(self.prjs_map, self.json_projects)
            pass

        self.studies = []

        self.requests = requests.Session()
        if insecure:
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
            self.requests.verify = False

        self.elastic = None
        self.type_name = "items"  # type inside the index to store items enriched

        # To add the gelk version to enriched items
        try:
            # gelk is executed directly from a git clone
            git_path = path.dirname(__file__)
            self.gelk_version = subprocess.check_output(["git", "-C", git_path, "describe"]).strip()
            self.gelk_version = self.gelk_version.decode("utf-8")
        except subprocess.CalledProcessError:
            logging.warning("Can't get the gelk version. %s", __file__)
            self.gelk_version = 'Unknown'

    def set_elastic(self, elastic):
        self.elastic = elastic

    def __convert_json_to_projects_map(self, json):
        """ Convert JSON format to the projects map format
        map[ds][repository] = project
        If a repository is in several projects assign to leaf
        Check that all JSON data is in the database

        :param json: data with the projects to repositories mapping
        :returns: the repositories to projects mapping per data source
        """
        ds_repo_to_prj = {}

        for project in json:
            for ds in json[project]:
                if ds == "meta": continue  # not a real data source
                if ds not in ds_repo_to_prj:
                    if not ds in ds_repo_to_prj:
                        ds_repo_to_prj[ds] = {}
                for repo in json[project][ds]:
                    if repo in ds_repo_to_prj[ds]:
                        if project == ds_repo_to_prj[ds][repo]:
                            logging.debug("Duplicated repo: %s %s %s", ds, repo, project)
                        else:
                            if len(project.split(".")) > len(ds_repo_to_prj[ds][repo].split(".")):
                                logging.debug("Changed repo project because we found a leaf: %s leaf vs %s (%s, %s)",
                                              project, ds_repo_to_prj[ds][repo], repo, ds)
                                ds_repo_to_prj[ds][repo] = project
                    else:
                        ds_repo_to_prj[ds][repo] = project
        return  ds_repo_to_prj

    def __compare_projects_map(self, db, json):
        # Compare the projects coming from db and from a json file in eclipse
        ds_map_db = {}
        ds_map_json = {
            "git":"scm",
            "pipermail":"mls",
            "gerrit":"scr",
            "bugzilla":"its"
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
                    logging.error("Project not found in JSON ", project)
                    raise
                else:
                    if ds == 'mls':
                        repo_mls = repository.split("/")[-1]
                        repo_mls = repo_mls.replace(".mbox", "")
                        repository = 'https://dev.eclipse.org/mailman/listinfo/' + repo_mls
                    if ds_map_db[ds] not in json[project]:
                        logging.error("db repository not found in json %s", repository)
                    elif repository not in json[project][ds_map_db[ds]]:
                        logging.error("db repository not found in json %s", repository)

        for project in json.keys():
            if project not in db_projects:
                logging.debug("JSON project %s not found in db" % project)

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
                        logging.debug("Not found repository in db %s %s", repo, ds)

        logging.debug("Number of db projects: %i", len(db_projects))
        logging.debug("Number of json projects: %i (>=%i)", len(json.keys()), len(db_projects))

    def __get_projects_map(self, db_projects_map, db_user=None, db_password=None, db_host=None):
        # Read the repo to project mapping from a database
        ds_repo_to_prj = {}

        db = MySQLdb.connect(user=db_user, passwd=db_password, host=db_host,
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

    def set_elastic(self, elastic):
        self.elastic = elastic

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

    def enrich_items(self, items, events=False):
        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        url = self.elastic.index_url+'/items/_bulk'

        logging.debug("Adding items to %s (in %i packs)", url, max_items)

        if events:
            logging.debug("Adding events items")

        for item in items:
            if current >= max_items:
                try:
                    r = self.requests.put(url, data=bulk_json)
                    r.raise_for_status()
                    logging.debug("Added %i items to %s", total, url)
                except UnicodeEncodeError:
                    # Why is requests encoding the POST data as ascii?
                    logging.error("Unicode error in enriched items")
                    logging.debug(bulk_json)
                    safe_json = str(bulk_json.encode('ascii', 'ignore'), 'ascii')
                    self.requests.put(url, data=safe_json)
                bulk_json = ""
                current = 0

            if not events:
                rich_item = self.get_rich_item(item)
                data_json = json.dumps(rich_item)
                bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                    (item[self.get_field_unique_id()])
                bulk_json += data_json +"\n"  # Bulk document
                current += 1
                total += 1
            else:
                rich_events = self.get_rich_events(item)
                for rich_event in rich_events:
                    data_json = json.dumps(rich_event)
                    bulk_json += '{"index" : {"_id" : "%s_%s" } }\n' % \
                        (item[self.get_field_unique_id()],
                         rich_event[self.get_field_event_unique_id()])
                    bulk_json += data_json +"\n"  # Bulk document
                    current += 1
                    total += 1

        if total == 0:
            # No items enriched, nothing to upload to ES
            return total

        r = self.requests.put(url, data=bulk_json)
        r.raise_for_status()

        return total

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

    def get_email_domain(self, email):
        domain = None
        try:
            domain = email.split("@")[1]
        except IndexError:
            # logging.warning("Bad email format: %s" % (identity['email']))
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

        last_update = self.elastic.get_last_date(self.get_field_date(), _filters)

        return last_update

    def get_last_offset_from_es(self, _filters=[]):
        # offset is always the field name from perceval
        last_update = self.elastic.get_last_offset("offset", _filters)

        return last_update

    def get_elastic_mappings(self):
        """ Mappings for enriched indexes """

        mapping = '{}'

        return {"items":mapping}

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
            grimoire_date = parser.parse(creation_date).isoformat()
        except Exception as ex:
            pass

        name = "is_"+self.get_connector_name()+"_"+item_name

        return {
            "grimoire_creation_date": grimoire_date,
            name: 1
        }

    # Get items for the generator: initial scroll query
    def __get_elastic_items(self, elastic_scroll_id=None, query_string=None):
        """ Get the items from the enriched index related to the backend """

        elastic_page = 1000
        url = self.elastic.index_url
        max_process_items_pack_time = "10m"  # 10 minutes
        url += "/_search?scroll=%s&size=%i" % (max_process_items_pack_time,
                                               elastic_page)
        res_json = None # query results in JSON format

        if elastic_scroll_id:
            """ Just continue with the scrolling """
            url = self.elastic.url
            url += "/_search/scroll"
            scroll_data = {
                "scroll" : max_process_items_pack_time,
                "scroll_id" : elastic_scroll_id
            }
            r = self.requests.post(url, data=json.dumps(scroll_data))
        else:

            if query_string:
                filters = """
                {
                "query_string": {
                    "fields" : ["%s"],
                    "query": "%s"
                    }
                }
                """ % (query_string['fields'], query_string['query'])
            else:

                filters = """
                {
                "query_string": {
                    "analyze_wildcard": true,
                    "query": "*"
                    }
                }
                """
            order_field = self.get_field_date()
            order_query = ''
            order_query = ', "sort": { "%s": { "order": "asc" }} ' % order_field

            query = """
            {
                "query": {
                    "bool": {
                        "must": [%s]
                    }
                } %s
            }
            """ % (filters, order_query)

            logging.debug("%s %s", url, query)

            r = self.requests.post(url, data=query)

        try:
            res_json = r.json()
        except Exception as e:
            print(e)
            logging.error("No JSON found in %s", r.text)
            logging.error("No results found from %s", url)

        return res_json


    # Enriched items generator
    def fetch(self, query_string = None):
        logging.debug("Creating enriched items generator.")

        elastic_scroll_id = None

        while True:
            rjson = self.__get_elastic_items(elastic_scroll_id, query_string)

            if rjson and "_scroll_id" in rjson:
                elastic_scroll_id = rjson["_scroll_id"]

            if rjson and "hits" in rjson:
                if len(rjson["hits"]["hits"]) == 0:
                    break
                for hit in rjson["hits"]["hits"]:
                    eitem = hit['_source']
                    yield eitem
            else:
                logging.error("No results found from %s", self.elastic.index_url)
                break
        return

    # Project field enrichment
    def get_project_repository(self, eitem):
        """
            Get the repository name used for mapping to project name from
            the enriched item.
            To be implemented for each data source
        """
        return ''

    def get_item_project(self, eitem):
        """ Get project mapping enrichment field """
        eitem_project = {}
        ds_name = self.get_connector_name()  # data source name in projects map
        repository = self.get_project_repository(eitem)
        try:
            project = (self.prjs_map[ds_name][repository])
            # logging.debug("Project FOUND for repository %s %s", repository, project)
        except KeyError:
            # logging.warning("Project not found for repository %s (data source: %s)", repository, ds_name)
            project = None
            # Try to use always the origin in any case
            if ds_name in self.prjs_map and eitem['origin'] in self.prjs_map[ds_name]:
                project = self.prjs_map[ds_name][eitem['origin']]

        if project is None:
            project = DEFAULT_PROJECT

        eitem_project = {"project": project}
        # Time to add the project levels: eclipse.platform.releng.aggregator
        eitem_path = ''
        if project is not None:
            subprojects = project.split('.')
            for i in range(0, len(subprojects)):
                if i > 0:
                    eitem_path += "."
                eitem_path += subprojects[i]
                eitem_project['project_' + str(i+1)] = eitem_path
        return eitem_project

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
                # logging.warning("Bad email format: %s" % (identity['email']))
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
            item_date = (item_date-item_date.utcoffset()).replace(tzinfo=None)

        enrollments = self.get_enrollments(uuid)
        enroll = 'Unknown'
        if len(enrollments) > 0:
            for enrollment in enrollments:
                if not item_date:
                    enroll = enrollment.organization.name
                    break
                elif item_date >= enrollment.start and item_date <= enrollment.end:
                    enroll = enrollment.organization.name
                    break
        return enroll

    def get_item_sh_fields(self, identity=None, item_date=None, sh_id=None,
                           rol='author'):
        """ Get standard SH fields from a SH identity """
        eitem_sh = {}  # Item enriched

        if identity:
            # Use the identity to get the SortingHat identity
            sh_ids = self.get_sh_ids(identity, self.get_connector_name())
            eitem_sh[rol+"_id"] = sh_ids['id']
            eitem_sh[rol+"_uuid"] = sh_ids['uuid']
        elif sh_id:
            # Use the SortingHat id to get the identity
            eitem_sh[rol+"_id"] = sh_id
            eitem_sh[rol+"_uuid"] = self.get_uuid_from_id(sh_id)
        else:
            raise RuntimeError("identity or sh_id needed for sortinghat fields")

        # Get the SH profile to use first this data
        profile = self.get_profile_sh(eitem_sh[rol+"_uuid"])

        if profile:
            eitem_sh[rol+"_name"] = profile['name']
            # username is not included in SH profile
            eitem_sh[rol+"_user_name"] = None
            if identity:
                eitem_sh[rol+"_user_name"] = identity['username']
            eitem_sh[rol+"_domain"] = self.get_identity_domain(profile)
        elif not profile and sh_id:
            logger.warning("Can't find SH identity: %s", sh_id)
            return {}
        else:
            # Just use directly the data in the identity
            eitem_sh[rol+"_name"] = identity['name']
            eitem_sh[rol+"_user_name"] = identity['username']
            eitem_sh[rol+"_domain"] = self.get_identity_domain(identity)

        eitem_sh[rol+"_org_name"] = self.get_enrollment(eitem_sh[rol+"_uuid"], item_date)
        eitem_sh[rol+"_bot"] = self.is_bot(eitem_sh[rol+'_uuid'])
        return eitem_sh

    def get_profile_sh(self, uuid):
        profile = {}

        u = self.get_unique_identity(uuid)
        if u.profile:
            profile['name'] = u.profile.name
            profile['email'] = u.profile.email

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

        date = parser.parse(eitem[self.get_field_date()])

        for rol in roles:
            if rol+"_id" not in eitem:
                logging.warning("Enriched index does not include SH ids for %s. Can not refresh it.", rol+"_id")
                continue
            sh_id = eitem[rol+"_id"]
            if not sh_id:
                logging.warning("%s_id is None", sh_id)
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
            item_date = parser.parse(item[self.get_field_date()])
        else:
            item_date = parser.parse(item[date_field])

        users_data = self.get_users_data(item)

        for rol in roles:
            if rol in users_data:
                identity = self.get_sh_identity(item, rol)
                if not identity:
                    continue
                eitem_sh.update(self.get_item_sh_fields(identity, item_date, rol=rol))

        # Add the author field common in all data sources
        rol_author = 'author'
        if author_field in users_data and author_field != rol_author:
            identity = self.get_sh_identity(item, author_field)
            eitem_sh.update(self.get_item_sh_fields(identity, item_date, rol=rol_author))

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

        try:
            # Find the uuid for a given id. A bit hacky in SH yet
            api.add_identity(self.sh_db, backend_name,
                             iden['email'], iden['name'],
                             iden['username'])
        except AlreadyExistsError as ex:
            uuid = ex.uuid
            u = api.unique_identities(self.sh_db, uuid)[0]
            sh_ids['id'] = utils.uuid(backend_name, email=iden['email'],
                                      name=iden['name'], username=iden['username'])
            sh_ids['uuid'] = u.uuid
        except WrappedValueError:
            logger.error("None Identity found")
            logger.error(identity)
        except NotFoundError:
            logger.error("Identity not found in Sorting Hat")
            logger.error(identity)
        except UnicodeEncodeError:
            logger.error("UnicodeEncodeError")
            logger.error(identity)
        except Exception as ex:
            logger.error("Unknown error adding sorting hat identity %s", ex)
            logger.error(identity)
            logger.error(ex)

        return sh_ids
