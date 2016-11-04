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
import logging

import requests

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
    from sortinghat import api
    from sortinghat.exceptions import AlreadyExistsError, NotFoundError, WrappedValueError
    SORTINGHAT_LIBS = True
except ImportError:
    logger.info("SortingHat not available")
    SORTINGHAT_LIBS = False

DEFAULT_PROJECT = 'Main'
DEFAULT_DB_USER = 'root'

class Enrich(object):

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', insecure=True):
        self.sortinghat = False
        if db_user == '':
            db_user = DEFAULT_DB_USER
        if db_sortinghat and not SORTINGHAT_LIBS:
            raise RuntimeError("Sorting hat configured but libraries not available.")
        if db_sortinghat:
            # self.sh_db = Database("root", "", db_sortinghat, "mariadb")
            self.sh_db = Database(db_user, db_password, db_sortinghat, db_host)
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

        self.requests = requests.Session()
        if insecure:
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
            self.requests.verify = False

        self.elastic = None
        self.type_name = "items"  # type inside the index to store items enriched

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
                    self.requests.put(url, data=bulk_json)
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
        self.requests.put(url, data = bulk_json)

        return total

    def get_connector_name(self):
        """ Find the name for the current connector """
        from ..utils import get_connector_name
        return get_connector_name(type(self))

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

    def get_last_update_from_es(self, _filter=None):

        last_update = self.elastic.get_last_date(self.get_field_date(), _filter)

        return last_update

    def get_last_offset_from_es(self, _filter=None):
        # offset is always the field name from perceval
        last_update = self.elastic.get_last_offset("offset", _filter)

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

    # Project field enrichment

    def get_project_repository(self, item):
        """
            Get the repository name used for mapping to project name
            To be implemented for each data source
        """
        return ''

    def get_item_project(self, item):
        """ Get project mapping enrichment field """
        item_project = {}
        ds_name = self.get_connector_name()  # data source name in projects map
        repository = self.get_project_repository(item)
        try:
            project = (self.prjs_map[ds_name][repository])
            # logging.debug("Project FOUND for repository %s %s", repository, project)
        except KeyError:
            # logging.warning("Project not found for repository %s (data source: %s)", repository, ds_name)
            project = None
            # Try to use always the origin in any case
            if ds_name in self.prjs_map and item['origin'] in self.prjs_map[ds_name]:
                project = self.prjs_map[ds_name][item['origin']]

        if project is None:
            project = DEFAULT_PROJECT

        item_project = {"project": project}
        # Time to add the project levels: eclipse.platform.releng.aggregator
        item_path = ''
        if project is not None:
            subprojects = project.split('.')
            for i in range(0, len(subprojects)):
                if i > 0:
                    item_path += "."
                item_path += subprojects[i]
                item_project['project_' + str(i+1)] = item_path
        return item_project

    # Sorting Hat stuff to be moved to SortingHat class

    def get_sh_identity(self, identity):
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
        u = self.get_unique_identities(uuid)[0]
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

    def get_item_sh_fields(self, identity, item_date):
        """ Get standard SH fields from a SH identity """
        eitem = {}  # Item enriched

        eitem["author_uuid"] = self.get_uuid(identity, self.get_connector_name())
        # Always try to use first the data from SH
        identity_sh = self.get_identity_sh(eitem["author_uuid"])

        if identity_sh:
            eitem["author_name"] = identity_sh['name']
            eitem["author_user_name"] = identity_sh['username']
            eitem["author_domain"] = self.get_identity_domain(identity_sh)
        else:
            eitem["author_name"] = identity['name']
            eitem["author_user_name"] = identity['username']
            eitem["author_domain"] = self.get_identity_domain(identity)

        eitem["author_org_name"] = self.get_enrollment(eitem["author_uuid"], item_date)
        eitem["author_bot"] = self.is_bot(eitem['author_uuid'])
        return eitem

    def get_identity_sh(self, uuid):
        identity = {}

        u = self.get_unique_identities(uuid)[0]
        if u.profile:
            identity['name'] = u.profile.name
            identity['username'] = None
            identity['email'] = u.profile.email

        return identity

    def get_item_sh(self, item, identity_field):
        """ Add sorting hat enrichment fields for the author of the item """

        eitem = {}  # Item enriched
        if 'data' in item:
            # perceval data
            data = item['data']
        else:
            data = item

        # Add Sorting Hat fields
        if identity_field not in data:
            return eitem
        identity  = self.get_sh_identity(data[identity_field])
        eitem = self.get_item_sh_fields(identity, parser.parse(item[self.get_field_date()]))

        return eitem

    @lru_cache()
    def get_enrollments(self, uuid):
        return api.enrollments(self.sh_db, uuid)

    @lru_cache()
    def get_unique_identities(self, uuid):
        return api.unique_identities(self.sh_db, uuid)

    def get_uuid(self, identity, backend_name):
        """ Return the Sorting Hat uuid for an identity """
        # Convert the dict to tuple so it is hashable
        identity_tuple = tuple(identity.items())
        uuid = self.__get_uuid_cache(identity_tuple, backend_name)
        return uuid

    @lru_cache()
    def __get_uuid_cache(self, identity_tuple, backend_name):

        # Convert tuple to the original dict
        identity = dict((x, y) for x, y in identity_tuple)

        if not self.sortinghat:
            raise RuntimeError("Sorting Hat not active during enrich")

        iden = {}
        uuid = None

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
            uuid = u.uuid
        except WrappedValueError:
            logger.error("None Identity found")
            logger.error("%s %s" % (identity, uuid))
            uuid = None
        except NotFoundError:
            logger.error("Identity found in Sorting Hat which is not unique")
            logger.error("%s %s" % (identity, uuid))
            uuid = None
        except UnicodeEncodeError:
            logger.error("UnicodeEncodeError")
            logger.error("%s %s" % (identity, uuid))
            uuid = None
        except Exception as ex:
            logger.error("Unknown error adding sorting hat identity.")
            logger.error("%s %s" % (identity, uuid))
            uuid = None
        return uuid
