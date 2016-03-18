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

from functools import lru_cache
import logging
import MySQLdb


from sortinghat.db.database import Database
from sortinghat import api
from sortinghat.exceptions import AlreadyExistsError, NotFoundError, WrappedValueError

logger = logging.getLogger(__name__)

class Enrich(object):

    def __init__(self, db_projects_map = None, db_sortinghat = None, ):
        self.sortinghat = False
        if db_sortinghat:
            self.sh_db = Database("root", "", db_sortinghat, "mariadb")
            self.sortinghat = True
        self.prjs_map = None
        if  db_projects_map:
            self.prjs_map = self._get_projects_map(db_projects_map)

    def _get_projects_map(self, db_projects_map):
        prjs_map = {}

        db = MySQLdb.connect(user="root", passwd="", host="mariadb",
                             db = db_projects_map)
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
                if ds not in prjs_map:
                    prjs_map[ds] = {}
                prjs_map[ds][repo] = name
        else:
            raise RuntimeException("Can't find projects mapping in %s" % (db_projects_map))
        return prjs_map

    def set_elastic(self, elastic):
        self.elastic = elastic

    def enrich_items(self, items):
        raise NotImplementedError

    def get_connector_name(self):
        """ Find the name for the current connector """
        from ..utils import get_connector_name
        return get_connector_name(type(self))

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        raise NotImplementedError

    def get_fields_uuid(self):
        """ Fields with unique identities in the JSON enriched items """
        raise NotImplementedError

    def get_identities(self, item):
        """ Return the identities from an item """
        raise NotImplementedError

    def get_item_id(self, eitem):
        """ Return the item_id linked to this enriched eitem """

        # If possible, enriched_item and item will have the same id
        return eitem["_id"]

    def get_last_update_from_es(self, _filter=None):

        last_update = self.elastic.get_last_date(self.get_field_date(), _filter)

        return last_update

    def get_elastic_mappings(self):
        """ Mappings for enriched indexes """
        pass


    # Sorting Hat stuff to be moved to SortingHat class
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
        return uuid
