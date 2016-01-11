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

import logging

from sortinghat.db.database import Database
from sortinghat import api
from sortinghat.exceptions import AlreadyExistsError, NotFoundError, WrappedValueError

logger = logging.getLogger(__name__)

class Enrich(object):

    def __init__(self):
        self.sh_db = Database("root", "", "ocean_sh", "mariadb")

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

    def get_last_update_from_es(self, _filter = None):

        last_update = self.elastic.get_last_date(self.get_field_date(), _filter)

        return last_update

    def get_elastic_mappings(self):
        """ Mappings for enriched indexes """
        pass

    def get_uuid(self, identity, backend_name):
        """ Return the Sorting Hat uuid for an identity """
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
        return uuid
