#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# SortingHat class helper
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
import logging
import traceback

from sortinghat import api
from sortinghat.db.model import Identity, UniqueIdentity
from sortinghat.exceptions import AlreadyExistsError, NotFoundError, WrappedValueError
from sortinghat.matcher import create_identity_matcher

logger = logging.getLogger(__name__)

class SortingHat(object):

    @classmethod
    def get_uuid_from_id(cls, db, sh_id):
        uuid = None

        with db.connect() as session:
            query = session.query(Identity).\
            filter(Identity.id == sh_id)
            identities = query.all()
            if identities:
                uuid = identities[0].uuid
        return uuid

    @classmethod
    def get_github_commit_username(cls, db, identity, source):
        user = None

        with db.connect() as session:
            query = session.query(Identity).\
            filter(Identity.name == identity['name'], Identity.email == identity['email'], Identity.source == source)
            identities = query.all()
            if identities:
                user = {}
                user['name'] = identities[0].name
                user['email'] = identities[0].email
                user['username'] = identities[0].username
        return user

    @classmethod
    def add_identity(cls, db, identity, backend):
        """ Load and identity list from backend in Sorting Hat """
        uuid = None

        try:
            uuid = api.add_identity(db, backend, identity['email'],
                                    identity['name'], identity['username'])

            logger.debug("New sortinghat identity %s %s,%s,%s ",
                        uuid, identity['username'], identity['name'], identity['email'])

            profile = {"name": identity['name'] if identity['name'] else identity['username'],
                       "email": identity['email']}

            api.edit_profile(db, uuid, **profile)

        except AlreadyExistsError as ex:
            uuid = ex.uuid
        except WrappedValueError as ex:
            logging.warning("Trying to add a None identity. Ignoring it.")
        except UnicodeEncodeError as ex:
            logging.warning("UnicodeEncodeError. Ignoring it. %s %s %s",
                            identity['email'], identity['name'],
                            identity['username'])
        except Exception as ex:
            logging.warning("Unknown exception adding identity. Ignoring it. %s %s %s",
                            identity['email'], identity['name'],
                            identity['username'])
            traceback.print_exc()

        if 'company' in identity and identity['company'] is not None:
            try:
                api.add_organization(db, identity['company'])
                api.add_enrollment(db, uuid, identity['company'],
                                   datetime(1900, 1, 1),
                                   datetime(2100, 1, 1))
            except AlreadyExistsError:
                pass

        return uuid


    @classmethod
    def add_identities(cls, db, identities, backend):
        """ Load identities list from backend in Sorting Hat """

        logger.info("Adding the identities to SortingHat")

        total = 0

        for identity in identities:
            cls.add_identity(db, identity, backend)
            total +=1

        logging.info("Total identities added to SH: %i", total)
