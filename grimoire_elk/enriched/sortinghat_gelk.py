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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import datetime
import logging

from grimoirelab_toolkit.datetime import datetime_utcnow
from sortinghat import api
from sortinghat.db.database import Database
from sortinghat.db.model import Identity
from sortinghat.exceptions import AlreadyExistsError, InvalidValueError


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
            uuid = ex.eid
        except InvalidValueError as ex:
            logger.warning("Trying to add a None identity. Ignoring it.")
        except UnicodeEncodeError as ex:
            logger.warning("UnicodeEncodeError. Ignoring it. %s %s %s",
                           identity['email'], identity['name'],
                           identity['username'])
        except Exception as ex:
            logger.warning("Unknown exception adding identity. Ignoring it. %s %s %s",
                           identity['email'], identity['name'],
                           identity['username'], exc_info=True)

        if 'company' in identity and identity['company'] is not None:
            try:
                api.add_organization(db, identity['company'])
                api.add_enrollment(db, uuid, identity['company'],
                                   datetime.datetime(1900, 1, 1),
                                   datetime.datetime(2100, 1, 1))
            except AlreadyExistsError:
                pass

        return uuid

    @classmethod
    def add_identities(cls, db, identities, backend):
        """ Load identities list from backend in Sorting Hat """

        logger.info("Adding the identities to SortingHat")

        total = 0

        for identity in identities:
            try:
                cls.add_identity(db, identity, backend)
                total += 1
            except Exception as e:
                logger.error("Unexcepted error when adding identities: %s" % e)
                continue

        logger.info("Total identities added to SH: %i", total)

    @classmethod
    def remove_uidentities(cls, hours_to_retain, sh_user, sh_pwd, sh_db, sh_host):
        """Delete unique identities from Sorting Hat before a given date.

        :param hours_to_retain: maximum number of hours wrt the current date to retain unique identities
        :param sh_user: SortingHat user
        :param sh_pwd: SortingHat password
        :param sh_db: SortingHat database
        param sh_host: SortingHat host
        """
        if hours_to_retain is None:
            logger.debug("Data retention policy disabled, no uidentities will be deleted.")
            return

        if hours_to_retain <= 0:
            logger.debug("Hours to retain must be greater than 0.")
            return

        deleted_uidentities = 0
        before_date = datetime_utcnow() - datetime.timedelta(hours=hours_to_retain)
        before_date = before_date.replace(minute=0, second=0, microsecond=0, tzinfo=None)

        sh_db = Database(sh_user, sh_pwd, sh_db, sh_host)
        uidentities = api.unique_identities(sh_db)

        for uidentity in uidentities:
            if uidentity.last_modified <= before_date:

                try:
                    api.delete_unique_identity(sh_db, uidentity.uuid)
                    logger.debug("Unique identity %s deleted since last modified before %s.",
                                 uidentity.uuid, before_date.isoformat())
                    deleted_uidentities += 1
                except Exception as e:
                    logger.debug("Impossible to delete unique identity %s: %s", uidentity.uuid, str(e))

        logger.debug("%s unique identities deleted since last modified before %s.",
                     deleted_uidentities, before_date.isoformat())
