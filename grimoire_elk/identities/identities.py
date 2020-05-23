# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2020 Bitergia
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
#   Jose Javier Merchante Picazo <jjmerchante@gcauldron.io>
#

import logging

from functools import lru_cache

from grimoirelab_toolkit.datetime import (str_to_datetime)
from ..elastic_items import ElasticItems

from ..enriched.sortinghat_gelk import MULTI_ORG_NAMES

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

    from ..enriched.sortinghat_gelk import SortingHat

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

import hashlib


class Identities:
    """Class for managing identities.

    This class will be subclassed by backends,
    which will provide specific implementations.
    """
    def __init__(self):
        self.elastic = None
        self.type_name = "items"  # type inside the index to store items enriched
        self.backend_params = None
        self.unaffiliated_group = 'Unknown'
        self.unknown_gender = 'Unknown'

    @staticmethod
    def _hash(name):
        sha1 = hashlib.sha1(name.encode('UTF-8', errors="surrogateescape"))
        return sha1.hexdigest()

    @classmethod
    def anonymize_item(cls, item):
        """Remove or hash the fields that contain personal information"""

        pass

    def get_field_unique_id(self):
        """ Field in the raw item with the unique id """
        return "uuid"

    def get_field_event_unique_id(self):
        """ Field in the rich event with the unique id """
        raise NotImplementedError

    def get_field_author(self):
        """ Field with the author information """
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
        backend_name = self.get_connector_name()

        if not (username or email or name):
            return self.__get_item_sh_fields_empty(rol)

        uuid = utils.uuid(backend_name, email=email,
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
            logger.warning("Can't find SH identity profile: {}".format(sh_id))

        # Ensure we always write gender fields
        if not eitem_sh.get(rol + "_gender"):
            eitem_sh[rol + "_gender"] = self.unknown_gender
            eitem_sh[rol + "_gender_acc"] = 0

        eitem_sh[rol + "_org_name"] = self.get_enrollment(eitem_sh[rol + "_uuid"], item_date)
        eitem_sh[rol + "_bot"] = self.is_bot(eitem_sh[rol + '_uuid'])

        eitem_sh[rol + MULTI_ORG_NAMES] = self.get_multi_enrollment(eitem_sh[rol + "_uuid"], item_date)
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
                logger.debug("Enriched index does not include SH ids for {}_id. Can not refresh it.".format(rol))
                continue
            sh_id = eitem[rol + "_id"]
            if not sh_id:
                logger.debug("{}_id is None".format(rol))
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

    def get_multi_enrollment(self, uuid, item_date):
        """ Get the enrollments for the uuid when the item was done """

        enrolls = []

        # item_date must be offset-naive (utc)
        if item_date and item_date.tzinfo:
            item_date = (item_date - item_date.utcoffset()).replace(tzinfo=None)

        enrollments = self.get_enrollments(uuid)

        if enrollments:
            for enrollment in enrollments:
                if not item_date:
                    enrolls.append(enrollment.organization.name)
                elif enrollment.start <= item_date <= enrollment.end:
                    enrolls.append(enrollment.organization.name)
        else:
            enrolls.append(self.unaffiliated_group)

        return enrolls

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
            logger.warning("Name, email and username are none in {}".format(backend_name))
            return sh_ids

        try:
            # Find the uuid for a given id.
            id = utils.uuid(backend_name, email=iden['email'],
                            name=iden['name'], username=iden['username'])

            if not id:
                logger.warning("Id not found in SortingHat for name: {}, email: {} and username: {} in {}".format(
                               iden['name'], iden['email'], iden['username'], backend_name))
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
