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

from sortinghat import api
from sortinghat.db.database import Database
from sortinghat.db.model import UniqueIdentity
from sortinghat.exceptions import AlreadyExistsError, NotFoundError
from sortinghat.matcher import create_identity_matcher

logger = logging.getLogger(__name__)

class SortingHat(object):

    @classmethod
    def add_identities(cls, identities, backend):
        """ Load identities list from backend in Sorting Hat """

        logger.info("Adding the identities to SortingHat")

        db = Database("root", "", "ocean_sh", "mariadb")

        total = 0

        matching = 'email-name';

        merged_identities = []  # old identities merged into new ones
        blacklist = api.blacklist(db)
        matcher = create_identity_matcher(matching, blacklist)

        for identity in identities:
            try:
                uuid = api.add_identity(db, backend, identity['email'],
                                        identity['name'], identity['username'])

                logger.info("New sortinghat identity %s %s %s" % (uuid, identity['name'], identity['email']))

                total += 1
                # Time to  merge
                matches = api.match_identities(db, uuid, matcher)

                if len(matches) > 1:
                    u = api.unique_identities(db, uuid)[0]
                    for m in matches:
                        if m.uuid == uuid:
                            continue
                        api.merge_unique_identities(db, u.uuid, m.uuid)
                        uuid = m.uuid
                        u = api.unique_identities(db, uuid, backend)
                        # Include all identities related to this uuid
                        merged_identities += u.identities

            except AlreadyExistsError as ex:
                uuid = ex.uuid


            if 'company' in identity and identity['company'] is not None:
                try:
                    api.add_organization(db, identity['company'])
                except AlreadyExistsError:
                    pass

                api.add_enrollment(db, uuid, identity['company'],
                                   datetime(1900, 1, 1),
                                   datetime(2100, 1, 1))

        logger.info("Total NEW identities: %i" % (total))
        logger.info("Total NEW identities merged: %i" % (len(merged_identities)))

        return merged_identities
