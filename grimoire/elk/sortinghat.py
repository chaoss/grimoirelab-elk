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
from sortinghat.db.model import UniqueIdentity
from sortinghat.exceptions import AlreadyExistsError, NotFoundError, WrappedValueError
from sortinghat.matcher import create_identity_matcher

logger = logging.getLogger(__name__)

class SortingHat(object):

    @classmethod
    def add_identities(cls, db, identities, backend):
        """ Load identities list from backend in Sorting Hat """

        merge_identities = False

        logger.info("Adding the identities to SortingHat")
        if not merge_identities:
            logger.info("Not doing identities merge")

        total = 0
        lidentities = len(identities)


        if merge_identities:
            merged_identities = []  # old identities merged into new ones
            blacklist = api.blacklist(db)
            matching = 'email-name'  # Not active
            matcher = create_identity_matcher(matching, blacklist)

        for identity in identities:
            try:
                uuid = api.add_identity(db, backend, identity['email'],
                                        identity['name'], identity['username'])

                logger.debug("New sortinghat identity %s %s,%s,%s (%i/%i)" % \
                            (uuid, identity['username'], identity['name'], identity['email'],
                            total, lidentities))

                total += 1
                if not merge_identities:
                    continue  # Don't do the merge here. Too slow in large projects

                # Time to  merge
                matches = api.match_identities(db, uuid, matcher)

                if len(matches) > 1:
                    u = api.unique_identities(db, uuid)[0]
                    for m in matches:
                        # First add the old uuid to the list of changed by merge uuids
                        if m.uuid not in merged_identities:
                            merged_identities.append(m.uuid)
                        if m.uuid == uuid:
                            continue
                        # Merge matched identity into added identity
                        api.merge_unique_identities(db, m.uuid, u.uuid)
                        # uuid = m.uuid
                        # u = api.unique_identities(db, uuid, backend)[0]
                        # Include all identities related to this uuid
                        # merged_identities.append(m.uuid)

            except AlreadyExistsError as ex:
                uuid = ex.uuid
                continue
            except WrappedValueError as ex:
                logging.warning("Trying to add a None identity. Ignoring it.")
                continue
            except UnicodeEncodeError as ex:
                logging.warning("UnicodeEncodeError. Ignoring it. %s %s %s" % \
                                (identity['email'], identity['name'],
                                identity['username']))
                continue
            except Exception as ex:
                logging.warning("Unknown exception adding identity. Ignoring it. %s %s %s" % \
                                (identity['email'], identity['name'],
                                identity['username']))
                continue

            if 'company' in identity and identity['company'] is not None:
                try:
                    api.add_organization(db, identity['company'])
                    api.add_enrollment(db, uuid, identity['company'],
                                       datetime(1900, 1, 1),
                                       datetime(2100, 1, 1))
                except AlreadyExistsError:
                    pass

        logger.info("Total NEW identities: %i" % (total))

        if merge_identities:
            logger.info("Total NEW identities merged: %i" % \
                        (len(merged_identities)))
            return merged_identities
        else:
            return []
