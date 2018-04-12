#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2016 Bitergia
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
#     Valerio Cosentino <valcos@bitergia.com>
#

import configparser
import datetime
import logging
import random
import string
import sys
import unittest

if '..' not in sys.path:
    sys.path.insert(0, '..')

from grimoire_elk.utils import get_connectors
from grimoire_elk.enriched.sortinghat_gelk import SortingHat

CONFIG_FILE = 'tests.conf'
DB_SORTINGHAT = "test_sh"

logger = logging.getLogger(__name__)


def create_fake_identities():
    identities = []

    for i in range(10):
        username = ''.join(random.choice(string.ascii_lowercase) for _ in range(random.randint(10, 15)))
        email = ''.join(random.choice(string.ascii_lowercase) for _ in range(random.randint(10, 15))) + "@mail.com"
        name = ''.join(random.choice(string.ascii_lowercase) for _ in range(random.randint(10, 20)))

        identity = {'username': username, 'email': email, 'name': name}
        identities.append(identity)

    return identities


class TestLoadIdentities(unittest.TestCase):
    """Tests performance of load identities in GrimoireELK"""

    @classmethod
    def setUpClass(cls):
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)

        # Sorting hat settings
        cls.db_user = ''
        cls.db_password = ''
        if 'Database' in config:
            if 'user' in config['Database']:
                cls.db_user = config['Database']['user']
            if 'password' in config['Database']:
                cls.db_password = config['Database']['password']

    def setUp(self):

        # The name of the connector is needed only to get access to the SortingHat DB
        self.enrich_backend = get_connectors()["github"][2](db_sortinghat=DB_SORTINGHAT,
                                                            db_user=self.db_user,
                                                            db_password=self.db_password)

    def _test_load_identities(self, items=10):
        """Test whether fetched items are properly loaded to ES"""

        start = datetime.datetime.now().timestamp()
        new_identities = []

        for i in range(items):
            identities = create_fake_identities()

            for identity in identities:
                if identity not in new_identities:
                    new_identities.append(identity)

        next = datetime.datetime.now().timestamp()
        print("create %s identities: %s" % (len(new_identities), next - start))

        SortingHat.add_identities(self.enrich_backend.sh_db, new_identities,
                                  self.enrich_backend.get_connector_name())

        stop = datetime.datetime.now().timestamp()
        print("add identities: %s" % (stop - next))
        print("end load identities: %s" % (stop - start))

        self.assertLess((stop - start), 5.0)

    def test_load_identities(self):

        self._test_load_identities()
