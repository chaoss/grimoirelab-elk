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
#     Alvaro del Castillo <acs@bitergia.com>
#     Valerio Cosentino <valcos@bitergia.com>
#

import json
import logging
import os
import sys
import unittest

if '..' not in sys.path:
    sys.path.insert(0, '..')

from grimoire_elk.arthur import load_identities
from grimoire_elk.utils import get_connectors, get_elastic
from tests.base import (TestCaseBackend,
                        data2es,
                        refresh_identities,
                        refresh_projects,
                        DB_PROJECTS, DB_SORTINGHAT)


class TestAskbotBackend(TestCaseBackend):
    """Askbot backend tests."""

    def setUp(self):
        super().setUp()
        self.connector_name = 'askbot'

    def test_read_data(self):
        """Test whether test JSON data is available"""

        with open(os.path.join("data", self.connector_name + ".json")) as f:
            json.load(f)

    def test_raw(self):
        """Test whether raw data is properly inserted"""

        connector = get_connectors()[self.connector_name]
        with open(os.path.join("data", self.connector_name + ".json")) as f:
            items = json.load(f)
            es_index = "test_" + self.connector_name
            clean = True
            perceval_backend = None
            ocean_backend = connector[1](perceval_backend)
            elastic_ocean = get_elastic(self.es_con, es_index, clean, ocean_backend)
            ocean_backend.set_elastic(elastic_ocean)
            inserted = data2es(items, ocean_backend)

            self.assertTrue(len(items), inserted)

    def test_enrich(self, sortinghat=False, projects=False):
        """Test whether enriched data is properly inserted"""

        connector = get_connectors()[self.connector_name]
        perceval_backend = None
        ocean_index = "test_" + self.connector_name
        enrich_index = "test_" + self.connector_name + "_enrich"
        clean = False
        ocean_backend = connector[1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, ocean_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)
        clean = True

        if not sortinghat and not projects:
            enrich_backend = connector[2]()
        elif sortinghat and not projects:
            enrich_backend = connector[2](db_sortinghat=DB_SORTINGHAT,
                                          db_user=self.db_user,
                                          db_password=self.db_password)
        elif not sortinghat and projects:
            enrich_backend = connector[2](db_projects_map=DB_PROJECTS,
                                          db_user=self.db_user,
                                          db_password=self.db_password)
        elastic_enrich = get_elastic(self.es_con, enrich_index, clean, enrich_backend)

        enrich_backend.set_elastic(elastic_enrich)
        if sortinghat:
            # Load SH identities
            load_identities(ocean_backend, enrich_backend)
        enrich_count = enrich_backend.enrich_items(ocean_backend)

        if enrich_count is not None:
            logging.info("Total items enriched %i ", enrich_count)

    def test_enrich_sh(self):
        """Test enrich with SortingHat"""

        self.test_enrich(sortinghat=True)

    def test_enrich_projects(self):
        """Test enrich with Projects"""

        self.test_enrich(projects=True)

    def test_refresh_identities(self):
        """Test refresh identities"""

        connector = get_connectors()[self.connector_name]
        enrich_index = "test_" + self.connector_name + "_enrich"
        enrich_backend = connector[2](db_sortinghat=DB_SORTINGHAT,
                                      db_user=self.db_user,
                                      db_password=self.db_password)
        clean = False
        elastic_enrich = get_elastic(self.es_con, enrich_index, clean, enrich_backend)
        enrich_backend.set_elastic(elastic_enrich)
        refresh_identities(enrich_backend)

    def test_refresh_project(self):
        """Test refresh project field"""

        connectors = get_connectors()
        for con in sorted(connectors.keys()):
            enrich_index = "test_" + con + "_enrich"
            enrich_backend = connectors[con][2](db_projects_map=DB_PROJECTS,
                                                db_user=self.db_user,
                                                db_password=self.db_password)
            clean = False
            elastic_enrich = get_elastic(self.es_con, enrich_index, clean, enrich_backend)
            enrich_backend.set_elastic(elastic_enrich)
            refresh_projects(enrich_backend)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
