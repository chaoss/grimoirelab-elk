# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2023 Bitergia
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
#     Alvaro del Castillo <acs@bitergia.com>
#     Valerio Cosentino <valcos@bitergia.com>
#     Venu Vardhan Reddy Tekula <venu@bitergia.com>
#


import logging
# import time
import unittest
from unittest.mock import MagicMock

from base import (TestBaseBackend, refresh_identities, data2es,
                  DB_SORTINGHAT, DB_HOST, FILE_PROJECTS)
from grimoire_elk.enriched.utils import REPO_LABELS
from grimoire_elk.utils import get_elastic
from grimoire_elk.elk import load_identities

from perceval.backends.core.discourse import Discourse


class TestDiscourse(TestBaseBackend):
    """Test Discourse backend"""

    connector = "discourse"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_has_identites(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()
        self.assertEqual(result['items'], 3)
        self.assertEqual(result['raw'], 3)

    def _test_raw_to_enrich(self, sortinghat=False, projects=False, pair_programming=False):
        """Test whether raw indexes are properly enriched"""

        # populate raw index
        perceval_backend = None
        clean = True
        self.ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, self.ocean_backend)
        self.ocean_backend.set_elastic(elastic_ocean)
        data2es(self.items, self.ocean_backend)

        # populate enriched index
        if not sortinghat and not projects:
            self.enrich_backend = self.connectors[self.connector][2]()
        elif sortinghat and not projects:
            self.enrich_backend = self.connectors[self.connector][2](db_sortinghat=DB_SORTINGHAT,
                                                                     db_user=self.db_user,
                                                                     db_password=self.db_password,
                                                                     db_host=DB_HOST)
        elif sortinghat and projects:
            self.enrich_backend = self.connectors[self.connector][2](json_projects_map=FILE_PROJECTS,
                                                                     db_sortinghat=DB_SORTINGHAT,
                                                                     db_user=self.db_user,
                                                                     db_password=self.db_password,
                                                                     db_host=DB_HOST)
        elif not sortinghat and projects:
            self.enrich_backend = self.connectors[self.connector][2](json_projects_map=FILE_PROJECTS,
                                                                     db_user=self.db_user,
                                                                     db_password=self.db_password,
                                                                     db_host=DB_HOST)
        if pair_programming:
            self.enrich_backend.pair_programming = pair_programming

        elastic_enrich = get_elastic(self.es_con, self.enrich_index, clean, self.enrich_backend, self.enrich_aliases)
        self.enrich_backend.set_elastic(elastic_enrich)

        categories = {1: 'General', 6: 'Technical', 2: 'Ecosystem', 3: 'Staff'}
        self.enrich_backend.categories = MagicMock(return_value=categories)

        categories_tree = {1: {}, 6: {}, 2: {}, 3: {}}
        self.enrich_backend.categories_tree = MagicMock(return_value=categories_tree)

        # Load SH identities
        if sortinghat:
            load_identities(self.ocean_backend, self.enrich_backend)

        raw_count = len([item for item in self.ocean_backend.fetch()])
        enrich_count = self.enrich_backend.enrich_items(self.ocean_backend)
        # self._test_csv_mappings(sortinghat)

        return {'raw': raw_count, 'enrich': enrich_count}

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()
        self.assertEqual(result['raw'], 3)
        self.assertEqual(result['enrich'], 35)

        enrich_backend = self.connectors[self.connector][2]()

        categories = {1: 'General', 6: 'Technical', 2: 'Ecosystem', 3: 'Staff'}
        enrich_backend.categories = MagicMock(return_value=categories)

        categories_tree = {1: {}, 6: {}, 2: {}, 3: {}}
        enrich_backend.categories_tree = MagicMock(return_value=categories_tree)

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertIn('question_title', eitem)
        self.assertIn('question_title_analyzed', eitem)

    def test_enrich_repo_labels(self):
        """Test whether the field REPO_LABELS is present in the enriched items"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        categories = {1: 'General', 6: 'Technical', 2: 'Ecosystem', 3: 'Staff'}
        enrich_backend.categories = MagicMock(return_value=categories)

        categories_tree = {1: {}, 6: {}, 2: {}, 3: {}}
        enrich_backend.categories_tree = MagicMock(return_value=categories_tree)

        for item in self.items:
            eitem = enrich_backend.get_rich_item(item)
            self.assertIn(REPO_LABELS, eitem)

    def test_raw_to_enrich_sorting_hat(self):
        """Test enrich with SortingHat"""

        result = self._test_raw_to_enrich(sortinghat=True)
        self.assertEqual(result['raw'], 3)
        self.assertEqual(result['enrich'], 35)

        enrich_backend = self.connectors[self.connector][2]()

        categories = {1: 'General', 6: 'Technical', 2: 'Ecosystem', 3: 'Staff'}
        enrich_backend.categories = MagicMock(return_value=categories)

        categories_tree = {1: {}, 6: {}, 2: {}, 3: {}}
        enrich_backend.categories_tree = MagicMock(return_value=categories_tree)

        url = self.es_con + "/" + self.enrich_index + "/_search"
        response = enrich_backend.requests.get(url, verify=False).json()
        for hit in response['hits']['hits']:
            source = hit['_source']
            if 'author_uuid' in source:
                self.assertIn('author_domain', source)
                self.assertIn('author_gender', source)
                self.assertIn('author_gender_acc', source)
                self.assertIn('author_org_name', source)
                self.assertIn('author_bot', source)
                self.assertIn('author_multi_org_names', source)

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        self.assertEqual(result['raw'], 3)
        self.assertEqual(result['enrich'], 35)

    def test_copy_raw_fields(self):
        """Test copied raw fields"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        categories = {1: 'General', 6: 'Technical', 2: 'Ecosystem', 3: 'Staff'}
        enrich_backend.categories = MagicMock(return_value=categories)

        categories_tree = {1: {}, 6: {}, 2: {}, 3: {}}
        enrich_backend.categories_tree = MagicMock(return_value=categories_tree)

        for item in self.items:
            eitem = enrich_backend.get_rich_item(item)
            for attribute in enrich_backend.RAW_FIELDS_COPY:
                if attribute in item:
                    self.assertEqual(item[attribute], eitem[attribute])
                else:
                    self.assertIsNone(eitem[attribute])

    def test_refresh_identities(self):
        """Test refresh identities"""

        # populate raw index
        perceval_backend = Discourse('https://example.com', api_token='1234', api_username='user')
        clean = True
        self.ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, self.ocean_backend)
        self.ocean_backend.set_elastic(elastic_ocean)
        data2es(self.items, self.ocean_backend)

        # populate enriched index
        self.enrich_backend = self.connectors[self.connector][2]()
        load_identities(self.ocean_backend, self.enrich_backend)
        self.enrich_backend = self.connectors[self.connector][2](db_sortinghat=DB_SORTINGHAT,
                                                                 db_user=self.db_user,
                                                                 db_password=self.db_password,
                                                                 db_host=DB_HOST)
        elastic_enrich = get_elastic(self.es_con, self.enrich_index, clean, self.enrich_backend)
        self.enrich_backend.set_elastic(elastic_enrich)
        self.enrich_backend.enrich_items(self.ocean_backend)
        total = refresh_identities(self.enrich_backend)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
