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
#     Animesh Kumar <animuz111@gmail.com>
#
import logging
import unittest

import requests
from base import TestBaseBackend
import grimoire_elk.enriched.pagure as pagure_enriched
from grimoire_elk.enriched.utils import REPO_LABELS
from grimoire_elk.raw.pagure import PagureOcean


class TestPagure(TestBaseBackend):
    """Test Pagure backend"""

    connector = "pagure"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_has_identites(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()

        self.assertEqual(result['items'], 7)
        self.assertEqual(result['raw'], 7)
        self.assertEqual(result['items'], result['raw'])

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()

        self.assertEqual(result['raw'], 7)
        self.assertEqual(result['enrich'], 26)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'issue')
        self.assertEqual(eitem['id'], 1)
        self.assertEqual(eitem['origin'], 'https://pagure.io/Test-group/Project-example-namespace')
        self.assertEqual(eitem['tag'], 'https://pagure.io/Test-group/Project-example-namespace')
        self.assertEqual(eitem['author_username'], 'animeshk08')
        self.assertEqual(eitem['assignee_username'], 'animeshk08')
        self.assertEqual(eitem['title'], 'Issue Title 1')
        self.assertEqual(eitem['status'], 'Open')
        self.assertIsNone(eitem['close_status'])
        self.assertEqual(eitem['num_comments'], 1)
        self.assertListEqual(eitem['blocks'], ['3'])
        self.assertListEqual(eitem['tags'], [])
        self.assertListEqual(eitem['custom_fields'], [])
        self.assertIsNone(eitem['time_to_close_days'])
        self.assertGreater(eitem['time_open_days'], 0.0)
        self.assertIsNone(eitem['time_to_first_attention'])

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'issue')
        self.assertEqual(eitem['id'], 2)
        self.assertEqual(eitem['origin'], 'https://pagure.io/Test-group/Project-example-namespace')
        self.assertEqual(eitem['tag'], 'https://pagure.io/Test-group/Project-example-namespace')
        self.assertEqual(eitem['author_username'], 'animeshk08')
        self.assertIsNone(eitem['assignee_username'])
        self.assertEqual(eitem['title'], 'Issue Title 3')
        self.assertEqual(eitem['status'], 'Open')
        self.assertIsNone(eitem['close_status'])
        self.assertEqual(eitem['num_comments'], 4)
        self.assertListEqual(eitem['blocks'], [])
        self.assertListEqual(eitem['tags'], [])
        self.assertIsNotNone(eitem['custom_fields'])
        self.assertIsNone(eitem['time_to_close_days'])
        self.assertGreater(eitem['time_open_days'], 0.0)
        self.assertIsNone(eitem['time_to_first_attention'])

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'issue')
        self.assertEqual(eitem['id'], 3)
        self.assertEqual(eitem['origin'], 'https://pagure.io/Test-group/Project-example-namespace')
        self.assertEqual(eitem['tag'], 'https://pagure.io/Test-group/Project-example-namespace')
        self.assertEqual(eitem['author_username'], 'animeshk08')
        self.assertEqual(eitem['assignee_username'], 'animeshk0806')
        self.assertEqual(eitem['title'], 'Sample Title 3')
        self.assertEqual(eitem['status'], 'Open')
        self.assertEqual(eitem['milestone'], 'Milestone1')
        self.assertIsNone(eitem['close_status'])
        self.assertEqual(eitem['num_comments'], 9)
        self.assertListEqual(eitem['blocks'], ['2'])
        self.assertListEqual(eitem['tags'], ['Tag1'])
        self.assertListEqual(eitem['custom_fields'], [])
        self.assertIsNone(eitem['time_to_close_days'])
        self.assertGreater(eitem['time_open_days'], 0.0)
        self.assertEqual(eitem['time_to_first_attention'], 0.0)

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'issue')
        self.assertEqual(eitem['id'], 5)
        self.assertEqual(eitem['origin'], 'https://pagure.io/Test-group/Project-example-namespace')
        self.assertEqual(eitem['tag'], 'https://pagure.io/Test-group/Project-example-namespace')
        self.assertEqual(eitem['author_username'], 'animeshk08')
        self.assertIsNone(eitem['assignee_username'])
        self.assertEqual(eitem['closed_by_username'], 'animeshk08')
        self.assertEqual(eitem['title'], 'Sample title 4')
        self.assertEqual(eitem['status'], 'Closed')
        self.assertIsNone(eitem['milestone'])
        self.assertIsNone(eitem['close_status'])
        self.assertEqual(eitem['num_comments'], 2)
        self.assertListEqual(eitem['blocks'], [])
        self.assertListEqual(eitem['tags'], [])
        self.assertListEqual(eitem['custom_fields'], [])
        self.assertEqual(eitem['time_to_close_days'], 0.0)
        self.assertEqual(eitem['time_open_days'], 0.0)
        self.assertIsNone(eitem['time_to_first_attention'])

        item = self.items[4]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'issue')
        self.assertEqual(eitem['id'], 6)
        self.assertEqual(eitem['origin'], 'https://pagure.io/Test-group/Project-example-namespace')
        self.assertEqual(eitem['tag'], 'https://pagure.io/Test-group/Project-example-namespace')
        self.assertEqual(eitem['author_username'], 'animeshk08')
        self.assertEqual(eitem['assignee_username'], 'animeshk0806')
        self.assertEqual(eitem['closed_by_username'], 'animeshk08')
        self.assertEqual(eitem['title'], 'Sample issue 5')
        self.assertEqual(eitem['status'], 'Closed')
        self.assertEqual(eitem['milestone'], 'Milestone1')
        self.assertEqual(eitem['close_status'], 'Close status 1')
        self.assertEqual(eitem['num_comments'], 3)
        self.assertListEqual(eitem['blocks'], [])
        self.assertListEqual(eitem['tags'], ['Tag1'])
        self.assertListEqual(eitem['custom_fields'], [])
        self.assertEqual(eitem['time_to_close_days'], 0.0)
        self.assertEqual(eitem['time_open_days'], 0.0)
        self.assertIsNone(eitem['time_to_first_attention'])

    def test_enrich_repo_labels(self):
        """Test whether the field REPO_LABELS is present in the enriched items"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        for item in self.items:
            eitem = enrich_backend.get_rich_item(item)
            if eitem and 'id' in eitem:
                self.assertIn(REPO_LABELS, eitem)

    def test_raw_to_enrich_sorting_hat(self):
        """Test enrich with SortingHat"""

        result = self._test_raw_to_enrich(sortinghat=True)
        self.assertEqual(result['raw'], 7)
        self.assertEqual(result['enrich'], 26)

        enrich_backend = self.connectors[self.connector][2]()
        enrich_backend.sortinghat = True

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

    def test_perceval_params(self):
        """Test the extraction of perceval params from an URL"""

        url = "https://pagure.io/Test-group/Project-example-namespace"
        expected_params = [
            'Test-group', 'Project-example-namespace'
        ]
        self.assertListEqual(PagureOcean.get_perceval_params_from_url(url), expected_params)

        url = "https://pagure.io/Project-example"
        expected_params = [
            'Project-example'
        ]
        self.assertListEqual(PagureOcean.get_perceval_params_from_url(url), expected_params)

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        self.assertEqual(result['raw'], 7)
        self.assertEqual(result['enrich'], 26)

        res = requests.get(self.es_con + "/" + self.enrich_index + "/_search", verify=False)
        for eitem in res.json()['hits']['hits']:
            self.assertEqual(eitem['_source']['project'], "grimoire")

    def test_refresh_identities(self):
        """Test refresh identities"""

        result = self._test_refresh_identities()
        # ... ?

    def test_max_bulk_item_exceed(self):
        """Test bulk upload of documents when number of documents
         exceeds max bulk item limit"""
        pagure_enriched.MAX_SIZE_BULK_ENRICHED_ITEMS = 2
        result = self._test_raw_to_enrich(projects=True)
        self.assertEqual(result['raw'], 7)
        self.assertEqual(result['enrich'], 26)

    def test_copy_raw_fields(self):
        """Test copied raw fields"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        for item in self.items:
            eitem = enrich_backend.get_rich_item(item)
            if 'author_name' in eitem:
                for attribute in enrich_backend.RAW_FIELDS_COPY:
                    if attribute in item:
                        self.assertEqual(item[attribute], eitem[attribute])
                    else:
                        self.assertIsNone(eitem[attribute])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
