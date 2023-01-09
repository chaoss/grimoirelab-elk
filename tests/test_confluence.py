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
#
import logging
import json
import os
import unittest

from base import TestBaseBackend, data2es
from grimoire_elk.enriched.utils import anonymize_url, REPO_LABELS
from grimoire_elk.enriched.confluence import NO_ANCESTOR_TITLE
from grimoire_elk.raw.confluence import ConfluenceOcean
from grimoire_elk.utils import get_elastic


class TestConfluence(TestBaseBackend):
    """Test Confluence backend"""

    connector = "confluence"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_has_identites(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()

        self.assertEqual(result['items'], 4)
        self.assertEqual(result['raw'], 4)

        # Check enriched data
        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertListEqual(eitem['ancestors_titles'],
                             ['Title 1', 'Title 2', 'Title 3'])
        self.assertListEqual(eitem['ancestors_links'],
                             ['/spaces/TEST/title1', '/spaces/TEST/title2', '/spaces/TEST/title3'])

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertListEqual(eitem['ancestors_titles'], [NO_ANCESTOR_TITLE])
        self.assertListEqual(eitem['ancestors_links'], ['/spaces/TEST/title1'])

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertListEqual(eitem['ancestors_titles'], ['Title 3'])
        self.assertListEqual(eitem['ancestors_links'], ['/spaces/TEST/title3'])

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertListEqual(eitem['ancestors_titles'],
                             ['Title 1', 'Title 2', 'Title 3'])
        self.assertListEqual(eitem['ancestors_links'],
                             ['/spaces/TEST/title1', '/spaces/TEST/title2', '/spaces/TEST/title3'])

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()
        self.assertEqual(result['raw'], 4)
        self.assertEqual(result['enrich'], 4)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        identity = enrich_backend.get_sh_identity(item, identity_field='by')
        self.assertEqual(identity['username'], 'jdoe')
        self.assertIsNone(identity['email'])
        self.assertEqual(identity['name'], 'John Doe')

        item = self.items[1]
        identity = enrich_backend.get_sh_identity(item, identity_field='by')
        self.assertEqual(identity['username'], 'jsmith')
        self.assertIsNone(identity['email'])
        self.assertEqual(identity['name'], 'John Smith')

        item = self.items[2]
        identity = enrich_backend.get_sh_identity(item, identity_field='by')
        self.assertEqual(identity['username'], 'anonymous')
        self.assertIsNone(identity['email'])
        self.assertEqual(identity['name'], 'Anonymous')

        item = self.items[3]
        identity = enrich_backend.get_sh_identity(item, identity_field='by')
        self.assertIsNone(identity['username'])
        self.assertIsNone(identity['email'])
        self.assertEqual(identity['name'], 'Anonymous')

    def test_enrich_repo_labels(self):
        """Test whether the field REPO_LABELS is present in the enriched items"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        for item in self.items:
            eitem = enrich_backend.get_rich_item(item)
            self.assertIn(REPO_LABELS, eitem)

    def test_perceval_params(self):
        """Test the extraction of perceval params from an URL"""

        url = "http://example.com --spaces=[TEST, HOME]"
        expected_params = ["http://example.com", "--spaces", "TEST", "HOME"]
        self.assertListEqual(ConfluenceOcean.get_perceval_params_from_url(url), expected_params)

    def test_raw_to_enrich_sorting_hat(self):
        """Test enrich with SortingHat"""

        result = self._test_raw_to_enrich(sortinghat=True)
        self.assertEqual(result['raw'], 4)
        self.assertEqual(result['enrich'], 4)

        enrich_backend = self.connectors[self.connector][2]()

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
        self.assertEqual(result['raw'], 4)
        self.assertEqual(result['enrich'], 4)

    def test_copy_raw_fields(self):
        """Test copied raw fields"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        for item in self.items:
            eitem = enrich_backend.get_rich_item(item)
            for attribute in enrich_backend.RAW_FIELDS_COPY:
                if attribute in item:
                    self.assertEqual(item[attribute], eitem[attribute])
                else:
                    self.assertIsNone(eitem[attribute])

    def test_refresh_identities(self):
        """Test refresh identities"""

        result = self._test_refresh_identities()
        # ... ?

    def test_raw_fix_item(self):
        """Test fix item to anonymize fields"""

        with open(os.path.join("data", self.connector + "_with_credentials.json")) as f:
            self.items = json.load(f)

        self.ocean_backend = self.connectors[self.connector][1](None)

        for item in self.items:
            # Check that the fields are not anonymized.
            self.assertNotEqual(item['origin'], anonymize_url(item['origin']))
            self.assertNotEqual(item['tag'], anonymize_url(item['tag']))
            self.assertNotEqual(item['data']['content_url'], anonymize_url(item['data']['content_url']))

            # Anonymize fields (origin, tag, data.content_url) using '_fix_item' method.
            self.ocean_backend._fix_item(item)

            # Check that the fields are anonymized.
            self.assertEqual(item['origin'], anonymize_url(item['origin']))
            self.assertEqual(item['tag'], anonymize_url(item['tag']))
            self.assertEqual(item['data']['content_url'], anonymize_url(item['data']['content_url']))

    def test_items_to_raw_anonymized_fields(self):
        """Test that the documents stored in the raw index the fields are anonymized"""

        with open(os.path.join("data", self.connector + "_with_credentials.json")) as f:
            self.items = json.load(f)

        self.ocean_backend = self.connectors[self.connector][1](None)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, True, self.ocean_backend, self.ocean_aliases)
        self.ocean_backend.set_elastic(elastic_ocean)

        # Anonymize the fields using 'anonymize_url' method.
        expected_anonymized_fields = []
        for item in self.items:
            new = {
                'origin': anonymize_url(item['origin']),
                'tag': anonymize_url(item['tag']),
                'content_url': anonymize_url(item['data']['content_url'])
            }
            expected_anonymized_fields.append(new)

        # Put items into the raw index.
        expected_items = len(self.items)
        raw_items = data2es(self.items, self.ocean_backend)
        self.assertEqual(raw_items, expected_items)

        # Fetch items from the raw index.
        raw_fields = []
        for item in self.ocean_backend.fetch():
            new = {
                'origin': item['origin'],
                'tag': item['tag'],
                'content_url': item['data']['content_url']
            }
            raw_fields.append(new)

        # Check that the fields are anonymized.
        self.assertListEqual(raw_fields, expected_anonymized_fields)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
