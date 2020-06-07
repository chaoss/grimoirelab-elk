# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2021 Bitergia
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
#   Venu Vardhan Reddy Tekula <venuvardhanreddytekula8@gmail.com>
#


import logging
import unittest
import unittest.mock

from base import TestBaseBackend
import grimoire_elk.enriched.gitlabcomments as gitlabcomments_enriched
from grimoire_elk.enriched.utils import REPO_LABELS
from grimoire_elk.raw.gitlab import GitLabOcean


class TestGitLabComments(TestBaseBackend):
    """Test GitLabComments backend"""

    connector = "gitlabcomments"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_has_identities(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()

        self.assertGreater(result['items'], 0)
        self.assertGreater(result['raw'], 0)
        self.assertEqual(result['items'], result['raw'])

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()

        self.assertEqual(result['raw'], 5)
        self.assertEqual(result['enrich'], 36)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'issue')
        self.assertNotEqual(eitem['issue_state'], 'closed')
        self.assertEqual(eitem['author_name'], 'Vibhoothi')
        self.assertEqual(eitem['issue_labels'], ['UI', 'enhancement', 'feature'])
        self.assertEqual(eitem['reactions_total_count'], 1)
        self.assertEqual(eitem['reactions'][0]['type'], 'thumbsdown')
        self.assertEqual(eitem['reactions'][0]['count'], 1)
        self.assertEqual(eitem['issue_id_in_repo'], '25')
        self.assertEqual(eitem['issue_created_at'], '2020-04-07T11:31:36.167Z')
        self.assertEqual(eitem['time_to_first_attention'], 11.27)

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'merge_request')
        self.assertEqual(eitem['merge_state'], 'closed')
        self.assertEqual(eitem['author_name'], 'Athira')
        self.assertEqual(eitem['merge_labels'], [])
        self.assertEqual(eitem['reactions_total_count'], 0)
        self.assertEqual(eitem['reactions'], [])
        self.assertNotIn(eitem['time_to_merge_request_response'], eitem)
        self.assertEqual(eitem['time_to_close_days'], eitem['time_open_days'])
        self.assertEqual(eitem['merge_id_in_repo'], '41')
        self.assertEqual(eitem['merge_created_at'], '2020-04-20T19:17:54.576Z')

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'merge_request')
        self.assertEqual(eitem['merge_state'], 'merged')
        self.assertEqual(eitem['reactions_total_count'], 1)
        self.assertEqual(eitem['reactions'][0]['type'], 'rocket')
        self.assertEqual(eitem['reactions'][0]['count'], 1)
        self.assertEqual(eitem['time_to_merge_request_response'], 0.0)

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'issue')
        self.assertEqual(eitem['issue_state'], 'closed')
        self.assertEqual(eitem['author_name'], None)
        self.assertIsNone(eitem['author_domain'])
        self.assertIsNone(eitem['assignee_domain'])
        self.assertEqual(eitem['reactions_total_count'], 0)
        self.assertEqual(eitem['reactions'], [])
        self.assertEqual(eitem['time_to_first_attention'], None)

        item = self.items[4]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'merge_request')
        self.assertEqual(eitem['merge_state'], 'merged')
        self.assertEqual(eitem['author_name'], None)
        self.assertIsNone(eitem['author_domain'])
        self.assertIsNone(eitem['merge_author_domain'])
        self.assertEqual(eitem['num_versions'], 1)
        self.assertEqual(eitem['num_merge_comments'], 3)
        self.assertEqual(eitem['time_to_merge_request_response'], None)

    def test_enrich_repo_labels(self):
        """Test whether the field REPO_LABELS is present in the enriched items"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        for item in self.items:
            eitem = enrich_backend.get_rich_item(item)
            self.assertIn(REPO_LABELS, eitem)

    def test_raw_to_enrich_sorting_hat(self):
        """Test enrich with SortingHat"""

        result = self._test_raw_to_enrich(sortinghat=True)

        self.assertEqual(result['raw'], 5)
        self.assertEqual(result['enrich'], 36)

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

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        # ... ?

    def test_refresh_identities(self):
        """Test refresh identities"""

        result = self._test_refresh_identities()
        # ... ?

    def test_refresh_project(self):
        """Test refresh project field for all sources"""

        result = self._test_refresh_project()
        # ... ?

    def test_perceval_params(self):
        """Test the extraction of perceval params from an URL"""

        url = "https://gitlab.com/amfoss/cms-mobile"
        expected_params = [
            'amfoss', 'cms-mobile'
        ]
        self.assertListEqual(GitLabOcean.get_perceval_params_from_url(url), expected_params)

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

    def test_max_bulk_item_exceed(self):
        """Test bulk upload of documents when number of documents
         exceeds max bulk item limit"""

        gitlabcomments_enriched.MAX_SIZE_BULK_ENRICHED_ITEMS = 2
        result = self._test_raw_to_enrich(projects=True)

        self.assertEqual(result['raw'], 5)
        self.assertEqual(result['enrich'], 36)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
