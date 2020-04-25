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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Valerio Cosentino <valcos@bitergia.com>
#
import logging
import unittest

from base import TestBaseBackend
from grimoire_elk.enriched.utils import REPO_LABELS
from grimoire_elk.raw.githubql import GitHubQLOcean


class TestGitHubQL(TestBaseBackend):
    """Test GitHubQL backend"""

    connector = "githubql"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()

        self.assertGreater(result['items'], 0)
        self.assertGreater(result['raw'], 0)
        self.assertEqual(result['items'], result['raw'])

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()

        self.assertGreater(result['raw'], 0)
        self.assertGreater(result['enrich'], 0)
        self.assertEqual(result['raw'], result['enrich'])

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'LabeledEvent')
        self.assertEqual(eitem['created_at'], '2020-04-07T11:21:12Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'Issue 2')
        self.assertEqual(eitem['label'], 'bug')
        self.assertEqual(eitem['label_description'], "Something isn't working")
        self.assertTrue(eitem['label_is_default'])
        self.assertEqual(eitem['label_created_at'], '2020-04-07T10:30:46Z')
        self.assertEqual(eitem['label_updated_at'], '2020-04-07T10:30:46Z')

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'UnlabeledEvent')
        self.assertEqual(eitem['created_at'], '2020-04-07T11:21:32Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'Issue 2')
        self.assertEqual(eitem['label'], 'bug')
        self.assertEqual(eitem['label_description'], "Something isn't working")
        self.assertTrue(eitem['label_is_default'])
        self.assertEqual(eitem['label_created_at'], '2020-04-07T10:30:46Z')
        self.assertEqual(eitem['label_updated_at'], '2020-04-07T10:30:46Z')

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'AddedToProjectEvent')
        self.assertEqual(eitem['created_at'], '2020-04-07T13:22:25Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'Issue 2')
        self.assertEqual(eitem['board_column'], 'backlog')
        self.assertEqual(eitem['board_name'], "my fantastic board")
        self.assertEqual(eitem['board_url'], 'https://github.com/valeriocos/test-issues-update/projects/1')
        self.assertEqual(eitem['board_created_at'], '2020-04-07T11:41:41Z')
        self.assertEqual(eitem['board_updated_at'], '2020-04-09T18:54:08Z')
        self.assertIsNone(eitem['board_closed_at'])
        self.assertEqual(eitem['board_state'], 'open')

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'MovedColumnsInProjectEvent')
        self.assertEqual(eitem['created_at'], '2020-04-07T13:22:51Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'Issue 2')
        self.assertEqual(eitem['board_column'], 'analysis')
        self.assertEqual(eitem['board_previous_column'], 'backlog')
        self.assertEqual(eitem['board_name'], "my fantastic board")
        self.assertEqual(eitem['board_url'], 'https://github.com/valeriocos/test-issues-update/projects/1')
        self.assertEqual(eitem['board_created_at'], '2020-04-07T11:41:41Z')
        self.assertEqual(eitem['board_updated_at'], '2020-04-09T18:54:08Z')
        self.assertIsNone(eitem['board_closed_at'])
        self.assertEqual(eitem['board_state'], 'open')

        item = self.items[4]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'RemovedFromProjectEvent')
        self.assertEqual(eitem['created_at'], '2020-04-09T18:54:08Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'Issue 2')
        self.assertEqual(eitem['board_column'], 'done')
        self.assertEqual(eitem['board_name'], "my fantastic board")
        self.assertEqual(eitem['board_url'], 'https://github.com/valeriocos/test-issues-update/projects/1')
        self.assertEqual(eitem['board_created_at'], '2020-04-07T11:41:41Z')
        self.assertEqual(eitem['board_updated_at'], '2020-04-09T18:54:08Z')
        self.assertIsNone(eitem['board_closed_at'])
        self.assertEqual(eitem['board_state'], 'open')

        item = self.items[5]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'CrossReferencedEvent')
        self.assertEqual(eitem['created_at'], '2020-04-07T10:36:16Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'First issue')
        self.assertFalse(eitem['reference_cross_repo'])
        self.assertFalse(eitem['reference_will_close_target'])
        self.assertEqual(eitem['reference_source_type'], "Issue")
        self.assertEqual(eitem['reference_source_number'], 2)
        self.assertEqual(eitem['reference_source_repo'], 'valeriocos/test-issues-update')
        self.assertEqual(eitem['reference_source_created_at'], '2020-04-07T10:35:56Z')
        self.assertEqual(eitem['reference_source_updated_at'], '2020-04-07T11:21:32Z')
        self.assertIsNone(eitem['reference_source_closed_at'])
        self.assertFalse(eitem['reference_source_closed'])
        self.assertIsNone(eitem['reference_source_merged'])

        item = self.items[6]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'ClosedEvent')
        self.assertEqual(eitem['created_at'], '2020-04-12T07:51:05Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'First issue')
        self.assertEqual(eitem['closer_type'], "PullRequest")
        self.assertEqual(eitem['closer_number'], 3)
        self.assertEqual(eitem['closer_repo'], "valeriocos/test-issues-update")
        self.assertEqual(eitem['closer_created_at'], '2020-04-12T07:50:25Z')
        self.assertEqual(eitem['closer_updated_at'], '2020-04-12T07:51:05Z')
        self.assertEqual(eitem['closer_closed_at'], '2020-04-12T07:51:05Z')
        self.assertTrue(eitem['closer_closed'])
        self.assertTrue(eitem['closer_merged'])

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
        self.assertGreater(result['raw'], 0)
        self.assertGreater(result['enrich'], 0)
        self.assertEqual(result['raw'], result['enrich'])

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
        self.assertGreater(result['raw'], 0)
        self.assertGreater(result['enrich'], 0)
        self.assertEqual(result['raw'], result['enrich'])

        enrich_backend = self.connectors[self.connector][2]()

        url = self.es_con + "/" + self.enrich_index + "/_search"
        response = enrich_backend.requests.get(url, verify=False).json()
        for hit in response['hits']['hits']:
            source = hit['_source']
            self.assertEqual(source['project'], 'Main')
            self.assertEqual(source['project_1'], 'Main')

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

        url = "https://github.com/chaoss/grimoirelab-perceval"
        expected_params = [
            'chaoss', 'grimoirelab-perceval'
        ]
        self.assertListEqual(GitHubQLOcean.get_perceval_params_from_url(url), expected_params)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
