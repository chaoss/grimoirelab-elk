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
#     Valerio Cosentino <valcos@bitergia.com>
#
import logging
import unittest
import time

import requests

from base import TestBaseBackend
from grimoire_elk.enriched.gitlab import NO_MILESTONE_TAG
from grimoire_elk.enriched.utils import REPO_LABELS
from grimoire_elk.raw.gitlab import GitLabOcean


class TestGitLab(TestBaseBackend):
    """Test GitLab backend"""

    connector = "gitlab"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"
    ocean_index_anonymized = "test_" + connector + "_anonymized"
    enrich_index_anonymized = "test_" + connector + "_enrich_anonymized"

    def test_has_identites(self):
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

        self.assertGreater(result['raw'], 0)
        self.assertGreater(result['enrich'], 0)
        self.assertEqual(result['raw'], result['enrich'])

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], "8.17")
        self.assertEqual(eitem['milestone_start_date'], "2017-01-07T00:00:00")
        self.assertEqual(eitem['milestone_due_date'], "2017-02-21T00:00:00")
        self.assertEqual(eitem['milestone_url'], "https://gitlab.com/gitlab-org/gitlab-ce/milestones/34")
        self.assertEqual(eitem['milestone_id'], 134231)
        self.assertEqual(eitem['milestone_iid'], 34)
        self.assertEqual(eitem['labels'], [])
        self.assertEqual(eitem['author_username'], "redfish64")
        self.assertEqual(eitem['author_uuid'], '3a9ab60b1586bf0a292ccb447746f7bcd1ab14e4')
        self.assertEqual(eitem['assignee_username'], 'redfish64')
        self.assertEqual(eitem['assignee_uuid'], '3a9ab60b1586bf0a292ccb447746f7bcd1ab14e4')
        self.assertEqual(eitem['repository'], "https://gitlab.com/fdroid/fdroiddata")

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], NO_MILESTONE_TAG)
        self.assertEqual(eitem['milestone_start_date'], None)
        self.assertEqual(eitem['milestone_due_date'], None)
        self.assertEqual(eitem['milestone_url'], None)
        self.assertEqual(eitem['milestone_id'], None)
        self.assertEqual(eitem['milestone_iid'], None)
        self.assertEqual(eitem['labels'], [])
        self.assertEqual(eitem['author_username'], "redfish64")
        self.assertEqual(eitem['author_uuid'], '3a9ab60b1586bf0a292ccb447746f7bcd1ab14e4')
        self.assertEqual(eitem['repository'], "https://gitlab.com/fdroid/fdroiddata")

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], NO_MILESTONE_TAG)
        self.assertEqual(eitem['milestone_start_date'], None)
        self.assertEqual(eitem['milestone_due_date'], None)
        self.assertEqual(eitem['milestone_url'], None)
        self.assertEqual(eitem['milestone_id'], None)
        self.assertEqual(eitem['milestone_iid'], None)
        self.assertEqual(eitem['labels'], ['CI/CD', 'Deliverable'])
        self.assertEqual(eitem['author_username'], "YoeriNijs")
        self.assertEqual(eitem['author_uuid'], '02a45079282ec8c5ee5e3d04cc5ef46441d45af8')
        self.assertEqual(eitem['repository'], "https://gitlab.com/fdroid/fdroiddata")

        item = self.items[4]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], NO_MILESTONE_TAG)
        self.assertEqual(eitem['milestone_start_date'], None)
        self.assertEqual(eitem['milestone_due_date'], None)
        self.assertEqual(eitem['milestone_url'], None)
        self.assertEqual(eitem['milestone_id'], None)
        self.assertEqual(eitem['milestone_iid'], None)
        self.assertEqual(eitem['labels'], [])
        self.assertEqual(eitem['author_username'], None)
        self.assertEqual(eitem['repository'], "https://gitlab.com/gitlab-org/gitlab")

        item = self.items[5]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], "8.17")
        self.assertEqual(eitem['milestone_start_date'], "2017-01-07T00:00:00")
        self.assertEqual(eitem['milestone_due_date'], "2017-02-21T00:00:00")
        self.assertEqual(eitem['milestone_url'], "https://gitlab.com/gitlab-org/gitlab-ce/milestones/34")
        self.assertEqual(eitem['milestone_id'], 134231)
        self.assertEqual(eitem['milestone_iid'], 34)
        self.assertEqual(eitem['labels'], [])
        self.assertEqual(eitem['author_username'], "grote")
        self.assertEqual(eitem['author_uuid'], 'c6f116d5e92c5ec1393542f63790c3c904ddba28')
        self.assertEqual(eitem['repository'], "https://gitlab.com/fdroid/fdroiddata")

        item = self.items[6]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], NO_MILESTONE_TAG)
        self.assertEqual(eitem['milestone_start_date'], None)
        self.assertEqual(eitem['milestone_due_date'], None)
        self.assertEqual(eitem['milestone_url'], None)
        self.assertEqual(eitem['milestone_id'], None)
        self.assertEqual(eitem['milestone_iid'], None)
        self.assertEqual(eitem['labels'], [])
        self.assertEqual(eitem['author_username'], "Rudloff")
        self.assertEqual(eitem['author_uuid'], '56958a849936fca7a0e4361a6a08d99795770b2f')
        self.assertEqual(eitem['repository'], "https://gitlab.com/fdroid/fdroiddata")

        item = self.items[7]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], NO_MILESTONE_TAG)
        self.assertEqual(eitem['milestone_start_date'], None)
        self.assertEqual(eitem['milestone_due_date'], None)
        self.assertEqual(eitem['milestone_url'], None)
        self.assertEqual(eitem['milestone_id'], None)
        self.assertEqual(eitem['milestone_iid'], None)
        self.assertEqual(eitem['labels'], ['CI/CD', 'Deliverable'])
        self.assertEqual(eitem['author_username'], "marc.nause")
        self.assertEqual(eitem['author_uuid'], 'b7bddb53fc3b7fbba0188015aa74bfc501a7f230')
        self.assertEqual(eitem['repository'], "https://gitlab.com/fdroid/fdroiddata")

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
        # ... ?

    def test_refresh_identities(self):
        """Test refresh identities"""

        result = self._test_refresh_identities()
        # ... ?

    def test_items_to_raw_anonymized(self):
        """Test whether JSON items are properly inserted into ES anonymized"""

        result = self._test_items_to_raw_anonymized()

        self.assertGreater(result['items'], 0)
        self.assertGreater(result['raw'], 0)
        self.assertEqual(result['items'], result['raw'])

        item = self.items[0]['data']
        self.assertEqual(item['assignee']['username'], 'd04b492e0e54884033ddd4e32d4208a8340e4c85')
        self.assertEqual(item['assignee']['name'], '4e1e0c87bda6381324fb2b868859e1350c595c71')
        self.assertEqual(item['author']['username'], 'd04b492e0e54884033ddd4e32d4208a8340e4c85')
        self.assertEqual(item['author']['name'], '4e1e0c87bda6381324fb2b868859e1350c595c71')

        item = self.items[1]['data']
        self.assertIsNone(item['assignee'])
        self.assertEqual(item['author']['username'], 'd04b492e0e54884033ddd4e32d4208a8340e4c85')
        self.assertEqual(item['author']['name'], '4e1e0c87bda6381324fb2b868859e1350c595c71')

        item = self.items[4]['data']
        self.assertIsNone(item['assignee'])
        self.assertIsNone(item['author'])

        item = self.items[5]['data']
        self.assertEqual(item['merged_by']['username'], '5ef093e29634f927da333b7eaa479c248c93f26c')
        self.assertEqual(item['merged_by']['name'], '323866ff0cf9b5c7f817d1d444274582bef7d324')
        self.assertEqual(item['author']['username'], '499986947ba884c3c5946e15600a84f5fee8e9cb')
        self.assertEqual(item['author']['name'], 'ffcf43a6e72ca6d166ee38df916eee30d079d47c')

    def test_raw_to_enrich_anonymized(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich_anonymized()

        self.assertGreater(result['raw'], 0)
        self.assertGreater(result['enrich'], 0)
        self.assertEqual(result['raw'], result['enrich'])

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_uuid'], 'b9f586e623a9e6e41df48f8ddcf097c0ca333b52')
        self.assertEqual(eitem['author_name'], '4e1e0c87bda6381324fb2b868859e1350c595c71')
        self.assertEqual(eitem['author_username'], 'd04b492e0e54884033ddd4e32d4208a8340e4c85')
        self.assertEqual(eitem['assignee_name'], '4e1e0c87bda6381324fb2b868859e1350c595c71')
        self.assertEqual(eitem['assignee_username'], 'd04b492e0e54884033ddd4e32d4208a8340e4c85')

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_uuid'], 'b9f586e623a9e6e41df48f8ddcf097c0ca333b52')
        self.assertEqual(eitem['author_name'], '4e1e0c87bda6381324fb2b868859e1350c595c71')
        self.assertEqual(eitem['author_username'], 'd04b492e0e54884033ddd4e32d4208a8340e4c85')
        self.assertEqual(eitem['assignee_name'], 'Unknown')
        self.assertEqual(eitem['assignee_username'], None)

        item = self.items[4]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_uuid'], '')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertIsNone(eitem['author_username'])
        self.assertEqual(eitem['assignee_name'], 'Unknown')
        self.assertIsNone(eitem['assignee_username'])

        item = self.items[5]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_uuid'], '5a7534f25eaaeacf2d534172b6ed3f9effef94dd')
        self.assertEqual(eitem['author_name'], 'ffcf43a6e72ca6d166ee38df916eee30d079d47c')
        self.assertEqual(eitem['author_username'], '499986947ba884c3c5946e15600a84f5fee8e9cb')
        self.assertEqual(eitem['merge_author_name'], '323866ff0cf9b5c7f817d1d444274582bef7d324')
        self.assertEqual(eitem['merge_author_login'], '5ef093e29634f927da333b7eaa479c248c93f26c')

    def test_perceval_params(self):
        """Test the extraction of perceval params from an URL"""

        url = "https://gitlab.com/fdroid/fdroiddata"
        expected_params = [
            'fdroid', 'fdroiddata'
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

    def test_onion_study(self):
        """ Test that the onion study works correctly """

        alias = "all_onion"
        study, ocean_backend, enrich_backend = self._test_study('enrich_onion')
        study(ocean_backend, enrich_backend, alias, in_index='test_gitlab_enrich', out_index='test_gitlab_onion',
              data_source='gitlab-issues')

        url = self.es_con + "/_aliases"
        response = requests.get(url, verify=False).json()
        self.assertTrue('test_gitlab_onion' in response)

        time.sleep(1)

        url = self.es_con + "/test_gitlab_onion/_search?size=50"
        response = requests.get(url, verify=False).json()
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 22)
        for hit in hits:
            source = hit['_source']
            self.assertIn('timeframe', source)
            self.assertIn('author_uuid', source)
            self.assertIn('author_name', source)
            self.assertIn('contributions', source)
            self.assertIn('metadata__timestamp', source)
            self.assertIn('project', source)
            self.assertIn('author_org_name', source)
            self.assertIn('cum_net_sum', source)
            self.assertIn('percent_cum_net_sum', source)
            self.assertIn('onion_role', source)
            self.assertIn('quarter', source)
            self.assertIn('metadata__enriched_on', source)
            self.assertIn('data_source', source)
            self.assertIn('grimoire_creation_date', source)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
