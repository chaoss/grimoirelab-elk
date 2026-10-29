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
#     Miguel Ángel Fernández <mafesan@bitergia.com>
#
import logging
import time
import unittest
from unittest.mock import MagicMock, patch

import requests

from base import TestBaseBackend
from grimoire_elk.enriched.enrich import logger
from grimoire_elk.enriched.github import GitHubEnrich, logger as logger_github
from grimoire_elk.enriched.utils import REPO_LABELS, anonymize_url
from grimoire_elk.raw.github import GitHubOcean
from grimoirelab_toolkit.datetime import datetime_utcnow

HEADER_JSON = {"Content-Type": "application/json"}


class TestGitHub(TestBaseBackend):
    """Test GitHub backend"""

    connector = "github"
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
        self.assertEqual(eitem['labels'], [])
        self.assertEqual(item['category'], 'issue')
        self.assertEqual(eitem['author_uuid'], '5f9d42ce000e46e9eee60a3c64a353b560051a2e')
        self.assertEqual(eitem['author_domain'], 'zhquan_example.com')
        self.assertEqual(eitem['user_data_uuid'], '5f9d42ce000e46e9eee60a3c64a353b560051a2e')
        self.assertEqual(eitem['user_data_domain'], 'zhquan_example.com')
        self.assertEqual(eitem['assignee_data_uuid'], '5f9d42ce000e46e9eee60a3c64a353b560051a2e')
        self.assertEqual(eitem['assignee_data_domain'], 'zhquan_example.com')

        self.assertEqual(eitem['url'], 'https://github.com/zhquan_example/repo/pull/1')
        self.assertEqual(eitem['issue_url'], 'https://github.com/zhquan_example/repo/pull/1')

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['labels'], ['bug', 'feature'])
        self.assertEqual(item['category'], 'pull_request')
        self.assertEqual(eitem['time_to_merge_request_response'], 335.81)
        self.assertEqual(eitem['author_uuid'], '5f9d42ce000e46e9eee60a3c64a353b560051a2e')
        self.assertEqual(eitem['author_domain'], 'zhquan_example.com')
        self.assertEqual(eitem['user_data_uuid'], '5f9d42ce000e46e9eee60a3c64a353b560051a2e')
        self.assertEqual(eitem['user_data_domain'], 'zhquan_example.com')
        self.assertEqual(eitem['merged_by_data_uuid'], '5f9d42ce000e46e9eee60a3c64a353b560051a2e')
        self.assertEqual(eitem['merged_by_data_domain'], 'zhquan_example.com')
        self.assertEqual(eitem['additions'], 528)
        self.assertEqual(eitem['deletions'], 0)
        self.assertEqual(eitem['changed_files'], 4)

        # item[1] has no reviews_data, so all approval fields must be zero/None
        self.assertEqual(eitem['num_approvals'], 0)
        self.assertIsNone(eitem['first_approver_login'])
        self.assertIsNone(eitem['first_approval_date'])
        self.assertIsNone(eitem['time_to_first_approval_days'])

        self.assertEqual(eitem['url'], 'https://github.com/zhquan_example/repo/pull/1')
        self.assertEqual(eitem['issue_url'], 'https://github.com/zhquan_example/repo/pull/1')

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'repository')
        self.assertEqual(eitem['forks_count'], 16687)
        self.assertEqual(eitem['subscribers_count'], 2904)
        self.assertEqual(eitem['stargazers_count'], 48188)
        self.assertEqual(eitem['url'], "https://github.com/kubernetes/kubernetes")

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'repository')
        self.assertEqual(eitem['forks_count'], 16687)
        self.assertEqual(eitem['subscribers_count'], 4301)
        self.assertEqual(eitem['stargazers_count'], 47118)
        self.assertEqual(eitem['url'], "https://github.com/kubernetes/kubernetes")

        item = self.items[4]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'repository')
        self.assertEqual(eitem['forks_count'], 1)
        self.assertEqual(eitem['subscribers_count'], 1)
        self.assertEqual(eitem['stargazers_count'], 1)
        self.assertEqual(eitem['url'], "https://github.com/kubernetes/kubernetes")

        item = self.items[5]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'issue')
        self.assertEqual(eitem['user_name'], 'acs')
        self.assertIsNone(eitem['user_domain'])
        self.assertIsNone(eitem['user_org'])
        self.assertEqual(eitem['author_name'], 'acs')
        self.assertEqual(eitem['author_uuid'], 'e8cc482634f2095c935b6a586ddb9ed8215d5cb8')
        self.assertIsNone(eitem['assignee_name'])
        self.assertIsNone(eitem['assignee_domain'])
        self.assertIsNone(eitem['assignee_org'])
        self.assertEqual(eitem['user_data_name'], 'acs')
        self.assertEqual(eitem['user_data_uuid'], 'e8cc482634f2095c935b6a586ddb9ed8215d5cb8')
        self.assertIsNone(eitem['user_data_domain'])

        self.assertEqual(eitem['url'], 'https://github.com/chaoss/grimoirelab-perceval/pull/7')
        self.assertEqual(eitem['issue_url'], 'https://github.com/chaoss/grimoirelab-perceval/pull/7')

        item = self.items[6]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'pull_request')
        self.assertEqual(eitem['user_name'], 'acs')
        self.assertIsNone(eitem['user_domain'])
        self.assertIsNone(eitem['user_org'])
        self.assertEqual(eitem['author_name'], 'acs')
        self.assertEqual(eitem['author_uuid'], 'e8cc482634f2095c935b6a586ddb9ed8215d5cb8')
        self.assertIsNone(eitem['merge_author_name'])
        self.assertIsNone(eitem['merge_author_domain'])
        self.assertIsNone(eitem['merge_author_org'])
        self.assertEqual(eitem['user_data_name'], 'acs')
        self.assertEqual(eitem['user_data_uuid'], 'e8cc482634f2095c935b6a586ddb9ed8215d5cb8')
        self.assertIsNone(eitem['user_data_domain'])
        self.assertEqual(eitem['additions'], 5)
        self.assertEqual(eitem['deletions'], 1)
        self.assertEqual(eitem['changed_files'], 1)

        self.assertEqual(eitem['num_approvals'], 0)
        self.assertIsNone(eitem['first_approver_login'])
        self.assertIsNone(eitem['first_approval_date'])
        self.assertIsNone(eitem['time_to_first_approval_days'])

        self.assertEqual(eitem['url'], 'https://github.com/chaoss/grimoirelab-perceval/pull/4')
        self.assertEqual(eitem['issue_url'], 'https://github.com/chaoss/grimoirelab-perceval/pull/4')

        # item[7]: PR with one APPROVED review — the happy path for approval metrics
        item = self.items[7]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'pull_request')
        self.assertEqual(eitem['num_approvals'], 1)
        self.assertEqual(eitem['first_approver_login'], 'rikoe')
        self.assertEqual(eitem['first_approval_date'], '2019-02-21T17:41:41Z')
        # PR created 2019-02-21T11:52:46Z, approved 2019-02-21T17:41:41Z → ~0.24 days
        self.assertAlmostEqual(eitem['time_to_first_approval_days'], 0.24, places=2)

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

    def test_perceval_params(self):
        """Test the extraction of perceval params from an URL"""

        url = "https://github.com/chaoss/grimoirelab-perceval"
        expected_params = [
            'chaoss', 'grimoirelab-perceval'
        ]
        self.assertListEqual(GitHubOcean.get_perceval_params_from_url(url), expected_params)

        url = "https://github.com/chaoss/grimoirelab-perceval/"
        expected_params = [
            'chaoss', 'grimoirelab-perceval'
        ]
        self.assertListEqual(GitHubOcean.get_perceval_params_from_url(url), expected_params)

    def test_demography_study(self):
        """ Test that the demography study works correctly """

        alias = "demographics"
        study, ocean_backend, enrich_backend = self._test_study('enrich_demography')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_demography":
                study(ocean_backend, enrich_backend, alias)

            self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.enrich:[github] Demography '
                                           'starting study %s/test_github_enrich'
                             % anonymize_url(self.es_con))
            self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.enrich:[github] Demography '
                                            'end %s/test_github_enrich'
                             % anonymize_url(self.es_con))

        time.sleep(5)  # HACK: Wait until github enrich index has been written
        items = [item for item in enrich_backend.fetch()]
        self.assertEqual(len(items), 7)
        for item in items:
            self.assertNotIn('username:password', item['origin'])
            self.assertNotIn('username:password', item['tag'])
            if 'author_uuid' in item:
                self.assertTrue('demography_min_date' in item.keys())
                self.assertTrue('demography_max_date' in item.keys())

        r = enrich_backend.elastic.requests.get(enrich_backend.elastic.index_url + "/_alias",
                                                headers=HEADER_JSON, verify=False)
        self.assertIn(alias, r.json()[enrich_backend.elastic.index]['aliases'])

    def test_geolocation_study(self):
        """ Test that the geolocation study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_geolocation')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_geolocation":
                study(ocean_backend, enrich_backend,
                      location_field="user_location", geolocation_field="user_geolocation")

            self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.enrich:[github] Geolocation '
                                           'starting study %s/test_github_enrich'
                             % anonymize_url(self.es_con))
            self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.enrich:[github] Geolocation '
                                            'end %s/test_github_enrich'
                             % anonymize_url(self.es_con))

        time.sleep(5)  # HACK: Wait until github enrich index has been written
        items = [item for item in enrich_backend.fetch() if 'user_location' in item]
        self.assertEqual(len(items), 4)
        for item in items:
            self.assertIn('user_geolocation', item)

    def test_enrich_backlog_analysis(self):
        """ Test that the backlog analysis works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_backlog_analysis')

        with self.assertLogs(logger_github, level='INFO') as cm:

            if study.__name__ == "enrich_backlog_analysis":
                study(ocean_backend, enrich_backend)

            self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.github:[github] '
                                           'Start enrich_backlog_analysis study')
            self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.github:[github] '
                                            'End enrich_backlog_analysis study')

        time.sleep(5)  # HACK: Wait until github enrich index has been written
        url = self.es_con + "/github_enrich_backlog/_search"
        response = enrich_backend.requests.get(url, verify=False).json()
        for hit in response['hits']['hits']:
            source = hit['_source']
            self.assertIn('uuid', source)
            self.assertIn('opened', source)
            self.assertIn('average_opened_time', source)
            self.assertIn('origin', source)
            self.assertIn('labels', source)
            self.assertIn('project', source)
            self.assertIn('interval_days', source)
            self.assertIn('study_creation_date', source)
            self.assertIn('metadata__enriched_on', source)
            self.assertIn('grimoire_creation_date', source)
            self.assertIn('is_github_stats', source)
            self.assertIn('organization', source)

    def test_items_to_raw_anonymized(self):
        """Test whether JSON items are properly inserted into ES anonymized"""

        result = self._test_items_to_raw_anonymized()

        self.assertGreater(result['items'], 0)
        self.assertGreater(result['raw'], 0)
        self.assertEqual(result['items'], result['raw'])

        item = self.items[0]['data']
        self.assertEqual(item['assignee']['login'], '176eee2bd2c010d02dd419c453aca854195de172')
        self.assertEqual(item['assignee_data']['login'], '176eee2bd2c010d02dd419c453aca854195de172')
        self.assertEqual(item['assignee_data']['name'], '176eee2bd2c010d02dd419c453aca854195de172')
        self.assertEqual(item['user']['login'], '176eee2bd2c010d02dd419c453aca854195de172')
        self.assertEqual(item['user_data']['login'], '176eee2bd2c010d02dd419c453aca854195de172')
        self.assertEqual(item['user_data']['name'], '176eee2bd2c010d02dd419c453aca854195de172')

        item = self.items[5]['data']
        self.assertEqual(item['comments_data'][0]['user']['login'], '2283e7d3eb1195c11d3ffafe7831f94a4b5952b2')
        self.assertEqual(item['comments_data'][0]['user_data']['login'], '2283e7d3eb1195c11d3ffafe7831f94a4b5952b2')
        self.assertEqual(item['comments_data'][0]['user_data']['name'], '2283e7d3eb1195c11d3ffafe7831f94a4b5952b2')
        self.assertEqual(item['comments_data'][1]['user']['login'], '257c699509389edc61f79193ca52f2b2368a126f')
        self.assertEqual(item['comments_data'][1]['user_data']['login'], '257c699509389edc61f79193ca52f2b2368a126f')
        self.assertEqual(item['comments_data'][1]['user_data']['name'], '257c699509389edc61f79193ca52f2b2368a126f')

        item = self.items[7]['data']
        self.assertEqual(item['merged_by']['login'], '29eb8410db7377c926a7b4b8006536049df4ead8')
        self.assertEqual(item['merged_by_data']['login'], '29eb8410db7377c926a7b4b8006536049df4ead8')
        self.assertEqual(item['merged_by_data']['name'], '29eb8410db7377c926a7b4b8006536049df4ead8')
        self.assertEqual(item['user']['login'], 'b45c6541a53918180164f1ff03c9995b69456b9b')
        self.assertEqual(item['user_data']['login'], 'b45c6541a53918180164f1ff03c9995b69456b9b')
        self.assertEqual(item['user_data']['name'], 'b45c6541a53918180164f1ff03c9995b69456b9b')

    def test_raw_to_enrich_anonymized(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich_anonymized()

        self.assertGreater(result['raw'], 0)
        self.assertGreater(result['enrich'], 0)
        self.assertEqual(result['raw'], result['enrich'])

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_uuid'], 'e4992881f28cf3318a566f0bd45dcf435216a82f')
        self.assertEqual(eitem['user_data_uuid'], 'e4992881f28cf3318a566f0bd45dcf435216a82f')
        self.assertEqual(eitem['user_data_name'], '176eee2bd2c010d02dd419c453aca854195de172')
        self.assertEqual(eitem['assignee_data_uuid'], 'e4992881f28cf3318a566f0bd45dcf435216a82f')
        self.assertEqual(eitem['assignee_data_name'], '176eee2bd2c010d02dd419c453aca854195de172')

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_uuid'], 'e4992881f28cf3318a566f0bd45dcf435216a82f')
        self.assertEqual(eitem['user_data_uuid'], 'e4992881f28cf3318a566f0bd45dcf435216a82f')
        self.assertEqual(eitem['user_data_name'], '176eee2bd2c010d02dd419c453aca854195de172')
        self.assertEqual(eitem['merged_by_data_uuid'], 'e4992881f28cf3318a566f0bd45dcf435216a82f')
        self.assertEqual(eitem['merged_by_data_name'], '176eee2bd2c010d02dd419c453aca854195de172')

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
        out_index_iss = "test_github_issues_onion"
        out_index_prs = "test_github_prs_onion"
        study, ocean_backend, enrich_backend = self._test_study('enrich_onion')
        study(ocean_backend, enrich_backend, alias, in_index_iss='test_github_enrich', in_index_prs="test_github_enrich",
              out_index_iss=out_index_iss, out_index_prs=out_index_prs)

        new_index_iss = out_index_iss + "_" + datetime_utcnow().strftime("%Y%m%d")
        new_index_prs = out_index_prs + "_" + datetime_utcnow().strftime("%Y%m%d")

        url = self.es_con + "/_aliases"
        response = requests.get(url, verify=False).json()
        self.assertTrue(new_index_iss in response)
        self.assertTrue(new_index_prs in response)

        time.sleep(1)

        url = self.es_con + "/" + new_index_iss + "/_search?size=20"
        response = requests.get(url, verify=False).json()
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 12)
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

        url = self.es_con + "/" + new_index_prs + "/_search?size=20"
        response = requests.get(url, verify=False).json()
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 12)
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


class TestGitHubPRApprovalMetrics(unittest.TestCase):
    """Isolated unit tests for the PR approval metrics block in __get_rich_pull.

    These tests do NOT require a live Elasticsearch / OpenSearch instance.
    They call get_rich_item() directly on a minimal synthetic item and inspect
    the four fields added for chaoss/grimoirelab-elk#1090:
      - num_approvals
      - first_approver_login
      - first_approval_date
      - time_to_first_approval_days
    """

    def _make_enrich_backend(self):
        """Return a GitHubEnrich instance with SortingHat and project-map disabled."""
        backend = GitHubEnrich()
        # Stub out SortingHat so get_item_sh returns an empty dict
        backend.sortinghat = False
        backend.prjs_map = None
        return backend

    def _make_pr_item(self, reviews_data, created_at='2023-01-01T00:00:00Z'):
        """Return a minimal raw Perceval pull_request item with the given reviews_data."""
        return {
            'uuid': 'test-uuid',
            'origin': 'https://github.com/test/repo',
            'tag': 'https://github.com/test/repo',
            'category': 'pull_request',
            'metadata__timestamp': '2023-01-01T00:00:00Z',
            'metadata__updated_on': '2023-01-01T00:00:00Z',
            'metadata__enriched_on': '2023-01-01T00:00:00Z',
            'data': {
                'number': 42,
                'id': 1001,
                'html_url': 'https://github.com/test/repo/pull/42',
                'title': 'Test PR',
                'state': 'closed',
                'created_at': created_at,
                'updated_at': '2023-01-02T00:00:00Z',
                'closed_at': '2023-01-02T00:00:00Z',
                'merged_at': '2023-01-02T00:00:00Z',
                'merged': True,
                'additions': 10,
                'deletions': 5,
                'changed_files': 2,
                'review_comments': 0,
                'labels': [],
                'user': {'login': 'author'},
                'user_data': None,
                'merged_by': None,
                'merged_by_data': None,
                'base': {'repo': {'forks_count': 0}},
                'review_comments_data': [],
                'comments_data': [],
                'reactions_data': [],
                'reviews_data': reviews_data,
            },
        }

    def _get_rich(self, reviews_data, created_at='2023-01-01T00:00:00Z'):
        """Helper: build a synthetic item, enrich it, and return the rich dict."""
        backend = self._make_enrich_backend()
        item = self._make_pr_item(reviews_data, created_at=created_at)
        # get_item_sh requires SortingHat; patch it to return empty
        with patch.object(backend, 'get_item_sh', return_value={}):
            return backend.get_rich_item(item)

    # ------------------------------------------------------------------
    # Test cases
    # ------------------------------------------------------------------

    def test_no_reviews_data_key(self):
        """PR with no reviews_data key at all → all approval fields are zero/None."""
        eitem = self._get_rich([])
        self.assertEqual(eitem['num_approvals'], 0)
        self.assertIsNone(eitem['first_approver_login'])
        self.assertIsNone(eitem['first_approval_date'])
        self.assertIsNone(eitem['time_to_first_approval_days'])

    def test_only_pending_reviews(self):
        """PENDING reviews (no submitted_at) are filtered out; fields remain None."""
        reviews = [
            {'state': 'PENDING', 'submitted_at': None,
             'user': {'login': 'reviewer1'}},
        ]
        eitem = self._get_rich(reviews)
        self.assertEqual(eitem['num_approvals'], 0)
        self.assertIsNone(eitem['first_approver_login'])
        self.assertIsNone(eitem['first_approval_date'])
        self.assertIsNone(eitem['time_to_first_approval_days'])

    def test_only_changes_requested_reviews(self):
        """CHANGES_REQUESTED reviews do not count as approvals."""
        reviews = [
            {'state': 'CHANGES_REQUESTED', 'submitted_at': '2023-01-01T06:00:00Z',
             'user': {'login': 'strict-reviewer'}},
        ]
        eitem = self._get_rich(reviews)
        self.assertEqual(eitem['num_approvals'], 0)
        self.assertIsNone(eitem['first_approver_login'])
        self.assertIsNone(eitem['first_approval_date'])
        self.assertIsNone(eitem['time_to_first_approval_days'])

    def test_only_commented_reviews(self):
        """COMMENTED reviews do not count as approvals; all four fields must be zero/None."""
        reviews = [
            {'state': 'COMMENTED', 'submitted_at': '2023-01-01T06:00:00Z',
             'user': {'login': 'drive-by'}},
        ]
        eitem = self._get_rich(reviews)
        self.assertEqual(eitem['num_approvals'], 0)
        self.assertIsNone(eitem['first_approver_login'])
        self.assertIsNone(eitem['first_approval_date'])
        self.assertIsNone(eitem['time_to_first_approval_days'])

    def test_single_approved_review(self):
        """Single APPROVED review → correct count, login, date, and a positive duration.

        get_time_diff_days computes (end - start) / 86400 and rounds to 2 decimal
        places (%.2f), so we assert with places=2 to stay within that precision.
        """
        # created_at = 2023-01-01T00:00:00Z, submitted_at = 2023-01-01T12:00:00Z → 0.50 days
        reviews = [
            {'state': 'APPROVED', 'submitted_at': '2023-01-01T12:00:00Z',
             'user': {'login': 'alice'}},
        ]
        eitem = self._get_rich(reviews, created_at='2023-01-01T00:00:00Z')
        self.assertEqual(eitem['num_approvals'], 1)
        self.assertEqual(eitem['first_approver_login'], 'alice')
        self.assertEqual(eitem['first_approval_date'], '2023-01-01T12:00:00Z')
        # Positive: approval happened AFTER creation
        self.assertGreater(eitem['time_to_first_approval_days'], 0)
        self.assertAlmostEqual(eitem['time_to_first_approval_days'], 0.5, places=2)

    def test_multiple_approvals_picks_earliest(self):
        """With multiple APPROVED reviews, the chronologically first one wins."""
        reviews = [
            # Intentionally out of order to exercise the sort
            {'state': 'APPROVED', 'submitted_at': '2023-01-03T00:00:00Z',
             'user': {'login': 'charlie'}},
            {'state': 'APPROVED', 'submitted_at': '2023-01-01T06:00:00Z',
             'user': {'login': 'alice'}},
            {'state': 'APPROVED', 'submitted_at': '2023-01-02T00:00:00Z',
             'user': {'login': 'bob'}},
        ]
        eitem = self._get_rich(reviews, created_at='2023-01-01T00:00:00Z')
        self.assertEqual(eitem['num_approvals'], 3)
        self.assertEqual(eitem['first_approver_login'], 'alice')   # earliest
        self.assertEqual(eitem['first_approval_date'], '2023-01-01T06:00:00Z')
        # 6 h = 0.25 days
        # 6 h / 24 = 0.25 exactly; %.2f rounding doesn't alter it
        self.assertAlmostEqual(eitem['time_to_first_approval_days'], 0.25, places=2)

    def test_mixed_states_only_approved_counted(self):
        """Mix of states: only APPROVED entries contribute to num_approvals."""
        reviews = [
            {'state': 'CHANGES_REQUESTED', 'submitted_at': '2023-01-01T01:00:00Z',
             'user': {'login': 'strict'}},
            {'state': 'APPROVED', 'submitted_at': '2023-01-01T12:00:00Z',
             'user': {'login': 'alice'}},
            {'state': 'PENDING', 'submitted_at': None,
             'user': {'login': 'slow'}},
            {'state': 'APPROVED', 'submitted_at': '2023-01-02T00:00:00Z',
             'user': {'login': 'bob'}},
        ]
        eitem = self._get_rich(reviews, created_at='2023-01-01T00:00:00Z')
        self.assertEqual(eitem['num_approvals'], 2)   # alice + bob
        self.assertEqual(eitem['first_approver_login'], 'alice')
        self.assertEqual(eitem['first_approval_date'], '2023-01-01T12:00:00Z')

    def test_approved_with_null_submitted_at_excluded(self):
        """Malformed APPROVED review with submitted_at=None must be excluded by the
        list-comprehension filter (line 541: `and r.get('submitted_at')`), so the
        sort lambda never receives None and no TypeError is raised.
        """
        reviews = [
            # Malformed: APPROVED but GitHub returned no timestamp
            {'state': 'APPROVED', 'submitted_at': None,
             'user': {'login': 'ghost'}},
            # Valid approval that should be the winner
            {'state': 'APPROVED', 'submitted_at': '2023-01-01T12:00:00Z',
             'user': {'login': 'alice'}},
        ]
        eitem = self._get_rich(reviews, created_at='2023-01-01T00:00:00Z')
        # Only the timestamped approval is counted
        self.assertEqual(eitem['num_approvals'], 1)
        self.assertEqual(eitem['first_approver_login'], 'alice')
        self.assertEqual(eitem['first_approval_date'], '2023-01-01T12:00:00Z')

    def test_sort_stability_at_second_boundaries(self):
        """ISO-8601 Z timestamps are fixed-width, so lexicographic == chronological.
        This test uses timestamps that differ only by seconds to confirm the sort
        picks the correct earliest entry and that no boundary error occurs.
        """
        reviews = [
            {'state': 'APPROVED', 'submitted_at': '2023-06-15T10:00:59Z',
             'user': {'login': 'late'}},
            {'state': 'APPROVED', 'submitted_at': '2023-06-15T10:00:01Z',
             'user': {'login': 'early'}},
            {'state': 'APPROVED', 'submitted_at': '2023-06-15T10:00:30Z',
             'user': {'login': 'middle'}},
        ]
        eitem = self._get_rich(reviews, created_at='2023-06-15T10:00:00Z')
        self.assertEqual(eitem['num_approvals'], 3)
        self.assertEqual(eitem['first_approver_login'], 'early')  # 10:00:01 is earliest
        self.assertEqual(eitem['first_approval_date'], '2023-06-15T10:00:01Z')
        # 1 second = 1/86400 days ≈ 0.0, rounds to 0.0 at 2 d.p.
        self.assertGreaterEqual(eitem['time_to_first_approval_days'], 0.0)

    def test_approved_review_with_no_user(self):
        """APPROVED review where user dict is absent → first_approver_login is None (no crash)."""
        reviews = [
            {'state': 'APPROVED', 'submitted_at': '2023-01-01T06:00:00Z',
             'user': None},
        ]
        eitem = self._get_rich(reviews)
        self.assertEqual(eitem['num_approvals'], 1)
        self.assertIsNone(eitem['first_approver_login'])
        self.assertEqual(eitem['first_approval_date'], '2023-01-01T06:00:00Z')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
