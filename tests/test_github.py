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

import requests

from base import TestBaseBackend
from grimoire_elk.enriched.enrich import logger
from grimoire_elk.enriched.github import logger as logger_github
from grimoire_elk.enriched.utils import REPO_LABELS, anonymize_url
from grimoire_elk.raw.github import GitHubOcean

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

        self.assertEqual(eitem['url'], 'https://github.com/chaoss/grimoirelab-perceval/pull/4')
        self.assertEqual(eitem['issue_url'], 'https://github.com/chaoss/grimoirelab-perceval/pull/4')

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
        study, ocean_backend, enrich_backend = self._test_study('enrich_onion')
        study(ocean_backend, enrich_backend, alias, in_index_iss='test_github_enrich', in_index_prs="test_github_enrich",
              out_index_iss='test_github_issues_onion', out_index_prs="test_github_prs_onion")

        url = self.es_con + "/_aliases"
        response = requests.get(url, verify=False).json()
        self.assertTrue('test_github_issues_onion' in response)
        self.assertTrue('test_github_prs_onion' in response)

        time.sleep(1)

        url = self.es_con + "/test_github_issues_onion/_search?size=20"
        response = requests.get(url, verify=False).json()
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 10)
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

        url = self.es_con + "/test_github_prs_onion/_search?size=20"
        response = requests.get(url, verify=False).json()
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 10)
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
