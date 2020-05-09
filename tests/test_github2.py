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
import time
import unittest
import unittest.mock
from unittest.mock import MagicMock

from base import TestBaseBackend
from grimoire_elk.enriched.enrich import logger
from grimoire_elk.enriched.utils import REPO_LABELS, anonymize_url
from grimoire_elk.raw.github import GitHubOcean


class TestGitHub2(TestBaseBackend):
    """Test GitHub2 backend"""

    connector = "github2"
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

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()

        self.assertEqual(result['raw'], 6)
        self.assertEqual(result['enrich'], 11)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['issue_labels'], [])
        self.assertEqual(eitem['reaction_thumb_up'], 0)
        self.assertEqual(eitem['reaction_thumb_down'], 0)
        self.assertEqual(eitem['reaction_confused'], 0)
        self.assertEqual(eitem['reaction_laugh'], 0)
        self.assertEqual(eitem['reaction_total_count'], 0)
        self.assertEqual(item['category'], 'issue')

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['pull_labels'], ['bug', 'feature'])
        self.assertEqual(item['category'], 'pull_request')
        self.assertNotIn('reaction_thumb_up', eitem)
        self.assertNotIn('reaction_thumb_down', eitem)
        self.assertNotIn('reaction_confused', eitem)
        self.assertNotIn('reaction_laugh', eitem)
        self.assertNotIn('reaction_total_count', eitem)
        self.assertEqual(eitem['time_to_merge_request_response'], 335.81)

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
        self.assertIsNone(eitem['assignee_name'])
        self.assertIsNone(eitem['assignee_domain'])
        self.assertIsNone(eitem['assignee_org'])
        self.assertEqual(eitem['reaction_thumb_up'], 0)
        self.assertEqual(eitem['reaction_thumb_down'], 0)
        self.assertEqual(eitem['reaction_confused'], 0)
        self.assertEqual(eitem['reaction_laugh'], 0)
        self.assertEqual(eitem['reaction_total_count'], 0)

        item = self.items[6]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'pull_request')
        self.assertEqual(eitem['user_name'], 'acs')
        self.assertIsNone(eitem['user_domain'])
        self.assertIsNone(eitem['user_org'])
        self.assertEqual(eitem['author_name'], 'acs')
        self.assertIsNone(eitem['merge_author_name'])
        self.assertIsNone(eitem['merge_author_domain'])
        self.assertIsNone(eitem['merge_author_org'])
        self.assertNotIn('reaction_thumb_up', eitem)
        self.assertNotIn('reaction_thumb_down', eitem)
        self.assertNotIn('reaction_confused', eitem)
        self.assertNotIn('reaction_laugh', eitem)
        self.assertNotIn('reaction_total_count', eitem)

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
        self.assertGreater(result['enrich'], result['raw'])

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
        self.assertListEqual(GitHubOcean.get_perceval_params_from_url(url), expected_params)

    def test_geolocation_study(self):
        """ Test that the geolocation study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_geolocation')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_geolocation":
                study(ocean_backend, enrich_backend,
                      location_field="user_location", geolocation_field="user_geolocation")

            self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.enrich:[github] Geolocation '
                                           'starting study %s/test_github2_enrich'
                             % anonymize_url(self.es_con))
            self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.enrich:[github] Geolocation '
                                            'end %s/test_github2_enrich'
                             % anonymize_url(self.es_con))

        time.sleep(5)  # HACK: Wait until github enrich index has been written
        items = [item for item in enrich_backend.fetch() if 'user_location' in item]
        self.assertEqual(len(items), 3)
        for item in items:
            self.assertIn('user_geolocation', item)

    def test_feelings_study(self):
        """ Test that feelings study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_feelings')

        def mocked_feelings(found, nlp_rest_url):
            return '__label__positive', '__label__love'

        enrich_backend.get_feelings = MagicMock(side_effect=mocked_feelings)

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_feelings":
                study(ocean_backend, enrich_backend, attributes=["body"], nlp_rest_url='http://localhost:2901')

            self.assertRegex(cm.output[0], 'INFO:grimoire_elk.enriched.enrich:\\[enrich-feelings\\] Start study.*')
            self.assertRegex(cm.output[-1], 'INFO:grimoire_elk.enriched.enrich:\\[enrich-feelings\\] End study.*')

        time.sleep(5)  # HACK: Wait until github enrich index has been written
        items = [item for item in enrich_backend.fetch() if 'body' in item]
        self.assertEqual(len(items), 5)
        for item in items:
            self.assertEqual(item['has_sentiment'], 1)
            self.assertEqual(item['has_emotion'], 1)
            self.assertEqual(item['feeling_emotion'], '__label__love')
            self.assertEqual(item['feeling_sentiment'], '__label__positive')

    def test_feelings_study_unknown(self):
        """ Test that feelings study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_feelings')

        def mocked_feelings(found, nlp_rest_url):
            return '__label__unknown', '__label__unknown'

        enrich_backend.get_feelings = MagicMock(side_effect=mocked_feelings)

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_feelings":
                study(ocean_backend, enrich_backend, attributes=["body"], nlp_rest_url='http://localhost:2901')

            self.assertRegex(cm.output[0], 'INFO:grimoire_elk.enriched.enrich:\\[enrich-feelings\\] Start study.*')
            self.assertRegex(cm.output[-1], 'INFO:grimoire_elk.enriched.enrich:\\[enrich-feelings\\] End study.*')

        time.sleep(5)  # HACK: Wait until github enrich index has been written
        items = [item for item in enrich_backend.fetch() if 'body' in item]
        self.assertEqual(len(items), 5)
        for item in items:
            self.assertEqual(item['has_sentiment'], 1)
            self.assertEqual(item['has_emotion'], 1)
            self.assertEqual(item['feeling_emotion'], '__label__unknown')
            self.assertEqual(item['feeling_sentiment'], '__label__unknown')

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
