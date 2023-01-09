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
import unittest

from base import TestBaseBackend
from grimoire_elk.enriched.utils import REPO_LABELS


class TestMozillaClub(TestBaseBackend):
    """Test MozillaClub backend"""

    connector = "mozillaclub"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_has_identites(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()
        self.assertEqual(result['items'], 92)
        self.assertEqual(result['raw'], 92)

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()
        self.assertEqual(result['raw'], 91)
        self.assertEqual(result['enrich'], 91)

        enrich_backend = self.connectors[self.connector][2]()

        for i in self.items:
            ei = enrich_backend.get_rich_item(i)
            self.assertIn('metadata__gelk_version', ei)
            self.assertIn('metadata__gelk_backend_name', ei)

            if 'Event Description' in ei:
                self.assertIn('Event Description Analyzed', ei)

            if 'Event Creations' in ei:
                self.assertIn('Event Creations Analyzed', ei)

            if 'Feedback from Attendees' in ei:
                self.assertIn('Feedback from Attendees Analyzed', ei)

            if 'Your Feedback' in ei:
                self.assertIn('Your Feedback Analyzed', ei)

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
        self.assertEqual(result['raw'], 91)
        self.assertEqual(result['enrich'], 91)

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
        self.assertEqual(result['raw'], 91)
        self.assertEqual(result['enrich'], 91)

        enrich_backend = self.connectors[self.connector][2](json_projects_map="data/projects-release.json",
                                                            db_user=self.db_user,
                                                            db_password=self.db_password)
        for i in self.items:
            ei = enrich_backend.get_rich_item(i)
            self.assertIn('project', ei)
            self.assertIn('project_1', ei)

    def test_refresh_identities(self):
        """Test refresh identities"""

        result = self._test_refresh_identities()
        # ... ?


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
