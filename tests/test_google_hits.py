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


class TestGoogleHits(TestBaseBackend):
    """Test GoogleHits backend"""

    connector = "google_hits"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_has_identites(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertFalse(enrich_backend.has_identities())

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

    def test_enrich_repo_labels(self):
        """Test whether the field REPO_LABELS is present in the enriched items"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        for item in self.items:
            eitem = enrich_backend.get_rich_item(item)
            self.assertIn(REPO_LABELS, eitem)

    def test_has_identities(self):
        """Test whether has_identities works"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertFalse(enrich_backend.has_identities())

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

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        # ... ?

    def test_refresh_project(self):
        """Test refresh project field for all sources"""

        result = self._test_refresh_project()
        # ... ?


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
