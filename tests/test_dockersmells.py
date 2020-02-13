# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2020 Bitergia
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
from grimoire_elk.raw.graal import GraalOcean


HEADER_JSON = {"Content-Type": "application/json"}


class TestCoSmellsDocker(TestBaseBackend):
    """Test CoSmellsDocker backend"""

    connector = "dockersmells"
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
        self.assertGreaterEqual(result['items'], result['raw'])

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()

        self.assertGreater(result['raw'], 0)
        self.assertGreater(result['enrich'], 0)
        self.assertGreaterEqual(result['enrich'], result['raw'])

        items = [item for item in self.enrich_backend.fetch()]

        item = items[0]
        self.assertEqual(item['file_path'], 'tests/Dockerfile')
        self.assertEqual(item['origin'], 'https://github.com/chaoss/grimoirelab')
        self.assertIn('smell', item)
        self.assertIn('smell_code', item)
        self.assertIn('smell_line', item)

        item = items[1]
        self.assertEqual(item['file_path'], 'tests/Dockerfile')
        self.assertEqual(item['origin'], 'https://github.com/chaoss/grimoirelab')
        self.assertIn('smell', item)
        self.assertIn('smell_code', item)
        self.assertIn('smell_line', item)

        item = items[2]
        self.assertEqual(item['file_path'], 'tests/Dockerfile')
        self.assertEqual(item['origin'], 'https://github.com/chaoss/grimoirelab')
        self.assertIn('smell', item)
        self.assertIn('smell_code', item)
        self.assertIn('smell_line', item)

        item = items[3]
        self.assertEqual(item['file_path'], 'tests/Dockerfile')
        self.assertEqual(item['origin'], 'https://github.com/chaoss/grimoirelab')
        self.assertIn('smell', item)
        self.assertIn('smell_code', item)
        self.assertIn('smell_line', item)

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        items = [item for item in self.enrich_backend.fetch()]

        item = items[0]
        self.assertEqual(item['file_path'], 'tests/Dockerfile')
        self.assertEqual(item['origin'], 'https://github.com/chaoss/grimoirelab')
        self.assertIn('smell', item)
        self.assertIn('smell_code', item)
        self.assertIn('smell_line', item)
        self.assertEqual(item['project'], 'Main')
        self.assertEqual(item['project_1'], 'Main')

        item = items[1]
        self.assertEqual(item['file_path'], 'tests/Dockerfile')
        self.assertEqual(item['origin'], 'https://github.com/chaoss/grimoirelab')
        self.assertIn('smell', item)
        self.assertIn('smell_code', item)
        self.assertIn('smell_line', item)
        self.assertEqual(item['project'], 'Main')
        self.assertEqual(item['project_1'], 'Main')

        item = items[2]
        self.assertEqual(item['file_path'], 'tests/Dockerfile')
        self.assertEqual(item['origin'], 'https://github.com/chaoss/grimoirelab')
        self.assertIn('smell', item)
        self.assertIn('smell_code', item)
        self.assertIn('smell_line', item)
        self.assertEqual(item['project'], 'Main')
        self.assertEqual(item['project_1'], 'Main')

        item = items[3]
        self.assertEqual(item['file_path'], 'tests/Dockerfile')
        self.assertEqual(item['origin'], 'https://github.com/chaoss/grimoirelab')
        self.assertIn('smell', item)
        self.assertIn('smell_code', item)
        self.assertIn('smell_line', item)
        self.assertEqual(item['project'], 'Main')
        self.assertEqual(item['project_1'], 'Main')

    def test_perceval_params(self):
        """Test the extraction of perceval params from an URL"""

        url = "https://github.com/grimoirelab/perceval"
        expected_params = [
            'https://github.com/grimoirelab/perceval'
        ]
        self.assertListEqual(GraalOcean.get_perceval_params_from_url(url), expected_params)

        url = "https://github.com/grimoirelab/perceval /tmp/perceval-repo"
        expected_params = [
            'https://github.com/grimoirelab/perceval'
        ]
        self.assertListEqual(GraalOcean.get_perceval_params_from_url(url), expected_params)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
