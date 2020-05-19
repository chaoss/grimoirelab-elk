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
#     Nishchith Shetty <inishchith@gmail.com>
#
import logging
import unittest

from base import TestBaseBackend
from grimoire_elk.raw.graal import GraalOcean
from grimoire_elk.enriched.cocom import logger


HEADER_JSON = {"Content-Type": "application/json"}


class TestCoCom(TestBaseBackend):
    """Test CoCom backend"""

    connector = "cocom"
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

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_items(item)[0]
        self.assertEqual(eitem['ccn'], 75)
        self.assertEqual(eitem['num_funs'], 31)
        self.assertEqual(eitem['tokens'], 2207)
        self.assertEqual(eitem['loc'], 372)
        self.assertEqual(eitem['ext'], "py")
        self.assertEqual(eitem['blanks'], 158)
        self.assertEqual(eitem['comments'], 193)
        self.assertEqual(eitem['file_path'], "graal/graal.py")
        self.assertEqual(eitem['modules'], ["graal"])
        self.assertEqual(eitem["comments_per_loc"], 0.52)
        self.assertEqual(eitem["blanks_per_loc"], 0.42)
        self.assertEqual(eitem["loc_per_function"], 12.0)
        self.assertIn('commit_parents', eitem)
        self.assertIn('files_at_commit', eitem)

        item = self.items[1]
        eitem = enrich_backend.get_rich_items(item)[0]
        self.assertEqual(eitem['ccn'], 70)
        self.assertEqual(eitem['num_funs'], 52)
        self.assertEqual(eitem['tokens'], 4623)
        self.assertEqual(eitem['loc'], 527)
        self.assertEqual(eitem['ext'], "py")
        self.assertEqual(eitem['blanks'], 204)
        self.assertEqual(eitem['comments'], 77)
        self.assertEqual(eitem['file_path'], "tests/test_graal.py")
        self.assertEqual(eitem['modules'], ["tests"])
        self.assertEqual(eitem["comments_per_loc"], 0.15)
        self.assertEqual(eitem["blanks_per_loc"], 0.39)
        self.assertEqual(eitem["loc_per_function"], 10.13)
        self.assertIn('commit_parents', eitem)
        self.assertIn('files_at_commit', eitem)

        item = self.items[2]
        eitem = enrich_backend.get_rich_items(item)[0]
        self.assertEqual(eitem['ccn'], 8)
        self.assertEqual(eitem['num_funs'], 3)
        self.assertEqual(eitem['tokens'], 421)
        self.assertEqual(eitem['loc'], 80)
        self.assertEqual(eitem['ext'], "py")
        self.assertEqual(eitem['blanks'], 26)
        self.assertEqual(eitem['comments'], 63)
        self.assertEqual(eitem['file_path'], "graal/backends/core/analyzers/lizard.py")
        self.assertEqual(eitem['modules'], ["graal", "graal/backends", "graal/backends/core", "graal/backends/core/analyzers"])
        self.assertEqual(eitem["comments_per_loc"], 0.79)
        self.assertEqual(eitem["blanks_per_loc"], 0.33)
        self.assertEqual(eitem["loc_per_function"], 26.67)
        self.assertIn('commit_parents', eitem)
        self.assertIn('files_at_commit', eitem)

        item = self.items[3]
        eitem = enrich_backend.get_rich_items(item)[0]
        self.assertEqual(eitem['ccn'], None)
        self.assertEqual(eitem['num_funs'], None)
        self.assertEqual(eitem['tokens'], None)
        self.assertEqual(eitem['loc'], None)
        self.assertEqual(eitem['ext'], None)
        self.assertEqual(eitem['blanks'], None)
        self.assertEqual(eitem['comments'], None)
        self.assertEqual(eitem['file_path'], "tests/data/analyzers/sample_code.py")
        self.assertEqual(eitem['modules'], ["tests", "tests/data", "tests/data/analyzers"])
        self.assertEqual(eitem["comments_per_loc"], None)
        self.assertEqual(eitem["blanks_per_loc"], None)
        self.assertEqual(eitem["loc_per_function"], None)
        self.assertIn('commit_parents', eitem)
        self.assertIn('files_at_commit', eitem)

    def test_cocom_analysis_study(self):
        """ Test that the cocom analysis study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_cocom_analysis')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_cocom_analysis":
                study(ocean_backend, enrich_backend)
                self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.cocom:[cocom] study enrich-cocom-analysis '
                                               'start')
                self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.cocom:[cocom] study enrich-cocom-analysis '
                                                'End')

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

    def test_copy_raw_fields(self):
        """Test copied raw fields"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        for item in self.items:
            eitem = enrich_backend.get_rich_items(item)[0]
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
