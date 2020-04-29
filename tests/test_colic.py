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
from grimoire_elk.enriched.colic import logger


HEADER_JSON = {"Content-Type": "application/json"}


class TestCoLic(TestBaseBackend):
    """Test CoLic backend"""

    connector = "colic"
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
        self.assertEqual(eitem['licenses'], ["gpl-3.0"])
        self.assertEqual(eitem['has_license'], 1)
        self.assertEqual(eitem['license_name'], ["GNU General Public License 3.0"])
        self.assertEqual(eitem['copyrights'], ["Copyright (c) 2007 Free Software Foundation, Inc. <http://fsf.org/>"])
        self.assertEqual(eitem['has_copyright'], 1)
        self.assertEqual(eitem['modules'], [])
        self.assertEqual(eitem['file_path'], "LICENSE")

        item = self.items[1]
        eitem = enrich_backend.get_rich_items(item)[0]
        self.assertEqual(eitem['licenses'], ["gpl-3.0-plus"])
        self.assertEqual(eitem['has_license'], 1)
        self.assertEqual(eitem['license_name'], ["GNU General Public License 3.0 or later"])
        self.assertEqual(eitem['copyrights'], ["Copyright (c) 2015-2018 Bitergia"])
        self.assertEqual(eitem['has_copyright'], 1)
        self.assertEqual(eitem['modules'], ["graal"])
        self.assertEqual(eitem['file_path'], "graal/codecomplexity.py")

        item = self.items[2]
        eitem = enrich_backend.get_rich_items(item)[0]
        self.assertEqual(eitem['licenses'], ["gpl-3.0-plus"])
        self.assertEqual(eitem['has_license'], 1)
        self.assertEqual(eitem['license_name'], ["GNU General Public License 3.0 or later"])
        self.assertEqual(eitem['copyrights'], ["Copyright (c) 2015-2018 Bitergia"])
        self.assertEqual(eitem['has_copyright'], 1)
        self.assertEqual(eitem['modules'], ["graal"])
        self.assertEqual(eitem['file_path'], "graal/codecomplexity.py")

        item = self.items[3]
        eitem = enrich_backend.get_rich_items(item)[0]
        self.assertEqual(eitem['licenses'], [])
        self.assertEqual(eitem['has_license'], 0)
        self.assertEqual(eitem['license_name'], [])
        self.assertEqual(eitem['copyrights'], [])
        self.assertEqual(eitem['has_copyright'], 0)
        self.assertEqual(eitem['modules'], [])
        self.assertEqual(eitem['file_path'], "README.md")

        item = self.items[4]
        eitem = enrich_backend.get_rich_items(item)[0]
        self.assertEqual(eitem['licenses'], ["GPL-3.0"])
        self.assertEqual(eitem['has_license'], 1)
        self.assertEqual(eitem['license_name'], ["GPL-3.0"])
        self.assertEqual(eitem['copyrights'], [])
        self.assertEqual(eitem['has_copyright'], 0)
        self.assertEqual(eitem['modules'], ["tests"])
        self.assertEqual(eitem['file_path'], "tests/test_colic.py")

    def test_colic_analysis_study(self):
        """ Test that the colic analysis study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_colic_analysis')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_colic_analysis":
                study(ocean_backend, enrich_backend)
                self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.colic:[colic] study enrich-colic-analysis '
                                               'start')
                self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.colic:[colic] study enrich-colic-analysis '
                                                'end')

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
