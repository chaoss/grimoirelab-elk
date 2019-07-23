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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Nishchith Shetty <inishchith@gmail.com>
#
import logging
import unittest

from base import TestBaseBackend


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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
