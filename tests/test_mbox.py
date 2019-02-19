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
#     Alvaro del Castillo <acs@bitergia.com>
#     Valerio Cosentino <valcos@bitergia.com>
#
import json
import logging
import unittest

from base import TestBaseBackend

from grimoire_elk.raw.mbox import MBoxOcean
from grimoire_elk.enriched.mbox import (logger,
                                        MBoxEnrich)


class TestMbox(TestBaseBackend):
    """Test Mbox backend"""

    connector = "mbox"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_has_identites(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()
        self.assertEqual(result['items'], 10)
        self.assertEqual(result['raw'], 10)

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()
        self.assertEqual(result['raw'], 9)
        self.assertEqual(result['enrich'], 9)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['mbox_author_domain'], 'domain.com')

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['mbox_author_domain'], 'domain.com')

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['mbox_author_domain'], 'hotmail.com')

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['mbox_author_domain'], 'gnome.org')

        item = self.items[4]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['mbox_author_domain'], 'wellsfargo.com')

        item = self.items[5]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['mbox_author_domain'], 'example.com')

        item = self.items[6]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['mbox_author_domain'], 'example.com')

        item = self.items[7]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['mbox_author_domain'], 'domain.com')

        item = self.items[8]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['mbox_author_domain'], 'example.org')

    def test_raw_to_enrich_sorting_hat(self):
        """Test enrich with SortingHat"""

        result = self._test_raw_to_enrich(sortinghat=True)
        self.assertEqual(result['raw'], 9)
        self.assertEqual(result['enrich'], 9)

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        self.assertEqual(result['raw'], 9)
        self.assertEqual(result['enrich'], 9)

    def test_refresh_identities(self):
        """Test refresh identities"""

        result = self._test_refresh_identities()
        # ... ?

    def test_refresh_project(self):
        """Test refresh project field for all sources"""

        result = self._test_refresh_project()
        # ... ?

    def test_empty_identity(self):
        """ Test support for from value with None"""
        enricher = MBoxEnrich()

        empty_identity = {f: None for f in ['email', 'name', 'username']}
        from_value = None

        item = {'data': {"author": None}}

        self.assertDictEqual(empty_identity, enricher.get_sh_identity(item, "author"))

    def test_kafka_kip_study(self):
        """ Test that the kafka kip study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('kafka_kip')
        with self.assertLogs(logger, level='INFO') as cm:
            study(ocean_backend, enrich_backend)
            self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.mbox:[Kafka KIP] Starting study')
            self.assertEqual(cm.output[1], 'INFO:grimoire_elk.enriched.mbox:[Kafka KIP] End')

    def test_arthur_params(self):
        """Test the extraction of arthur params from an URL"""

        with open("data/projects-release.json") as projects_filename:
            url = json.load(projects_filename)['grimoire']['mbox'][0]
            arthur_params = {'dirpath': '/home/bitergia/.perceval/mbox', 'uri': 'metrics-grimoire'}
            self.assertDictEqual(arthur_params, MBoxOcean.get_arthur_params_from_url(url))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
