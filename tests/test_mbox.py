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
#     Alvaro del Castillo <acs@bitergia.com>
#     Valerio Cosentino <valcos@bitergia.com>
#
import logging
import unittest
import time

from base import TestBaseBackend

from grimoire_elk.raw.mbox import MBoxOcean
from grimoire_elk.enriched.mbox import (logger,
                                        MBoxEnrich)
from grimoire_elk.enriched.utils import REPO_LABELS


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
        self.assertEqual(result['items'], 17)
        self.assertEqual(result['raw'], 17)

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()
        self.assertEqual(result['raw'], 16)
        self.assertEqual(result['enrich'], 16)

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
        self.assertEqual(result['raw'], 16)
        self.assertEqual(result['enrich'], 16)

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
        self.assertEqual(result['raw'], 16)
        self.assertEqual(result['enrich'], 16)

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

            if study.__name__ == "kafka_kip":
                study(ocean_backend, enrich_backend)

            self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.mbox:[mbox] study Kafka KIP starting')
            self.assertEqual(cm.output[1], 'INFO:grimoire_elk.enriched.mbox:[mbox] study Kafka KIP end')

            time.sleep(5)  # HACK: Wait until github enrich index has been written
            url = self.es_con + "/" + self.enrich_index + "/_search"
            response = enrich_backend.requests.get(url, verify=False).json()
            for hit in response['hits']['hits']:
                source = hit['_source']
                if 'kip' in source:
                    self.assertIn('kip_is_vote', source)
                    self.assertIn('kip_is_discuss', source)
                    self.assertIn('kip_vote', source)
                    self.assertIn('kip_binding', source)
                    self.assertIn('kip', source)
                    self.assertIn('kip_type', source)
                    self.assertIn('kip_status', source)
                    self.assertIn('kip_discuss_time_days', source)
                    self.assertIn('kip_discuss_inactive_days', source)
                    self.assertIn('kip_voting_time_days', source)
                    self.assertIn('kip_voting_inactive_days', source)
                    self.assertIn('kip_is_first_discuss', source)
                    self.assertIn('kip_is_first_vote', source)
                    self.assertIn('kip_is_last_discuss', source)
                    self.assertIn('kip_is_last_vote', source)
                    self.assertIn('kip_result', source)
                    self.assertIn('kip_start_end', source)
                    self.assertIn('kip_final_status', source)

    def test_perceval_params(self):
        """Test the extraction of perceval params from an URL"""

        url = "metrics-grimoire /home/bitergia/.perceval/mbox"
        expected_params = [
            'metrics-grimoire',
            '/home/bitergia/.perceval/mbox'
        ]
        self.assertListEqual(MBoxOcean.get_perceval_params_from_url(url), expected_params)

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
