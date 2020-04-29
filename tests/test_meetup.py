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

from base import TestBaseBackend
from grimoire_elk.raw.meetup import MeetupOcean
from grimoire_elk.enriched.utils import REPO_LABELS


class TestMeetup(TestBaseBackend):
    """Test Meetup backend"""

    connector = "meetup"
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
        self.assertEqual(result['items'], 3)
        self.assertEqual(result['raw'], 3)

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()
        self.assertEqual(result['raw'], 3)
        self.assertEqual(result['enrich'], 19)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['meetup_created'], '2016-03-22T16:36:44+00:00')
        self.assertEqual(eitem['meetup_time'], '2016-04-07T16:30:00+00:00')
        self.assertEqual(eitem['meetup_updated'], '2016-04-07T21:39:24+00:00')
        self.assertEqual(eitem['group_created'], '2016-03-20T15:13:47+00:00')
        self.assertEqual(eitem['group_urlname'], 'sqlpass-es')
        self.assertEqual(eitem['author_uuid'], '029aa3befc96d386e1c7270586f1ec1d673b0b1b')
        self.assertIsNone(eitem['venue_geolocation'])

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['meetup_created'], '2016-05-31T17:30:48+00:00')
        self.assertEqual(eitem['meetup_time'], '2016-06-09T16:45:00+00:00')
        self.assertEqual(eitem['meetup_updated'], '2016-06-09T20:18:18+00:00')
        self.assertEqual(eitem['group_created'], '2016-03-20T15:13:47+00:00')
        self.assertEqual(eitem['group_urlname'], 'sqlpass-es')
        self.assertEqual(eitem['author_uuid'], '810d53ef4a9ae2ebd8064ac690b2e13cfc2df924')
        self.assertIsNotNone(eitem['venue_geolocation'])

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
        self.assertEqual(result['raw'], 3)
        self.assertEqual(result['enrich'], 19)

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
        self.assertEqual(result['raw'], 3)
        self.assertEqual(result['enrich'], 19)

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

        url = "South-East-Puppet-User-Group"
        expected_params = [
            "--tag",
            "South-East-Puppet-User-Group",
            "South-East-Puppet-User-Group"
        ]
        self.assertListEqual(MeetupOcean.get_perceval_params_from_url(url), expected_params)

    def test_items_to_raw_anonymized(self):
        """Test whether JSON items are properly inserted into ES anonymized"""

        result = self._test_items_to_raw_anonymized()

        self.assertGreater(result['items'], 0)
        self.assertGreater(result['raw'], 0)
        self.assertEqual(result['items'], result['raw'])

        item = self.items[0]['data']
        self.assertEqual(item['event_hosts'][0]['name'], '3b3e55fdc7886baea165a854d080caf9808cac97')
        self.assertEqual(item['rsvps'][0]['member']['name'], '3b3e55fdc7886baea165a854d080caf9808cac97')
        self.assertEqual(item['rsvps'][1]['member']['name'], '9b0740c20617be08bd6b81a02017e63235cc0204')
        self.assertEqual(item['rsvps'][2]['member']['name'], 'cbd5438b1e1084c1d85bec65a96ca566d9b2ef2e')

        item = self.items[1]['data']
        self.assertEqual(item['event_hosts'][0]['name'], 'aff2cc6caa4228a709ac3bba6b303c7e5dcce550')
        self.assertEqual(item['event_hosts'][1]['name'], '3b3e55fdc7886baea165a854d080caf9808cac97')
        self.assertEqual(item['rsvps'][0]['member']['name'], '3b3e55fdc7886baea165a854d080caf9808cac97')
        self.assertEqual(item['rsvps'][1]['member']['name'], '9b0740c20617be08bd6b81a02017e63235cc0204')
        self.assertEqual(item['rsvps'][2]['member']['name'], 'cbd5438b1e1084c1d85bec65a96ca566d9b2ef2e')
        self.assertEqual(item['comments'][0]['member']['name'], '58668e7669fd564d99db5d581fcdb6a5618440b5')
        self.assertEqual(item['comments'][1]['member']['name'], 'c96634ae1100ab91de991e40bb2fe656bd765de1')

    def test_raw_to_enrich_anonymized(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich_anonymized()

        self.assertEqual(result['raw'], 3)
        self.assertEqual(result['enrich'], 19)

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
