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
#     Nitish Gupta <imnitish.ng@gmail.com>
#
import logging
import unittest

import requests
from base import TestBaseBackend
from grimoire_elk.enriched.utils import REPO_LABELS
from grimoire_elk.raw.gitter import GitterOcean


class TestGitter(TestBaseBackend):
    """Test Gitter backend"""

    connector = "gitter"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_has_identities(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()

        self.assertEqual(result['items'], 6)
        self.assertEqual(result['raw'], 6)

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()
        self.assertEqual(result['raw'], 6)
        self.assertEqual(result['enrich'], 6)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['id'], '1jf932nsdaf122h3292n402y')
        self.assertEqual(eitem['origin'], 'https://gitter.im/test_org/test_room')
        self.assertEqual(eitem['readBy'], 19)
        self.assertEqual(eitem['repository_labels'], None)
        self.assertEqual(eitem['tag'], 'https://gitter.im/test_org/test_room')
        self.assertIn('text_analyzed', eitem)
        self.assertEqual(eitem['unread'], 0)
        self.assertEqual(eitem['uuid'], '1a8b34f51ac1d831d095c6e39fae0a7e600cfcc7')

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['id'], '5e90c74af450c25cc8d05649')
        self.assertEqual(eitem['is_gitter_message'], 1)
        self.assertEqual(eitem['origin'], 'https://gitter.im/test_org/test_room')
        self.assertEqual(eitem['readBy'], 17)
        self.assertEqual(eitem['repository_labels'], None)
        self.assertEqual(eitem['tag'], 'https://gitter.im/test_org/test_room')
        self.assertIn('text_analyzed', eitem)
        self.assertIn('issues', eitem)
        self.assertEqual(eitem['issues'][0]['is_issue'], 'jenkinsci/docker #939')
        self.assertEqual(eitem['issues'][0]['url'], 'https://github.com/jenkinsci/docker/issues/939')
        self.assertEqual(eitem['issues'][0]['repo'], 'jenkinsci/docker')
        self.assertEqual(eitem['unread'], 0)
        self.assertEqual(eitem['uuid'], 'c42df902ac8277c70afb8296af6bd09b5ffa31d6')

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['id'], '5e879554f1207e2adc0cfe53')
        self.assertEqual(eitem['is_gitter_message'], 1)
        self.assertEqual(eitem['origin'], 'https://gitter.im/test_org/test_room')
        self.assertEqual(eitem['readBy'], 34)
        self.assertEqual(eitem['repository_labels'], None)
        self.assertEqual(eitem['tag'], 'https://gitter.im/test_org/test_room')
        self.assertIn('text_analyzed', eitem)
        self.assertIn('issues', eitem)
        self.assertEqual(eitem['issues'][0]['is_pull'], 'jenkinsci/jenkins #3861')
        self.assertEqual(eitem['issues'][0]['url'], 'https://github.com/jenkinsci/jenkins/pull/3861')
        self.assertEqual(eitem['issues'][0]['repo'], 'jenkinsci/jenkins')
        self.assertEqual(eitem['unread'], 0)
        self.assertEqual(eitem['uuid'], 'e8266108628134ae9ba2b1cf08ea3dffefebf8e3')

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['id'], '5e87070981a582042e974777')
        self.assertEqual(eitem['is_gitter_message'], 1)
        self.assertEqual(eitem['origin'], 'https://gitter.im/test_org/test_room')
        self.assertEqual(eitem['readBy'], 28)
        self.assertEqual(eitem['repository_labels'], None)
        self.assertEqual(eitem['tag'], 'https://gitter.im/test_org/test_room')
        self.assertIn('text_analyzed', eitem)
        self.assertIn('mentioned', eitem)
        self.assertEqual(eitem['mentioned'][0]['mentioned_username'], 'IbrahimPatel89')
        self.assertEqual(eitem['mentioned'][0]['mentioned_userId'], '5e870448d73408ce4fdf18b6')
        self.assertEqual(eitem['unread'], 0)
        self.assertEqual(eitem['uuid'], '42ca765de9425d85ac2aebb6750701b3c5f85dcc')

        item = self.items[4]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['id'], '5e85efad1742080c5dfffab0')
        self.assertEqual(eitem['is_gitter_message'], 1)
        self.assertEqual(eitem['origin'], 'https://gitter.im/test_org/test_room')
        self.assertEqual(eitem['readBy'], 24)
        self.assertEqual(eitem['repository_labels'], None)
        self.assertEqual(eitem['tag'], 'https://gitter.im/test_org/test_room')
        self.assertIn('text_analyzed', eitem)
        self.assertIn('url_hostname', eitem)
        self.assertEqual(eitem['url_hostname'][0], 'https://github.com/')
        self.assertEqual(eitem['unread'], 1)
        self.assertEqual(eitem['uuid'], '2bdae2762b5a132669869048a0f07c0bef7f15cb')

        item = self.items[5]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['id'], '5e85efad1742650c5dfffab0')
        self.assertEqual(eitem['is_gitter_message'], 1)
        self.assertEqual(eitem['origin'], 'https://gitter.im/test_org/test_room')
        self.assertEqual(eitem['readBy'], 1)
        self.assertEqual(eitem['repository_labels'], None)
        self.assertEqual(eitem['tag'], 'https://gitter.im/test_org/test_room')
        self.assertIn('text_analyzed', eitem)
        self.assertIn('url_hostname', eitem)
        self.assertIn('mentioned', eitem)
        self.assertIn('issues', eitem)
        self.assertEqual(eitem['unread'], 1)
        self.assertEqual(eitem['uuid'], '2bdae2762b5a132669869048a0f07c0bef777667')
        self.assertEqual(eitem['url_hostname'][1], 'https://facebook.com/')
        self.assertEqual(eitem['issues'][1]['is_pull'], 'jenkinsci/jenkins #3863')
        self.assertEqual(eitem['mentioned'][1]['mentioned_username'], 'devanshu123')

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
        self.assertEqual(result['raw'], 6)
        self.assertEqual(result['enrich'], 6)

        enrich_backend = self.connectors[self.connector][2]()
        enrich_backend.sortinghat = True

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
                self.assertIn('author_name', source)
                self.assertIn('author_multi_org_names', source)
                self.assertIn('fromUser_gender', source)
                self.assertIn('fromUser_gender_acc', source)
                self.assertIn('fromUser_multi_org_names', source)
                self.assertIn('fromUser_name', source)
                self.assertIn('fromUser_user_name', source)
                self.assertIn('fromUser_uuid', source)

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        self.assertEqual(result['raw'], 6)
        self.assertEqual(result['enrich'], 6)

        res = requests.get(self.es_con + "/" + self.enrich_index + "/_search", verify=False)
        for eitem in res.json()['hits']['hits']:
            self.assertEqual(eitem['_source']['project'], "grimoire")

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

        url = "https://gitter.im/test_org/test_room"
        expected_params = [
            'test_org', 'test_room'
        ]
        self.assertListEqual(GitterOcean.get_perceval_params_from_url(url), expected_params)

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
