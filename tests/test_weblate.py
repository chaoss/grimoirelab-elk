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
#     Quan Zhou <quan@bitergia.com>
#


import logging
import time
import unittest

import requests

from base import TestBaseBackend
from grimoire_elk.enriched.enrich import logger, anonymize_url
from grimoire_elk.enriched.utils import REPO_LABELS


HEADER_JSON = {"Content-Type": "application/json"}


class TestWeblate(TestBaseBackend):
    """Test Weblate backend"""

    connector = "weblate"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_has_identites(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()
        self.assertEqual(result['items'], 4)
        self.assertEqual(result['raw'], 4)

        author_data = [
            {
                "email": "quan@bitergia.com",
                "full_name": "quan zhou",
                "username": "quan",
                "groups": [
                    "https://my.weblate.org/api/groups/2/",
                    "https://my.weblate.org/api/groups/3/"
                ],
                "is_superuser": False,
                "is_active": True,
                "date_joined": "2020-09-16T09:03:33.581180Z",
                "url": "https://my.weblate.org/api/users/1/"
            },
            {
                "email": "tom@jerry.com",
                "full_name": "tom jerry",
                "username": "tom",
                "groups": [
                    "https://my.weblate.org/api/groups/2/",
                    "https://my.weblate.org/api/groups/3/"
                ],
                "is_superuser": False,
                "is_active": True,
                "date_joined": "2020-09-17T09:03:33.581180Z",
                "url": "https://my.weblate.org/api/users/2/"
            },
            {
                "email": "tom@jerry.com",
                "full_name": "tom jerry",
                "username": "tom",
                "groups": [
                    "https://my.weblate.org/api/groups/2/",
                    "https://my.weblate.org/api/groups/3/"
                ],
                "is_superuser": False,
                "is_active": True,
                "date_joined": "2020-09-17T09:03:33.581180Z",
                "url": "https://my.weblate.org/api/users/2/"
            }
        ]
        for i, item in enumerate(self.items):
            self.ocean_backend._fix_item(item)

            if i == 3:
                self.assertNotIn('author_data', item['data'])
                self.assertIsNone(item['data']['author'])
            else:
                self.assertEqual(author_data[i], item['data']['author_data'])

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()
        self.assertEqual(result['raw'], 4)
        self.assertEqual(result['enrich'], 4)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_full_name'], 'quan zhou')
        self.assertEqual(eitem['change_id'], 1)
        self.assertEqual(eitem['change_api_url'], 'https://my.weblate.org/api/changes/1/')
        self.assertIsNone(eitem['component_api_url'])
        self.assertIsNone(eitem['translation_api_url'])
        self.assertEqual(eitem['unit_id'], 1)
        self.assertEqual(eitem['unit_context'], 'content.text')
        self.assertEqual(eitem['unit_api_url'], 'https://my.weblate.org/api/units/1')

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_full_name'], 'tom jerry')
        self.assertEqual(eitem['change_id'], 2)
        self.assertEqual(eitem['change_api_url'], 'https://my.weblate.org/api/changes/2/')
        self.assertIsNone(eitem['component_api_url'])
        self.assertIsNone(eitem['translation_api_url'])
        self.assertNotIn('unit_context', eitem)

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_full_name'], 'tom jerry')
        self.assertEqual(eitem['change_id'], 3)
        self.assertEqual(eitem['change_api_url'], 'https://my.weblate.org/api/changes/3/')
        self.assertEqual(eitem['component_api_url'], 'https://my.weblate.org/api/components/master/native/')
        self.assertEqual(eitem['component_name'], 'native')
        self.assertEqual(eitem['translation_api_url'], 'https://my.weblate.org/api/translations/master/native/es')
        self.assertEqual(eitem['translation_name'], 'es')
        self.assertEqual(eitem['project_name'], 'master')
        self.assertNotIn('unit_context', eitem)

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertNotIn('author_full_name', eitem)
        self.assertEqual(eitem['change_id'], 4)
        self.assertEqual(eitem['change_api_url'], 'https://my.weblate.org/api/changes/4/')
        self.assertEqual(eitem['component_api_url'], 'https://my.weblate.org/api/components/italian/nativo/')
        self.assertEqual(eitem['component_name'], 'nativo')
        self.assertEqual(eitem['translation_api_url'], 'https://my.weblate.org/api/translations/italian/nativo/it')
        self.assertEqual(eitem['translation_name'], 'it')
        self.assertEqual(eitem['project_name'], 'italian')
        self.assertNotIn('unit_context', eitem)

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
        self.assertEqual(result['raw'], 4)
        self.assertEqual(result['enrich'], 4)

        enrich_backend = self.connectors[self.connector][2]()
        enrich_backend.sortinghat = True

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_name'], 'quan zhou')
        self.assertEqual(eitem['author_user_name'], 'quan')
        self.assertEqual(eitem['author_org_name'], 'Unknown')
        self.assertEqual(eitem['author_multi_org_names'], ['Unknown'])

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_name'], 'tom jerry')
        self.assertEqual(eitem['author_user_name'], 'tom')
        self.assertEqual(eitem['author_org_name'], 'Unknown')
        self.assertEqual(eitem['author_multi_org_names'], ['Unknown'])

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_name'], 'tom jerry')
        self.assertEqual(eitem['author_user_name'], 'tom')
        self.assertEqual(eitem['author_org_name'], 'Unknown')
        self.assertEqual(eitem['author_multi_org_names'], ['Unknown'])

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertNotIn('author_name', eitem)
        self.assertNotIn('author_user_name', eitem)
        self.assertNotIn('author_org_name', eitem)
        self.assertNotIn('author_multi_org_names', eitem)

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        self.assertEqual(result['raw'], 4)
        self.assertEqual(result['enrich'], 4)

        res = requests.get(self.es_con + "/" + self.enrich_index + "/_search", verify=False)
        for eitem in res.json()['hits']['hits']:
            self.assertEqual(eitem['_source']['project'], "grimoire")

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

    def test_demography_study(self):
        """ Test that the demography study works correctly """

        alias = 'demographics'
        study, ocean_backend, enrich_backend = self._test_study('enrich_demography')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_demography":
                study(ocean_backend, enrich_backend, alias)

            self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.enrich:[weblate] Demography '
                                           'starting study {}/test_weblate_enrich'.format(anonymize_url(self.es_con)))
            self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.enrich:[weblate] Demography '
                                            'end {}/test_weblate_enrich'.format(anonymize_url(self.es_con)))

        time.sleep(5)  # HACK: Wait until git enrich index has been written
        items = [item for item in enrich_backend.fetch()]
        self.assertEqual(len(items), 4)
        for item in items:
            if 'author_name' in item:
                self.assertTrue('demography_min_date' in item.keys())
                self.assertTrue('demography_max_date' in item.keys())
            else:
                self.assertFalse('demography_min_date' in item.keys())
                self.assertFalse('demography_max_date' in item.keys())

        r = enrich_backend.elastic.requests.get(enrich_backend.elastic.index_url + "/_alias",
                                                headers=HEADER_JSON, verify=False)
        self.assertIn(alias, r.json()[enrich_backend.elastic.index]['aliases'])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
