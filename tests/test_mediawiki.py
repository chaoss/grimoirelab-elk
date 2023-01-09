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
#     Alvaro del Castillo <acs@bitergia.com>
#     Valerio Cosentino <valcos@bitergia.com>
#
import logging
import unittest

from base import TestBaseBackend
from grimoire_elk.raw.mediawiki import MediaWikiOcean
from grimoire_elk.enriched.utils import REPO_LABELS


class TestMediawiki(TestBaseBackend):
    """Test Mediawiki backend"""

    connector = "mediawiki"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

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
        self.assertEqual(result['enrich'], 8)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]

        eitems = []
        for item in self.items:
            eitems.extend([ei for ei in enrich_backend.get_rich_item_reviews(item)])

        eitem = eitems[0]
        self.assertEqual(eitem['url'], 'https://wiki.mozilla.org/Main_Page/QA/NoMore404s')
        self.assertIn('metadata__gelk_version', eitem)
        self.assertIn('metadata__gelk_backend_name', eitem)
        self.assertIn('metadata__enriched_on', eitem)
        self.assertIn('grimoire_creation_date', eitem)
        self.assertIn('revision_comment', eitem)
        self.assertIn('revision_comment_analyzed', eitem)
        self.assertEqual(eitem['creation_date'], "2016-07-26T07:57:48Z")
        self.assertEqual(eitem['iscreated'], 1)
        self.assertEqual(eitem['isrevision'], 0)

        eitem = eitems[1]
        self.assertEqual(eitem['url'], 'https://wiki.mozilla.org/Main_Page/QA/NoMore404s')
        self.assertIn('metadata__gelk_version', eitem)
        self.assertIn('metadata__gelk_backend_name', eitem)
        self.assertIn('metadata__enriched_on', eitem)
        self.assertIn('grimoire_creation_date', eitem)
        self.assertIn('revision_comment', eitem)
        self.assertIn('revision_comment_analyzed', eitem)
        self.assertEqual(eitem['creation_date'], "2016-07-27T11:17:55Z")
        self.assertEqual(eitem['iscreated'], 0)
        self.assertEqual(eitem['isrevision'], 1)

        eitem = eitems[2]
        self.assertEqual(eitem['url'], 'https://wiki.mozilla.org/QA/Fennec/48/RC/smoketests-results')
        self.assertIn('metadata__gelk_version', eitem)
        self.assertIn('metadata__gelk_backend_name', eitem)
        self.assertIn('metadata__enriched_on', eitem)
        self.assertIn('grimoire_creation_date', eitem)
        self.assertIsNone(eitem['revision_comment'])
        self.assertEqual(eitem['creation_date'], "2016-07-27T10:02:21Z")
        self.assertEqual(eitem['iscreated'], 1)
        self.assertEqual(eitem['isrevision'], 0)

        eitem = eitems[3]
        self.assertEqual(eitem['url'], 'https://wiki.mozilla.org/QA/Fennec/48/RC/smoketests-results')
        self.assertIn('metadata__gelk_version', eitem)
        self.assertIn('metadata__gelk_backend_name', eitem)
        self.assertIn('metadata__enriched_on', eitem)
        self.assertIn('grimoire_creation_date', eitem)
        self.assertIn('revision_comment', eitem)
        self.assertIn('revision_comment_analyzed', eitem)
        self.assertEqual(eitem['creation_date'], "2016-07-27T11:14:17Z")
        self.assertEqual(eitem['iscreated'], 0)
        self.assertEqual(eitem['isrevision'], 1)

        eitem = eitems[4]
        self.assertEqual(eitem['url'],
                         'https://wiki-archive.opendaylight.org/Technical_Collaboration_Guideline/Translation')
        self.assertIn('metadata__gelk_version', eitem)
        self.assertIn('metadata__gelk_backend_name', eitem)
        self.assertIn('metadata__enriched_on', eitem)
        self.assertIn('grimoire_creation_date', eitem)
        self.assertIn('revision_comment', eitem)
        self.assertIn('revision_comment_analyzed', eitem)
        self.assertEqual(eitem['creation_date'], "2016-07-27T10:35:03Z")
        self.assertEqual(eitem['iscreated'], 1)
        self.assertEqual(eitem['isrevision'], 0)

        eitem = eitems[5]
        self.assertEqual(eitem['url'],
                         'https://wiki-archive.opendaylight.org/Technical_Collaboration_Guideline/Translation')
        self.assertIn('metadata__gelk_version', eitem)
        self.assertIn('metadata__gelk_backend_name', eitem)
        self.assertIn('metadata__enriched_on', eitem)
        self.assertIn('grimoire_creation_date', eitem)
        self.assertIn('revision_comment', eitem)
        self.assertIn('revision_comment_analyzed', eitem)
        self.assertEqual(eitem['creation_date'], "2016-07-27T10:43:41Z")
        self.assertEqual(eitem['iscreated'], 0)
        self.assertEqual(eitem['isrevision'], 1)

        eitem = eitems[6]
        self.assertEqual(eitem['url'],
                         'https://wiki-archive.opendaylight.org/Technical_Collaboration_Guideline/Translation')
        self.assertIn('metadata__gelk_version', eitem)
        self.assertIn('metadata__gelk_backend_name', eitem)
        self.assertIn('metadata__enriched_on', eitem)
        self.assertIn('grimoire_creation_date', eitem)
        self.assertIn('revision_comment', eitem)
        self.assertIn('revision_comment_analyzed', eitem)
        self.assertEqual(eitem['creation_date'], '2016-07-27T10:46:57Z')
        self.assertEqual(eitem['iscreated'], 0)
        self.assertEqual(eitem['isrevision'], 1)

        eitem = eitems[7]
        self.assertEqual(eitem['url'],
                         'https://wiki-archive.opendaylight.org/Technical_Collaboration_Guideline/Translation')
        self.assertIn('metadata__gelk_version', eitem)
        self.assertIn('metadata__gelk_backend_name', eitem)
        self.assertIn('metadata__enriched_on', eitem)
        self.assertIn('grimoire_creation_date', eitem)
        self.assertIn('revision_comment', eitem)
        self.assertIn('revision_comment_analyzed', eitem)
        self.assertEqual(eitem['creation_date'], "2016-06-21T17:16:29Z")
        self.assertEqual(eitem['iscreated'], 1)
        self.assertEqual(eitem['isrevision'], 0)

    def test_enrich_repo_labels(self):
        """Test whether the field REPO_LABELS is present in the enriched items"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitems = enrich_backend.get_rich_item_reviews(item)

        for ei in eitems:
            self.assertIn(REPO_LABELS, ei)

    def test_raw_to_enrich_sorting_hat(self):
        """Test enrich with SortingHat"""

        result = self._test_raw_to_enrich(sortinghat=True)
        self.assertEqual(result['raw'], 3)
        self.assertEqual(result['enrich'], 8)

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
        self.assertEqual(result['enrich'], 8)

        enrich_backend = self.connectors[self.connector][2](json_projects_map="data/projects-release.json",
                                                            db_user=self.db_user,
                                                            db_password=self.db_password)
        item = self.items[0]
        eitems = enrich_backend.get_rich_item_reviews(item)

        for ei in eitems:
            self.assertEqual(ei['url'], 'https://wiki.mozilla.org/view/Main_Page/QA/NoMore404s')
            self.assertIn('project', ei)
            self.assertIn('project_1', ei)

        # Test when only one URL is given in projects JSON file
        enrich_backend = self.connectors[self.connector][2](json_projects_map="data/projects-release-mediawiki-uc2.json",
                                                            db_user=self.db_user,
                                                            db_password=self.db_password)
        item = self.items[0]
        eitems = enrich_backend.get_rich_item_reviews(item)

        for ei in eitems:
            self.assertEqual(ei['url'], 'https://wiki.mozilla.org/Main_Page/QA/NoMore404s')
            self.assertIn('project', ei)
            self.assertIn('project_1', ei)

    def test_refresh_identities(self):
        """Test refresh identities"""

        result = self._test_refresh_identities()
        # ... ?

    def test_perceval_params(self):
        """Test the extraction of perceval params from an URL"""

        url = "https://wiki.mozilla.org"
        expected_params = [
            "https://wiki.mozilla.org"
        ]
        self.assertListEqual(MediaWikiOcean.get_perceval_params_from_url(url), expected_params)

        url = "https://wiki-archive.opendaylight.org https://wiki-archive.opendaylight.org/view"
        expected_params = [
            "https://wiki-archive.opendaylight.org"
        ]
        self.assertListEqual(MediaWikiOcean.get_perceval_params_from_url(url), expected_params)

    def test_p2o_params(self):
        """Test the extraction of p2o params from an URL"""

        url = "https://wiki-archive.opendaylight.org " \
              "https://wiki-archive.opendaylight.org/view" \
              "--filter-no-collection=true"
        expected_params = {
            'url': 'https://wiki-archive.opendaylight.org https://wiki-archive.opendaylight.org/view',
            'filter-no-collection': 'true'
        }
        self.assertDictEqual(MediaWikiOcean.get_p2o_params_from_url(url), expected_params)

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
