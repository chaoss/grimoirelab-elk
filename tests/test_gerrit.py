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
import json
import logging
import time
import unittest

from base import TestBaseBackend, DB_SORTINGHAT
from grimoire_elk.raw.gerrit import GerritOcean
from grimoire_elk.enriched.enrich import (logger,
                                          DEMOGRAPHICS_ALIAS)
from grimoire_elk.enriched.utils import REPO_LABELS


HEADER_JSON = {"Content-Type": "application/json"}


class TestGerrit(TestBaseBackend):
    """Test Gerrit backend"""

    connector = "gerrit"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_has_identites(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()
        self.assertEqual(result['items'], 5)
        self.assertEqual(result['raw'], 5)

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()
        self.assertEqual(result['raw'], 5)
        self.assertEqual(result['enrich'], 190)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)

        self.assertIn('metadata__enriched_on', eitem)
        self.assertIn('metadata__gelk_backend_name', eitem)
        self.assertIn('metadata__gelk_version', eitem)
        self.assertIn(REPO_LABELS, eitem)

        comments = item['data']['comments']
        ecomments = enrich_backend.get_rich_item_comments(comments, eitem)
        self.assertEqual(len(ecomments), 46)

        for ecomment in ecomments:
            self.assertIn('comment_message', ecomment)
            self.assertIn('comment_message_analyzed', ecomment)
            self.assertIn('metadata__enriched_on', ecomment)
            self.assertIn('metadata__gelk_backend_name', ecomment)
            self.assertIn('metadata__gelk_version', ecomment)
            self.assertIn(REPO_LABELS, ecomment)

        patchsets = item['data']['patchSets']
        eitems = enrich_backend.get_rich_item_patchsets(patchsets, eitem)
        self.assertEqual(len(eitems), 18)

        for eitem in eitems:
            self.assertIn('metadata__enriched_on', eitem)
            self.assertIn('metadata__gelk_backend_name', eitem)
            self.assertIn('metadata__gelk_version', eitem)
            self.assertIn(REPO_LABELS, eitem)

        epatchsets = [ei for ei in eitems if 'is_gerrit_patchset' in ei]
        eapprovals = [ei for ei in eitems if 'is_gerrit_approval' in ei]

        self.assertEqual(len(epatchsets), 5)
        for epatchset in epatchsets:
            self.assertIn('metadata__enriched_on', epatchset)
            self.assertIn('metadata__gelk_backend_name', epatchset)
            self.assertIn('metadata__gelk_version', epatchset)
            self.assertIn(REPO_LABELS, epatchset)

        self.assertEqual(len(eapprovals), 13)
        for eapproval in eapprovals:
            self.assertIn('approval_description', eapproval)
            self.assertIn('approval_description_analyzed', eapproval)
            self.assertIn('metadata__enriched_on', eapproval)
            self.assertIn('metadata__gelk_backend_name', eapproval)
            self.assertIn('metadata__gelk_version', eapproval)
            self.assertIn(REPO_LABELS, eapproval)

    def test_demography_study(self):
        """ Test that the demography study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_demography')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_demography":
                study(ocean_backend, enrich_backend, date_field="grimoire_creation_date")

            self.assertEqual(cm.output[0],
                             'INFO:grimoire_elk.enriched.enrich:[Demography] Starting study %s/test_gerrit_enrich'
                             % enrich_backend.elastic.anonymize_url(self.es_con))
            self.assertEqual(cm.output[-1],
                             'INFO:grimoire_elk.enriched.enrich:[Demography] End %s/test_gerrit_enrich'
                             % enrich_backend.elastic.anonymize_url(self.es_con))

        time.sleep(5)  # HACK: Wait until git enrich index has been written
        for item in enrich_backend.fetch():
            self.assertTrue('demography_min_date' in item.keys())
            self.assertTrue('demography_max_date' in item.keys())

        r = enrich_backend.elastic.requests.get(enrich_backend.elastic.index_url + "/_alias",
                                                headers=HEADER_JSON, verify=False)
        self.assertIn(DEMOGRAPHICS_ALIAS, r.json()[enrich_backend.elastic.index]['aliases'])

    def test_raw_to_enrich_sorting_hat(self):
        """Test enrich with SortingHat"""

        result = self._test_raw_to_enrich(sortinghat=True)
        self.assertEqual(result['raw'], 5)
        self.assertEqual(result['enrich'], 190)

        enrich_backend = self.connectors[self.connector][2](db_sortinghat=DB_SORTINGHAT,
                                                            db_user=self.db_user,
                                                            db_password=self.db_password)

        for item in self.items:
            eitem = enrich_backend.get_rich_item(item)

            self.assertIn('changeset_author_id', eitem)
            self.assertIn('changeset_author_uuid', eitem)
            self.assertIn('changeset_author_name', eitem)
            self.assertIn('changeset_author_user_name', eitem)
            self.assertIn('changeset_author_domain', eitem)
            self.assertIn('changeset_author_gender', eitem)
            self.assertIn('changeset_author_gender_acc', eitem)
            self.assertIn('changeset_author_org_name', eitem)
            self.assertIn('changeset_author_bot', eitem)

            comments = item['data']['comments']
            ecomments = enrich_backend.get_rich_item_comments(comments, eitem)

            for ecomment in ecomments:
                self.assertIn('changeset_author_id', ecomment)
                self.assertIn('changeset_author_uuid', ecomment)
                self.assertIn('changeset_author_name', ecomment)
                self.assertIn('changeset_author_user_name', ecomment)
                self.assertIn('changeset_author_domain', ecomment)
                self.assertIn('changeset_author_gender', ecomment)
                self.assertIn('changeset_author_gender_acc', ecomment)
                self.assertIn('changeset_author_org_name', ecomment)
                self.assertIn('changeset_author_bot', ecomment)

            patchsets = item['data']['patchSets']
            epatchsets = enrich_backend.get_rich_item_patchsets(patchsets, eitem)

            for epatchset in epatchsets:
                self.assertIn('changeset_author_id', epatchset)
                self.assertIn('changeset_author_uuid', epatchset)
                self.assertIn('changeset_author_name', epatchset)
                self.assertIn('changeset_author_user_name', epatchset)
                self.assertIn('changeset_author_domain', epatchset)
                self.assertIn('changeset_author_gender', epatchset)
                self.assertIn('changeset_author_gender_acc', epatchset)
                self.assertIn('changeset_author_org_name', epatchset)
                self.assertIn('changeset_author_bot', epatchset)

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        self.assertEqual(result['raw'], 5)
        self.assertEqual(result['enrich'], 190)

    def test_refresh_identities(self):
        """Test refresh identities"""

        result = self._test_refresh_identities()
        # ... ?

    def test_refresh_project(self):
        """Test refresh project field for all sources"""

        result = self._test_refresh_project()
        # ... ?

    def test_arthur_params(self):
        """Test the extraction of arthur params from an URL"""

        with open("data/projects-release.json") as projects_filename:
            url = json.load(projects_filename)['grimoire']['gerrit'][0]
            arthur_params = {'uri': 'review.openstack.org', 'url': 'review.openstack.org'}
            self.assertDictEqual(arthur_params, GerritOcean.get_arthur_params_from_url(url))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
