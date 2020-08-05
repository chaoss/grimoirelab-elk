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
import unittest

from base import TestBaseBackend
from grimoire_elk.raw.jira import JiraOcean
from grimoire_elk.enriched.utils import REPO_LABELS


class TestJira(TestBaseBackend):
    """Test Jira backend"""

    connector = "jira"
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
        self.assertEqual(result['enrich'], 14)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['data']['id'], "10010")
        self.assertEqual(eitem['id'], "fcf4a8418030d95844f0825fb5b1c3bdc0a1d942_issue_10010_user_creator")
        self.assertEqual(eitem['number_of_comments'], 0)
        self.assertIn("main_description", eitem)
        self.assertIn("main_description_analyzed", eitem)
        self.assertEqual(eitem['author_type'], 'creator')

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item, author_type='assignee')
        self.assertEqual(item['data']['id'], "10010")
        self.assertEqual(eitem['id'], "fcf4a8418030d95844f0825fb5b1c3bdc0a1d942_issue_10010_user_assignee")
        self.assertEqual(eitem['number_of_comments'], 0)
        self.assertIn("main_description", eitem)
        self.assertIn("main_description_analyzed", eitem)
        self.assertEqual(eitem['author_type'], 'assignee')
        self.assertEqual(eitem['status_category_key'], 'done')
        self.assertEqual(eitem['is_closed'], 1)

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item, author_type='reporter')
        self.assertEqual(item['data']['id'], "10010")
        self.assertEqual(eitem['id'], "fcf4a8418030d95844f0825fb5b1c3bdc0a1d942_issue_10010_user_reporter")
        self.assertEqual(eitem['number_of_comments'], 0)
        self.assertIn("main_description", eitem)
        self.assertIn("main_description_analyzed", eitem)
        self.assertEqual(eitem['author_type'], 'reporter')

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['data']['id'], "10008")
        self.assertEqual(eitem['id'], "d6b4168fd458f910fb9af1df0e9edcfaa188cc67_issue_10008_user_creator")
        self.assertEqual(eitem['number_of_comments'], 0)
        self.assertEqual(eitem['author_type'], 'creator')
        self.assertEqual(eitem['status_category_key'], 'done')
        self.assertEqual(eitem['is_closed'], 1)

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['data']['id'], "10017")
        self.assertEqual(eitem['id'], "305dbd15b1f250cb6941d8b9270af3d3a4405084_issue_10017_user_creator")
        self.assertEqual(eitem['number_of_comments'], 0)
        self.assertEqual(eitem['author_type'], 'creator')
        self.assertEqual(eitem['status_category_key'], 'done')
        self.assertEqual(eitem['is_closed'], 1)

        item = self.items[4]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['data']['id'], "10018")
        self.assertEqual(eitem['id'], "929182e386ddb7d290c0dbd2eb34140993c8f567_issue_10018_user_creator")
        self.assertEqual(eitem['number_of_comments'], 2)
        self.assertEqual(eitem['author_type'], 'creator')
        self.assertEqual(eitem['status_category_key'], 'done')
        self.assertEqual(eitem['is_closed'], 1)

    def test_enrich_repo_labels(self):
        """Test whether the field REPO_LABELS is present in the enriched items"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertIn(REPO_LABELS, eitem)

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertIn(REPO_LABELS, eitem)

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertIn(REPO_LABELS, eitem)

        item = self.items[4]
        eitem = enrich_backend.get_rich_item(item)
        self.assertIn(REPO_LABELS, eitem)

    def test_raw_to_enrich_sorting_hat(self):
        """Test enrich with SortingHat"""

        result = self._test_raw_to_enrich(sortinghat=True)
        self.assertEqual(result['raw'], 5)
        self.assertEqual(result['enrich'], 14)

        enrich_backend = self.connectors[self.connector][2]()
        enrich_backend.sortinghat = True

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_name'], 'Maurizio Pillitu')
        self.assertEqual(eitem['author_org_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'Unknown')
        self.assertEqual(eitem['author_type'], 'creator')
        self.assertEqual(eitem['author_multi_org_names'], ['Unknown'])

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item, author_type='assignee')
        self.assertEqual(eitem['author_name'], 'Peter Monks')
        self.assertEqual(eitem['author_org_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'peter')
        self.assertEqual(eitem['author_type'], 'assignee')
        self.assertEqual(eitem['author_multi_org_names'], ['Unknown'])

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item, author_type='reporter')
        self.assertEqual(eitem['author_name'], 'Maurizio Pillitu')
        self.assertEqual(eitem['author_org_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'maoo')
        self.assertEqual(eitem['author_type'], 'reporter')
        self.assertEqual(eitem['author_multi_org_names'], ['Unknown'])

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        self.assertEqual(result['raw'], 5)
        self.assertEqual(result['enrich'], 14)

    def test_refresh_identities(self):
        """Test refresh identities"""

        result = self._test_refresh_identities()
        # ... ?

    def test_refresh_project(self):
        """Test refresh project field for all sources"""

        result = self._test_refresh_project()
        # ... ?

    def test_get_p2o_params_from_url(self):
        """Test the extraction of p2o params from the projects.json entry"""

        with open("data/projects-release.json") as projects_filename:
            url = json.load(projects_filename)['grimoire']['jira'][0]
            p2o_params = {'url': 'https://jira.opnfv.org', 'filter-raw': 'data.fields.project.key:PROJECT-KEY'}
            self.assertDictEqual(p2o_params, JiraOcean.get_p2o_params_from_url(url))

    def test_copy_raw_fields(self):
        """Test copied raw fields"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        eitems = [enrich_backend.get_rich_item(self.items[0]),
                  enrich_backend.get_rich_item(self.items[0], author_type='assignee'),
                  enrich_backend.get_rich_item(self.items[0], author_type='reporter')]

        for eitem in eitems:
            for attribute in enrich_backend.RAW_FIELDS_COPY:
                if attribute in self.items[0]:
                    self.assertEqual(self.items[0][attribute], eitem[attribute])
                else:
                    self.assertIsNone(eitem[attribute])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
