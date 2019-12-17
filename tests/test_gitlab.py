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
#     Valerio Cosentino <valcos@bitergia.com>
#
import logging
import unittest

from base import TestBaseBackend
from grimoire_elk.raw.gitlab import GitLabOcean
from grimoire_elk.enriched.gitlab import NO_MILESTONE_TAG
from grimoire_elk.enriched.utils import REPO_LABELS


class TestGitLab(TestBaseBackend):
    """Test GitLab backend"""

    connector = "gitlab"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_has_identites(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()

        self.assertGreater(result['items'], 0)
        self.assertGreater(result['raw'], 0)
        self.assertEqual(result['items'], result['raw'])

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()

        self.assertGreater(result['raw'], 0)
        self.assertGreater(result['enrich'], 0)
        self.assertEqual(result['raw'], result['enrich'])

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], "8.17")
        self.assertEqual(eitem['milestone_start_date'], "2017-01-07T00:00:00")
        self.assertEqual(eitem['milestone_due_date'], "2017-02-21T00:00:00")
        self.assertEqual(eitem['milestone_url'], "https://gitlab.com/gitlab-org/gitlab-ce/milestones/34")
        self.assertEqual(eitem['milestone_id'], 134231)
        self.assertEqual(eitem['milestone_iid'], 34)
        self.assertEqual(eitem['labels'], [])
        self.assertEqual(eitem['author_username'], "redfish64")

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], NO_MILESTONE_TAG)
        self.assertEqual(eitem['milestone_start_date'], None)
        self.assertEqual(eitem['milestone_due_date'], None)
        self.assertEqual(eitem['milestone_url'], None)
        self.assertEqual(eitem['milestone_id'], None)
        self.assertEqual(eitem['milestone_iid'], None)
        self.assertEqual(eitem['labels'], [])
        self.assertEqual(eitem['author_username'], "redfish64")

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], NO_MILESTONE_TAG)
        self.assertEqual(eitem['milestone_start_date'], None)
        self.assertEqual(eitem['milestone_due_date'], None)
        self.assertEqual(eitem['milestone_url'], None)
        self.assertEqual(eitem['milestone_id'], None)
        self.assertEqual(eitem['milestone_iid'], None)
        self.assertEqual(eitem['labels'], ['CI/CD', 'Deliverable'])
        self.assertEqual(eitem['author_username'], "YoeriNijs")

        item = self.items[4]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], NO_MILESTONE_TAG)
        self.assertEqual(eitem['milestone_start_date'], None)
        self.assertEqual(eitem['milestone_due_date'], None)
        self.assertEqual(eitem['milestone_url'], None)
        self.assertEqual(eitem['milestone_id'], None)
        self.assertEqual(eitem['milestone_iid'], None)
        self.assertEqual(eitem['labels'], [])
        self.assertEqual(eitem['author_username'], None)

        item = self.items[5]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], "8.17")
        self.assertEqual(eitem['milestone_start_date'], "2017-01-07T00:00:00")
        self.assertEqual(eitem['milestone_due_date'], "2017-02-21T00:00:00")
        self.assertEqual(eitem['milestone_url'], "https://gitlab.com/gitlab-org/gitlab-ce/milestones/34")
        self.assertEqual(eitem['milestone_id'], 134231)
        self.assertEqual(eitem['milestone_iid'], 34)
        self.assertEqual(eitem['labels'], [])
        self.assertEqual(eitem['author_username'], "grote")

        item = self.items[6]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], NO_MILESTONE_TAG)
        self.assertEqual(eitem['milestone_start_date'], None)
        self.assertEqual(eitem['milestone_due_date'], None)
        self.assertEqual(eitem['milestone_url'], None)
        self.assertEqual(eitem['milestone_id'], None)
        self.assertEqual(eitem['milestone_iid'], None)
        self.assertEqual(eitem['labels'], [])
        self.assertEqual(eitem['author_username'], "Rudloff")

        item = self.items[7]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['milestone'], NO_MILESTONE_TAG)
        self.assertEqual(eitem['milestone_start_date'], None)
        self.assertEqual(eitem['milestone_due_date'], None)
        self.assertEqual(eitem['milestone_url'], None)
        self.assertEqual(eitem['milestone_id'], None)
        self.assertEqual(eitem['milestone_iid'], None)
        self.assertEqual(eitem['labels'], ['CI/CD', 'Deliverable'])
        self.assertEqual(eitem['author_username'], "marc.nause")

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
        # ... ?

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        # ... ?

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

        url = "https://gitlab.com/gitlab-org/gitlab-ee --blacklist-ids 1 10 100 --max-retries 100"
        arthur_params = {'owner': 'gitlab-org',
                         'repository': 'gitlab-ee',
                         'blacklist_ids': [1, 10, 100]}

        self.assertDictEqual(arthur_params, GitLabOcean.get_arthur_params_from_url(url))

        url = "https://gitlab.com/gitlab-org/gitlab-ee --blacklist-ids 1 10 100 --max-retries 100"
        perceval_params = ['gitlab-org', 'gitlab-ee', '--blacklist-ids', '1', '10', '100', '--max-retries', '100']

        self.assertListEqual(perceval_params, GitLabOcean.get_perceval_params_from_url(url))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
