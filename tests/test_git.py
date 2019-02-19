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
import requests
import time
import unittest

from base import TestBaseBackend
from grimoire_elk.raw.git import GitOcean
from grimoire_elk.enriched.enrich import (logger,
                                          DEMOGRAPHICS_ALIAS)


HEADER_JSON = {"Content-Type": "application/json"}


class TestGit(TestBaseBackend):
    """Test Git backend"""

    connector = "git"
    ocean_index = "test_" + connector
    ocean_aliases = ["a", "b", "git-raw"]
    enrich_index = "test_" + connector + "_enrich"
    enrich_aliases = ["c", "d", "git"]

    def test_has_identites(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()
        self.assertEqual(result['items'], 9)
        self.assertEqual(result['raw'], 9)

        aliases = self.ocean_backend.elastic.list_aliases()
        self.assertListEqual(self.ocean_aliases, list(aliases.keys()))

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()
        self.assertEqual(result['raw'], 9)
        self.assertEqual(result['enrich'], 9)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['committer_name'], '')

        for item in self.items[1:]:
            eitem = enrich_backend.get_rich_item(item)
            self.assertNotEqual(eitem['committer_name'], 'Unknown')
            self.assertNotEqual(eitem['author_name'], 'Unknown')

        aliases = self.enrich_backend.elastic.list_aliases()
        self.assertListEqual(self.enrich_aliases, list(aliases.keys()))

    def test_raw_to_enrich_sorting_hat(self):
        """Test enrich with SortingHat"""

        result = self._test_raw_to_enrich(sortinghat=True)
        self.assertEqual(result['raw'], 9)
        self.assertEqual(result['enrich'], 9)

        enrich_backend = self.connectors[self.connector][2]()
        enrich_backend.sortinghat = True

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['committer_name'], '')
        self.assertEqual(eitem['Commit_name'], '-- UNDEFINED --')
        self.assertEqual(eitem['Commit_user_name'], '-- UNDEFINED --')
        self.assertEqual(eitem['Commit_org_name'], '-- UNDEFINED --')

        self.assertEqual(eitem['author_name'], 'Eduardo Morais')
        self.assertEqual(eitem['Author_name'], 'Eduardo Morais')
        self.assertEqual(eitem['Author_user_name'], 'Unknown')

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

    def test_demography_study(self):
        """ Test that the demography study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_demography')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_demography":
                study(ocean_backend, enrich_backend, date_field="utc_commit")

            self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.enrich:[Demography] '
                                           'Starting study %s/test_git_enrich'
                             % enrich_backend.elastic.anonymize_url(self.es_con))
            self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.enrich:[Demography] '
                                            'End %s/test_git_enrich'
                             % enrich_backend.elastic.anonymize_url(self.es_con))

        time.sleep(1)  # HACK: Wait until git enrich index has been written
        for item in enrich_backend.fetch():
            self.assertTrue('demography_min_date' in item.keys())
            self.assertTrue('demography_max_date' in item.keys())

        r = enrich_backend.elastic.requests.get(enrich_backend.elastic.index_url + "/_alias",
                                                headers=HEADER_JSON, verify=False)
        self.assertIn(DEMOGRAPHICS_ALIAS, r.json()[enrich_backend.elastic.index]['aliases'])

    def test_onion_study(self):
        """ Test that the onion study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_onion')
        study(ocean_backend, enrich_backend, in_index='test_git_enrich')

        url = self.es_con + "/_aliases"
        response = requests.get(url, verify=False).json()
        self.assertTrue('git_onion-enriched' in response)

        time.sleep(1)

        url = self.es_con + "/git_onion-enriched/_count"
        response = requests.get(url, verify=False).json()

        self.assertGreater(response['count'], 0)

        delete_onion = self.es_con + "/git_onion-enriched"
        requests.delete(delete_onion, verify=False)

    def test_arthur_params(self):
        """Test the extraction of arthur params from an URL"""

        with open("data/projects-release.json") as projects_filename:
            url = json.load(projects_filename)['grimoire']['git'][0]
            arthur_params = {'uri': 'https://github.com/grimoirelab/perceval', 'url': 'https://github.com/grimoirelab/perceval'}
            self.assertDictEqual(arthur_params, GitOcean.get_arthur_params_from_url(url))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
