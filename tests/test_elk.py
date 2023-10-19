# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2023 Bitergia
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
#   Quan Zhou <quan@bitergia.com>
#

import configparser
import logging
import unittest

from grimoire_elk.elk import anonymize_params, enrich_backend, feed_backend, logger


CONFIG_FILE = 'tests.conf'


class TestElk(unittest.TestCase):
    """Unit tests for elk class"""

    @classmethod
    def setUpClass(cls):
        cls.config = configparser.ConfigParser()
        cls.config.read(CONFIG_FILE)
        cls.es_con = dict(cls.config.items('ElasticSearch'))['url']

    def test_anonymize_params(self):
        """Test anonymize parameters"""

        expected_params = ("repo", "--api-token", "xxxxx", "xxxxx", "--sleep-for-rate")
        params = ("repo", "--api-token", "token1", "token2", "--sleep-for-rate")
        anonymized_params = anonymize_params(params)
        self.assertTupleEqual(anonymized_params, expected_params)

        expected_params = ("--backend-password", "xxxxx", "--no-archive", "--api-token", "xxxxx")
        params = ("--backend-password", "mypassword", "--no-archive", "--api-token", "token")
        anonymized_params = anonymize_params(params)
        self.assertTupleEqual(anonymized_params, expected_params)

    def test_feed_backend_wrong_params(self):
        """Test feed_backend with wrong parameters."""

        # Jira has not --foo-bar argument
        backend_params = ['https://jira.example.org', '--foo-bar']
        projects_json_repo = 'https://jira.example.org'
        expected_msg = "Error feeding raw. Wrong jira arguments: ('https://jira.example.org', '--foo-bar')"
        error_msg = feed_backend(self.es_con, False, False, "jira", backend_params,
                                 "jira_raw", "jira_enriched", projects_json_repo=projects_json_repo)
        self.assertEqual(error_msg, expected_msg)

        # Github the repository name cannot start with '-'
        backend_params = ['chaoss', '-grimoirelab-elk', '--api-token', 'mypersonaltoken']
        projects_json_repo = 'https://github.com/chaoss/-grimoirelab-elk'
        expected_msg = "Error feeding raw. Wrong github arguments: ('chaoss', " \
                       "'-grimoirelab-elk', '--api-token', 'xxxxx')"
        error_msg = feed_backend(self.es_con, False, False, "github", backend_params,
                                 "github_raw", "github_enriched", projects_json_repo=projects_json_repo)
        self.assertEqual(error_msg, expected_msg)

    def test_enrich_backend_wrong_params(self):
        """Test enrich_backend with wrong parameters."""

        # Jira has not --foo-bar argument
        backend_params = ['https://jira.example.org', '--foo-bar']
        projects_json_repo = 'https://jira.example.org'
        expected_msg = "Error enriching raw. Wrong jira arguments: ('https://jira.example.org', '--foo-bar')"
        with self.assertLogs(logger, level='ERROR') as cm:
            enrich_backend(self.es_con, False, "jira", backend_params, "jira",
                           "jira_raw", "jira_enriched", projects_json_repo=projects_json_repo)
            self.assertEqual(cm.records[0].msg, expected_msg)

        # Github the repository name cannot start with '-'
        backend_params = ['chaoss', '-grimoirelab-elk', '--api-token', 'mypersonaltoken']
        projects_json_repo = 'https://github.com/chaoss/-grimoirelab-elk'
        expected_msg = "Error enriching raw. Wrong github arguments: ('chaoss', " \
                       "'-grimoirelab-elk', '--api-token', 'xxxxx')"
        with self.assertLogs(logger, level='ERROR') as cm:
            enrich_backend(self.es_con, False, "github", backend_params, "github",
                           "github_raw", "github_enriched", projects_json_repo=projects_json_repo)
            self.assertEqual(cm.records[0].msg, expected_msg)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    unittest.main()
