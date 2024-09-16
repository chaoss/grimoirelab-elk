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
#     Quan Zhou <quan@bitergia.com>
#


import datetime
import logging
import os
import requests
import time
import shutil
import unittest

from sgqlc.operation import Operation

from base import TestBaseBackend
from grimoire_elk.raw.git import GitOcean
from grimoire_elk.enriched.enrich import (logger,
                                          anonymize_url)
from grimoire_elk.enriched.git import logger as git_logger
from grimoire_elk.enriched.utils import REPO_LABELS
from sortinghat.cli.client import SortingHatSchema, SortingHatClientError


CONF_FILE = 'tests_sh.cfg'
HEADER_JSON = {"Content-Type": "application/json"}


class TestGit(TestBaseBackend):
    """Test Git backend"""

    connector = "git"
    ocean_index = "test_" + connector
    ocean_index_anonymized = "test_" + connector + "_anonymized"
    ocean_aliases = ["a", "b", "git-raw"]
    enrich_index = "test_" + connector + "_enrich"
    enrich_index_anonymized = "test_" + connector + "_enrich_anonymized"
    enrich_aliases = ["c", "d", "git"]

    def test_has_identites(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()
        self.assertEqual(result['items'], 11)
        self.assertEqual(result['raw'], 11)

        aliases = self.ocean_backend.elastic.list_aliases()
        self.assertListEqual(self.ocean_aliases, list(aliases.keys()))

        url = self.es_con + "/" + self.ocean_index + "/_search?size=20"
        response = self.ocean_backend.requests.get(url, verify=False).json()

        time.sleep(5)  # HACK: Wait until git enrich index has been written
        for hit in response['hits']['hits']:
            item = hit['_source']
            self.assertNotIn('username:password', item['origin'])
            self.assertNotIn('username:password', item['tag'])

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()
        self.assertEqual(result['raw'], 11)
        self.assertEqual(result['enrich'], 11)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['committer_name'], '')
        self.assertNotIn('username:password', eitem['origin'])
        self.assertNotIn('username:password', eitem['tag'])

        for item in self.items[1:]:
            eitem = enrich_backend.get_rich_item(item)
            self.assertNotEqual(eitem['committer_name'], 'Unknown')
            self.assertNotEqual(eitem['author_name'], 'Unknown')
            self.assertNotIn('username:password', eitem['origin'])
            self.assertNotIn('username:password', eitem['tag'])

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_date'], '2012-08-14T14:32:15')
        self.assertEqual(eitem['author_date_weekday'], 2)
        self.assertEqual(eitem['author_date_hour'], 14)
        self.assertEqual(eitem['utc_author_date_weekday'], 2)
        self.assertEqual(eitem['utc_author_date_hour'], 17)
        self.assertEqual(eitem['author_uuid'], '8b8d552af706acff79df0f18f5295391c51acd79')
        self.assertEqual(eitem['author_domain'], 'gmail.com')
        self.assertEqual(eitem['author_name'], 'Eduardo Morais and Zhongpeng Lin')
        self.assertEqual(eitem['commit_date'], '2012-08-14T14:32:15')
        self.assertEqual(eitem['commit_date_weekday'], 2)
        self.assertEqual(eitem['commit_date_hour'], 14)
        self.assertEqual(eitem['utc_commit_date_weekday'], 2)
        self.assertEqual(eitem['utc_commit_date_hour'], 17)
        self.assertListEqual(eitem['acked_by_multi_names'], [])
        self.assertListEqual(eitem['co_developed_by_multi_names'], [])
        self.assertListEqual(eitem['reported_by_multi_names'], [])
        self.assertListEqual(eitem['reviewed_by_multi_names'], [])
        self.assertListEqual(eitem['signed_off_by_multi_names'], [])
        self.assertListEqual(eitem['suggested_by_multi_names'], [])
        self.assertListEqual(eitem['tested_by_multi_names'], [])
        self.assertListEqual(eitem['non_authored_signed_off_by_multi_names'], [])
        self.assertListEqual(eitem['approved_by_multi_names'], [])
        self.assertListEqual(eitem['approved_by_multi_domains'], [])
        self.assertListEqual(eitem['approved_by_multi_bots'], [])
        self.assertListEqual(eitem['approved_by_multi_org_names'], [])
        self.assertListEqual(eitem['approved_by_multi_uuids'], [])
        self.assertListEqual(eitem['co_authored_by_multi_names'], [])
        self.assertListEqual(eitem['co_authored_by_multi_domains'], [])
        self.assertListEqual(eitem['merged_by_multi_names'], [])
        self.assertListEqual(eitem['merged_by_multi_domains'], [])
        self.assertListEqual(eitem['non_authored_approved_by_multi_names'], [])
        self.assertListEqual(eitem['non_authored_approved_by_multi_domains'], [])
        self.assertListEqual(eitem['non_authored_co_authored_by_multi_names'], [])
        self.assertListEqual(eitem['non_authored_co_authored_by_multi_domains'], [])
        self.assertListEqual(eitem['non_authored_merged_by_multi_names'], [])
        self.assertListEqual(eitem['non_authored_merged_by_multi_domains'], [])

        item = self.items[8]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_date'], '2014-02-11T22:10:39')
        self.assertEqual(eitem['author_date_weekday'], 2)
        self.assertEqual(eitem['author_date_hour'], 22)
        self.assertEqual(eitem['utc_author_date_weekday'], 3)
        self.assertEqual(eitem['utc_author_date_hour'], 6)
        self.assertEqual(eitem['author_uuid'], 'c77da8cd8adf8cb0d41ee16e57626528c0bf6740')
        self.assertEqual(eitem['author_domain'], 'boss.io')
        self.assertEqual(eitem['author_name'], 'Owl Capone')
        self.assertEqual(eitem['commit_date'], '2014-02-11T22:10:39')
        self.assertEqual(eitem['commit_date_weekday'], 2)
        self.assertEqual(eitem['commit_date_hour'], 22)
        self.assertEqual(eitem['utc_commit_date_weekday'], 3)
        self.assertEqual(eitem['utc_commit_date_hour'], 6)
        self.assertListEqual(eitem['acked_by_multi_names'], [])
        self.assertListEqual(eitem['co_developed_by_multi_names'], [])
        self.assertListEqual(eitem['reported_by_multi_names'], [])
        self.assertListEqual(eitem['reviewed_by_multi_domains'], ['boss.io'])
        self.assertListEqual(eitem['reviewed_by_multi_names'], ['Owl Capone'])
        self.assertListEqual(eitem['signed_off_by_multi_domains'], ['boss.io', 'second.io'])
        self.assertListEqual(eitem['signed_off_by_multi_names'], ['Owl Capone', 'Owl Second'])
        self.assertListEqual(eitem['suggested_by_multi_names'], [])
        self.assertListEqual(eitem['tested_by_multi_names'], [])
        self.assertListEqual(eitem['non_authored_signed_off_by_multi_names'], ['Owl Second'])
        self.assertListEqual(eitem['approved_by_multi_names'], ['Owl Third'])
        self.assertListEqual(eitem['approved_by_multi_domains'], ['third.io'])
        self.assertListEqual(eitem['approved_by_multi_bots'], [False])
        self.assertListEqual(eitem['approved_by_multi_org_names'], ['Unknown'])
        self.assertListEqual(eitem['approved_by_multi_uuids'], ['1bdac6c64760c1027a3777c9ba22adddb0e97c5e'])
        self.assertListEqual(eitem['co_authored_by_multi_names'], ['OwlBot'])
        self.assertListEqual(eitem['co_authored_by_multi_domains'], ['boss.io'])
        self.assertListEqual(eitem['merged_by_multi_names'], ['Owl Third'])
        self.assertListEqual(eitem['merged_by_multi_domains'], ['third.io'])
        self.assertListEqual(eitem['non_authored_approved_by_multi_names'], ['Owl Third'])
        self.assertListEqual(eitem['non_authored_approved_by_multi_domains'], ['third.io'])
        self.assertListEqual(eitem['non_authored_co_authored_by_multi_names'], ['OwlBot'])
        self.assertListEqual(eitem['non_authored_co_authored_by_multi_domains'], ['boss.io'])
        self.assertListEqual(eitem['non_authored_merged_by_multi_names'], ['Owl Third'])
        self.assertListEqual(eitem['non_authored_merged_by_multi_domains'], ['third.io'])

        aliases = self.enrich_backend.elastic.list_aliases()
        self.assertListEqual(self.enrich_aliases, list(aliases.keys()))

    def test_raw_to_enrich_pair_programming(self):
        """Test whether the raw index is properly enriched with pair programming info"""

        result = self._test_raw_to_enrich(pair_programming=True)
        self.assertEqual(result['raw'], 11)
        self.assertEqual(result['enrich'], 16)

        enrich_backend = self.connectors[self.connector][2](pair_programming=True)
        url = self.es_con + "/" + self.enrich_index + "/_search?size=20"
        response = enrich_backend.requests.get(url, verify=False).json()

        time.sleep(5)  # HACK: Wait until git enrich index has been written
        for hit in response['hits']['hits']:
            source = hit['_source']
            self.assertIn('author_date', source)
            self.assertIn('author_date_weekday', source)
            self.assertIn('author_date_hour', source)
            self.assertIn('utc_author_date_weekday', source)
            self.assertIn('utc_author_date_hour', source)
            self.assertIn('author_uuid', source)
            self.assertIn('author_domain', source)
            self.assertIn('author_name', source)
            self.assertIn('commit_date', source)
            self.assertIn('commit_date_weekday', source)
            self.assertIn('commit_date_hour', source)
            self.assertIn('utc_commit_date_weekday', source)
            self.assertIn('utc_commit_date_hour', source)
            self.assertIn('pair_programming_commit', source)
            self.assertIn('pair_programming_files', source)
            self.assertIn('pair_programming_lines_added', source)
            self.assertIn('pair_programming_lines_removed', source)
            self.assertIn('pair_programming_lines_changed', source)
            self.assertIn('is_git_commit_multi_author', source)
            self.assertIn('Signed-off-by_number', source)
            self.assertIn('is_git_commit_signed_off', source)
            self.assertIn('git_uuid', source)

    def test_enrich_repo_labels(self):
        """Test whether the field REPO_LABELS is present in the enriched items"""

        self._test_raw_to_enrich()
        enrich_backend = self.connectors[self.connector][2]()

        for item in self.items:
            eitem = enrich_backend.get_rich_item(item)
            self.assertIn(REPO_LABELS, eitem)
            self.assertNotIn('username:password', eitem['origin'])
            self.assertNotIn('username:password', eitem['tag'])

    def test_raw_to_enrich_sorting_hat(self):
        """Test enrich with SortingHat"""

        result = self._test_raw_to_enrich(sortinghat=True)
        self.assertEqual(result['raw'], 11)
        self.assertEqual(result['enrich'], 11)

        enrich_backend = self.connectors[self.connector][2]()
        enrich_backend.sortinghat = True

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertIn('Commit_org_name', eitem)
        self.assertIn('Commit_user_name', eitem)
        self.assertIn('Commit_name', eitem)
        self.assertIn('committer_name', eitem)
        self.assertIn('Author_name', eitem)
        self.assertIn('author_name', eitem)
        self.assertIn('Commit_multi_org_names', eitem)
        self.assertIn('Author_user_name', eitem)
        self.assertIn('Author_multi_org_names', eitem)
        self.assertNotIn('username:password', eitem['origin'])
        self.assertNotIn('username:password', eitem['tag'])

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        self.assertEqual(result['raw'], 11)
        self.assertEqual(result['enrich'], 11)
        enrich_backend = self.connectors[self.connector][2]()
        url = self.es_con + "/" + self.enrich_index + "/_search?size=20"
        response = enrich_backend.requests.get(url, verify=False).json()

        for hit in response['hits']['hits']:
            source = hit['_source']
            if 'author_uuid' in source:
                self.assertIn('author_date', source)
                self.assertIn('author_date_weekday', source)
                self.assertIn('author_date_hour', source)
                self.assertIn('utc_author_date_weekday', source)
                self.assertIn('utc_author_date_hour', source)
                self.assertIn('author_uuid', source)
                self.assertIn('author_domain', source)
                self.assertIn('author_name', source)
                self.assertIn('commit_date', source)
                self.assertIn('commit_date_weekday', source)
                self.assertIn('commit_date_hour', source)
                self.assertIn('utc_commit_date_weekday', source)
                self.assertIn('utc_commit_date_hour', source)
                self.assertIn('project', source)
                self.assertIn('project_1', source)
                self.assertEqual(source['project'], 'Main')
                self.assertEqual(source['project_1'], 'Main')
                self.assertNotIn('username:password', source['origin'])
                self.assertNotIn('username:password', source['tag'])

    def test_raw_to_enrich_sorting_hat_multi_enroll(self):
        """Test enrich with SortingHat with multiple enrollments"""

        def add_individual(task, args):
            op = Operation(SortingHatSchema.SortingHatMutation)
            identity = op.add_identity(**args)
            identity.uuid()
            try:
                result = task.execute(op)
                uuid = result['data']['addIdentity']['uuid']
            except SortingHatClientError as e:
                uuid = e.errors[0]['message'].split("'")[1]
            return uuid

        def add_organization(task, args):
            op = Operation(SortingHatSchema.SortingHatMutation)
            org = op.add_organization(**args)
            org.organization.name()
            try:
                task.execute(op)
            except SortingHatClientError:
                pass

        def add_enroll(task, args):
            op = Operation(SortingHatSchema.SortingHatMutation)
            add_org = op.enroll(**args)
            add_org.individual().enrollments().group().name()
            try:
                task.execute(op)
            except SortingHatClientError:
                pass

        self.enrich_backend = self.connectors[self.connector][2](db_sortinghat="test_sh",
                                                                 db_user=self.db_user,
                                                                 db_password=self.db_password,
                                                                 db_host=self.db_host,
                                                                 db_port=self.db_port,
                                                                 db_path=self.db_path,
                                                                 db_ssl=self.db_ssl,
                                                                 db_verify_ssl=self.db_verify_ssl,
                                                                 db_tenant=self.db_tenant)
        task = self.enrich_backend.sh_db
        individual_args = {
            "name": "Owl Capone",
            "email": "owlcapone@boss.io",
            "source": "git"
        }
        uuid = add_individual(task, individual_args)
        org1_args = {
            "name": "Test Boss"
        }
        org2_args = {
            "name": "Test Second"
        }
        add_organization(task, org1_args)
        add_organization(task, org2_args)
        enroll1_args = {
            "from_date": "1900-01-01T00:00:00",
            "group": "Test Boss",
            "to_date": "2100-01-01T00:00:00",
            "uuid": uuid
        }
        enroll2_args = {
            "from_date": "1900-01-01T00:00:00",
            "group": "Test Second",
            "to_date": "2100-01-01T00:00:00",
            "uuid": uuid
        }
        add_enroll(task, enroll1_args)
        add_enroll(task, enroll2_args)
        result = self._test_raw_to_enrich(sortinghat=True)
        self.assertEqual(result['raw'], 11)
        self.assertEqual(result['enrich'], 11)

        enrich_backend = self.connectors[self.connector][2]()
        enrich_backend.sortinghat = True
        item = self.items[8]
        eitem = enrich_backend.get_rich_item(item)
        self.assertListEqual(eitem['author_multi_org_names'], ['Test Boss', 'Test Second'])
        self.assertListEqual(eitem['signed_off_by_multi_names'], ['Owl Capone', 'Owl Second'])
        self.assertListEqual(eitem['signed_off_by_multi_org_names'], ['Test Boss', 'Test Second', 'Unknown'])

    def test_refresh_identities(self):
        """Test refresh identities"""

        result = self._test_refresh_identities()
        # ... ?

    def test_demography_study(self):
        """ Test that the demography study works correctly """

        alias = 'demographics'
        study, ocean_backend, enrich_backend = self._test_study('enrich_demography')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_demography":
                study(ocean_backend, enrich_backend, alias, date_field="utc_commit")

            self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.enrich:[git] Demography '
                                           'starting study %s/test_git_enrich'
                             % anonymize_url(self.es_con))
            self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.enrich:[git] Demography '
                                            'end %s/test_git_enrich'
                             % anonymize_url(self.es_con))

        time.sleep(5)  # HACK: Wait until git enrich index has been written
        items = [item for item in enrich_backend.fetch()]
        self.assertEqual(len(items), 11)
        for item in items:
            self.assertTrue('demography_min_date' in item.keys())
            self.assertTrue('demography_max_date' in item.keys())
            self.assertNotIn('username:password', item['origin'])
            self.assertNotIn('username:password', item['tag'])

        r = enrich_backend.elastic.requests.get(enrich_backend.elastic.index_url + "/_alias",
                                                headers=HEADER_JSON, verify=False)
        self.assertIn(alias, r.json()[enrich_backend.elastic.index]['aliases'])

    def test_extra_study(self):
        """ Test that the extra study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_extra_data')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_extra_data":
                study(ocean_backend, enrich_backend,
                      json_url="https://gist.githubusercontent.com/valeriocos/893f55c28c4bd8fa7a217c4e201f4698/raw/"
                               "ba298a6fb09558e68c5e4ec6ae23b1c89fe920ef/test_extra_study.txt")

        time.sleep(5)  # HACK: Wait until git enrich index has been written
        items = [item for item in enrich_backend.fetch()]
        self.assertEqual(len(items), 11)
        for item in items:
            if item['origin'] == '/tmp/perceval_mc84igfc/gittest':
                self.assertIn('extra_secret_repo', item.keys())
            else:
                self.assertNotIn('extra_secret_repo', item.keys())

    def test_enrich_forecast_activity(self):
        """ Test that the forecast activity study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_forecast_activity')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_forecast_activity":
                study(ocean_backend, enrich_backend, out_index='test_git_study_forecast_activity', observations=2)

                self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.enrich:'
                                               '[enrich-forecast-activity] Start study')
                self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.enrich:'
                                                '[enrich-forecast-activity] End study')

        time.sleep(5)  # HACK: Wait until git enrich index has been written
        url = self.es_con + "/test_git_study_forecast_activity/_search?size=20"
        response = enrich_backend.requests.get(url, verify=False).json()
        for hit in response['hits']['hits']:
            source = hit['_source']
            self.assertIn('uuid', source)
            self.assertIn('origin', source)
            self.assertIn('repository', source)
            self.assertIn('interval_months', source)
            self.assertIn('from_date', source)
            self.assertIn('to_date', source)
            self.assertIn('study_creation_date', source)
            self.assertIn('grimoire_creation_date', source)
            self.assertIn('is_git_survived', source)
            self.assertIn('author_uuid', source)
            self.assertIn('author_name', source)
            self.assertIn('author_bot', source)
            self.assertIn('author_user_name', source)
            self.assertIn('author_org_name', source)
            self.assertIn('author_domain', source)
            self.assertIn('prediction_05', source)
            self.assertIn('prediction_07', source)
            self.assertIn('prediction_09', source)
            self.assertIn('next_activity_05', source)
            self.assertIn('next_activity_07', source)
            self.assertIn('next_activity_09', source)
            self.assertIn('metadata__gelk_version', source)
            self.assertIn('metadata__gelk_backend_name', source)
            self.assertIn('metadata__enriched_on', source)

    def test_onion_study(self):
        """ Test that the onion study works correctly """

        alias = "all_onion"
        study, ocean_backend, enrich_backend = self._test_study('enrich_onion')
        study(ocean_backend, enrich_backend, alias, in_index='test_git_enrich', out_index='test_git_onion')

        url = self.es_con + "/_aliases"
        response = requests.get(url, verify=False).json()
        self.assertTrue('test_git_onion' in response)

        time.sleep(1)

        url = self.es_con + "/test_git_onion/_search?size=50"
        response = requests.get(url, verify=False).json()
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 28)
        for hit in hits:
            source = hit['_source']
            self.assertIn('timeframe', source)
            self.assertIn('author_uuid', source)
            self.assertIn('author_name', source)
            self.assertIn('contributions', source)
            self.assertIn('metadata__timestamp', source)
            self.assertIn('project', source)
            self.assertIn('author_org_name', source)
            self.assertIn('cum_net_sum', source)
            self.assertIn('percent_cum_net_sum', source)
            self.assertIn('onion_role', source)
            self.assertIn('quarter', source)
            self.assertIn('metadata__enriched_on', source)
            self.assertIn('data_source', source)
            self.assertIn('grimoire_creation_date', source)

    def test_enrich_areas_of_code(self):
        """ Test that areas of code works correctly"""

        alias = 'enrich_areas_of_code'
        projects_json_repo = "/tmp/perceval_mc84igfc/gittest"
        projects_json = {
            "project": {
                "git": [
                    "/tmp/perceval_mc84igfc/gittest"
                ]
            }
        }
        prjs_map = {
            "git": {
                "/tmp/perceval_mc84igfc/gittest": "project"
            }
        }

        study, ocean_backend, enrich_backend = self._test_study('enrich_areas_of_code',
                                                                projects_json=projects_json,
                                                                prjs_map=prjs_map,
                                                                projects_json_repo=projects_json_repo)

        study(ocean_backend, enrich_backend, alias, in_index='test_git', out_index='test_git_aoc')
        time.sleep(5)  # HACK: Wait until git area of code has been written
        url = self.es_con + "/test_git_aoc/_search?size=20"
        response = enrich_backend.requests.get(url, verify=False).json()
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 12)
        for hit in hits:
            source = hit['_source']
            self.assertIn('addedlines', source)
            self.assertIn('author_bot', source)
            self.assertIn('author_domain', source)
            self.assertIn('author_id', source)
            self.assertIn('author_name', source)
            self.assertIn('author_org_name', source)
            self.assertIn('author_multi_org_names', source)
            self.assertIn('author_user_name', source)
            self.assertIn('author_uuid', source)
            self.assertIn('committer', source)
            self.assertIn('committer_date', source)
            self.assertIn('date', source)
            self.assertIn('eventtype', source)
            self.assertIn('fileaction', source)
            self.assertIn('filepath', source)
            self.assertIn('files', source)
            self.assertIn('filetype', source)
            self.assertIn('file_name', source)
            self.assertIn('file_ext', source)
            self.assertIn('file_dir_name', source)
            self.assertIn('file_path_list', source)
            self.assertIn('git_author_domain', source)
            self.assertIn('grimoire_creation_date', source)
            self.assertIn('hash', source)
            self.assertIn('id', source)
            self.assertIn('message', source)
            self.assertIn('metadata__enriched_on', source)
            self.assertIn('metadata__timestamp', source)
            self.assertIn('metadata__updated_on', source)
            self.assertIn('origin', source)
            self.assertIn('owner', source)
            self.assertIn('perceval_uuid', source)
            self.assertIn('project', source)
            self.assertIn('project_1', source)
            self.assertIn('removedlines', source)
            self.assertIn('repository', source)
            self.assertIn('uuid', source)
            self.assertEqual(source['origin'], '/tmp/perceval_mc84igfc/gittest')
            self.assertEqual(source['repository'], '/tmp/perceval_mc84igfc/gittest')

    def test_enrich_areas_of_code_extra_fields(self):
        """ Test that areas of code works correctly when the repo contains extra fields"""

        alias = "git_areas_of_code"
        projects_json_repo = "https://github.com/acme/errors"
        projects_json = {
            "labels-repo": {
                "git": [
                    "https://github.com/acme/errors --labels=[errors]"
                ]
            }
        }
        prjs_map = {
            "git": {
                "https://github.com/acme/errors": "labels-repo"
            }
        }

        study, ocean_backend, enrich_backend = self._test_study('enrich_areas_of_code',
                                                                projects_json=projects_json,
                                                                prjs_map=prjs_map,
                                                                projects_json_repo=projects_json_repo)

        study(ocean_backend, enrich_backend, alias, in_index='test_git', out_index='test_git_aoc_repo_name')
        time.sleep(5)  # HACK: Wait until git area of code has been written
        url = self.es_con + "/test_git_aoc_repo_name/_search?size=20"
        response = enrich_backend.requests.get(url, verify=False).json()
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 2)
        for hit in hits:
            source = hit['_source']
            print(source['origin'])
            self.assertEqual(source['origin'], 'https://github.com/acme/errors')
            self.assertEqual(source['repository'], 'https://github.com/acme/errors')

    def test_enrich_areas_of_code_private_repo(self):
        """ Test that areas of code works correctly for git private repos"""

        alias = "git_areas_of_code"
        projects_json_repo = "https://username:password@github.com/acme/errors"
        projects_json = {
            "secret-repo": {
                "git": [
                    "https://username:password@github.com/acme/errors"
                ]
            }
        }
        prjs_map = {
            "git": {
                "https://username:password@github.com/acme/errors": "secret-repo"
            }
        }

        study, ocean_backend, enrich_backend = self._test_study('enrich_areas_of_code',
                                                                projects_json=projects_json,
                                                                prjs_map=prjs_map,
                                                                projects_json_repo=projects_json_repo)

        study(ocean_backend, enrich_backend, alias, in_index='test_git', out_index='test_git_aoc_anonymized')
        time.sleep(5)  # HACK: Wait until git area of code has been written
        url = self.es_con + "/test_git_aoc_anonymized/_search?size=20"
        response = enrich_backend.requests.get(url, verify=False).json()
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 2)
        for hit in hits:
            source = hit['_source']
            self.assertIn('addedlines', source)
            self.assertIn('author_bot', source)
            self.assertIn('author_domain', source)
            self.assertIn('author_id', source)
            self.assertIn('author_name', source)
            self.assertIn('author_org_name', source)
            self.assertIn('author_multi_org_names', source)
            self.assertIn('author_user_name', source)
            self.assertIn('author_uuid', source)
            self.assertIn('committer', source)
            self.assertIn('committer_date', source)
            self.assertIn('date', source)
            self.assertIn('eventtype', source)
            self.assertIn('fileaction', source)
            self.assertIn('filepath', source)
            self.assertIn('files', source)
            self.assertIn('filetype', source)
            self.assertIn('file_name', source)
            self.assertIn('file_ext', source)
            self.assertIn('file_dir_name', source)
            self.assertIn('file_path_list', source)
            self.assertIn('git_author_domain', source)
            self.assertIn('grimoire_creation_date', source)
            self.assertIn('hash', source)
            self.assertIn('id', source)
            self.assertIn('message', source)
            self.assertIn('metadata__enriched_on', source)
            self.assertIn('metadata__timestamp', source)
            self.assertIn('metadata__updated_on', source)
            self.assertIn('origin', source)
            self.assertIn('owner', source)
            self.assertIn('perceval_uuid', source)
            self.assertIn('project', source)
            self.assertIn('project_1', source)
            self.assertIn('removedlines', source)
            self.assertIn('repository', source)
            self.assertIn('uuid', source)
            self.assertEqual(source['origin'], 'https://github.com/acme/errors')
            self.assertEqual(source['repository'], 'https://github.com/acme/errors')

    def test_enrich_git_branches_study(self):
        """ Test that the git branches study works correctly """

        study, ocean_backend, enrich_backend = self._test_study('enrich_git_branches')

        items = [item for item in enrich_backend.fetch()]
        self.assertEqual(len(items), 11)
        for item in items:
            self.assertTrue('branches' in item.keys())

    def test_enrich_git_branches_study_filter_no_collection(self):
        """ Test that the git branches study skip when --filter-no-collection is present """

        projects_json_repo = "https://github.com/grimoirelab/perceval.git --filter-no-collection=true"
        projects_json = {
            "filter-repo": {
                "git": [
                    "https://github.com/grimoirelab/perceval.git --filter-no-collection=true"
                ]
            }
        }
        prjs_map = {
            "git": {
                "https://github.com/grimoirelab/perceval.git --filter-no-collection=true": "filter-repo"
            }
        }

        study, ocean_backend, enrich_backend = self._test_study('enrich_git_branches',
                                                                projects_json=projects_json,
                                                                prjs_map=prjs_map,
                                                                projects_json_repo=projects_json_repo)

        today = datetime.datetime.today().day
        with self.assertLogs(git_logger, level='INFO') as cm:
            if study.__name__ == "enrich_git_branches":
                study(ocean_backend, enrich_backend, run_month_days=[today])
                self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.git:[git] study git-branches start')
                self.assertEqual(cm.output[1], 'INFO:grimoire_elk.enriched.git:[git] study git-branches skipping'
                                               ' repo {}'.format(projects_json_repo))
                self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.git:[git] study git-branches end')

    def test_enrich_git_branches_study_skip_uncloned_repository(self):
        """ Test that the git branches study skip when the repository is not cloned """

        projects_json_repo = "https://github.com/chaoss/grimoirelab-perceval.git"
        projects_json = {
            "no-cloned": {
                "git": [
                    "https://github.com/chaoss/grimoirelab-perceval.git"
                ]
            }
        }
        prjs_map = {
            "git": {
                "https://github.com/chaoss/grimoirelab-perceval.git": "no-cloned"
            }
        }

        study, ocean_backend, enrich_backend = self._test_study('enrich_git_branches',
                                                                projects_json=projects_json,
                                                                prjs_map=prjs_map,
                                                                projects_json_repo=projects_json_repo)
        # Remove the clone
        base_path = os.path.expanduser('~/.perceval/repositories/')
        processed_uri = projects_json_repo.lstrip('/')
        git_path = os.path.join(base_path, processed_uri) + '-git'
        if os.path.exists(git_path) and os.path.isdir(git_path):
            shutil.rmtree(git_path, ignore_errors=True)

        today = datetime.datetime.today().day
        with self.assertLogs(git_logger, level='INFO') as cm:
            if study.__name__ == "enrich_git_branches":
                study(ocean_backend, enrich_backend, run_month_days=[today])
                self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.git:[git] study git-branches start')
                self.assertEqual(cm.output[1], 'ERROR:grimoire_elk.enriched.git:[git] study git-branches skipping'
                                               ' not cloned repo {}'.format(projects_json_repo))
                self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.git:[git] study git-branches end')

    def test_perceval_params(self):
        """Test the extraction of perceval params from an URL"""

        url = "https://github.com/grimoirelab/perceval"
        expected_params = [
            'https://github.com/grimoirelab/perceval'
        ]
        self.assertListEqual(GitOcean.get_perceval_params_from_url(url), expected_params)

        url = "https://github.com/grimoirelab/perceval /tmp/perceval-repo"
        expected_params = [
            'https://github.com/grimoirelab/perceval'
        ]
        self.assertListEqual(GitOcean.get_perceval_params_from_url(url), expected_params)

    def test_items_to_raw_anonymized(self):
        """Test whether JSON items are properly inserted into ES anonymized"""

        result = self._test_items_to_raw_anonymized()

        self.assertGreater(result['items'], 0)
        self.assertGreater(result['raw'], 0)
        self.assertEqual(result['items'], result['raw'])

        item = self.items[0]['data']
        self.assertEqual(item['Author'], 'e2ea52f7f782fe08109b762e474ff20656a51f47 <xxxxxx@gmail.com>')
        self.assertEqual(item['Commit'], '')

        item = self.items[1]['data']
        self.assertEqual(item['Author'], '7bb272958a7c0c54de85dc078aa1b98da7b930de <xxxxxx@gmail.com>')
        self.assertEqual(item['Commit'], '7bb272958a7c0c54de85dc078aa1b98da7b930de <xxxxxx@gmail.com>')

        item = self.items[6]['data']
        self.assertEqual(item['Author'], 'abe1a5515d468ed258124c4c946ceb34ef7ffbda <xxxxxx@gmail.com>')
        self.assertEqual(item['Commit'], 'abe1a5515d468ed258124c4c946ceb34ef7ffbda <xxxxxx@gmail.com>')

    def test_raw_to_enrich_anonymized(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich_anonymized()

        self.assertGreater(result['raw'], 0)
        self.assertGreater(result['enrich'], 0)
        self.assertEqual(result['raw'], result['enrich'])

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_uuid'], '5fe609e6ddadd19697e69832ac1889bb942cccfb')
        self.assertEqual(eitem['author_domain'], 'gmail.com')
        self.assertEqual(eitem['author_name'], 'e2ea52f7f782fe08109b762e474ff20656a51f47')
        self.assertIsNone(eitem['committer_domain'])
        self.assertEqual(eitem['committer_name'], '')

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_uuid'], '0442af9edda128740e57331769ee9e68a99ec36f')
        self.assertEqual(eitem['author_domain'], 'gmail.com')
        self.assertEqual(eitem['author_name'], '7bb272958a7c0c54de85dc078aa1b98da7b930de')
        self.assertEqual(eitem['committer_domain'], 'gmail.com')
        self.assertEqual(eitem['committer_name'], '7bb272958a7c0c54de85dc078aa1b98da7b930de')

        item = self.items[6]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['author_uuid'], '221aa5305c73e6574dbba1865ccc930eaa6f59a8')
        self.assertEqual(eitem['author_domain'], 'gmail.com')
        self.assertEqual(eitem['author_name'], 'abe1a5515d468ed258124c4c946ceb34ef7ffbda')
        self.assertEqual(eitem['committer_domain'], 'gmail.com')
        self.assertEqual(eitem['committer_name'], 'abe1a5515d468ed258124c4c946ceb34ef7ffbda')

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
