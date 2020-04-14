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
from grimoire_elk.raw.launchpad import LaunchpadOcean


class TestLaunchpad(TestBaseBackend):
    """Test Launchpad backend"""

    connector = "launchpad"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

    def test_has_identities(self):
        """Test value of has_identities method"""

        enrich_backend = self.connectors[self.connector][2]()
        self.assertTrue(enrich_backend.has_identities())

    def test_items_to_raw(self):
        """Test whether JSON items are properly inserted into ES"""

        result = self._test_items_to_raw()

        self.assertEqual(result['items'], 4)
        self.assertEqual(result['raw'], 4)

    def test_raw_to_enrich(self):
        """Test whether the raw index is properly enriched"""

        result = self._test_raw_to_enrich()
        self.assertEqual(result['raw'], 4)
        self.assertEqual(result['enrich'], 4)

        enrich_backend = self.connectors[self.connector][2]()

        item = self.items[0]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['offset'], None)
        self.assertEqual(eitem['origin'], 'https://launchpad.net/mydistribution')
        self.assertEqual(eitem['tag'], 'https://launchpad.net/mydistribution')
        self.assertEqual(eitem['uuid'], 'db64288a362e6c086d344bed242fd5914f966e29')
        self.assertIn('title', eitem)
        self.assertEqual(eitem['web_link'], 'https://bugs.launchpad.net/mydistribution/+source/mypackage/+bug/1')
        self.assertEqual(eitem['date_created'], '2017-08-21T14:28:00.336698+00:00')
        self.assertEqual(eitem['date_incomplete'], None)
        self.assertEqual(eitem['is_complete'], False)
        self.assertEqual(eitem['status'], 'Triaged')
        self.assertEqual(eitem['bug_target_name'], 'synaptic (Ubuntu)')
        self.assertEqual(eitem['importance'], 'High')
        self.assertEqual(eitem['date_triaged'], '2017-08-21T14:50:35.930044+00:00')
        self.assertEqual(eitem['date_left_new'], '2017-08-21T14:50:35.930044+00:00')
        self.assertEqual(eitem['time_to_last_update_days'], 0.0)
        self.assertEqual(eitem['reopened'], 0)
        self.assertEqual(eitem['time_to_fix_commit'], None)
        self.assertEqual(eitem['time_worked_on'], None)
        self.assertEqual(eitem['time_to_confirm'], 0.02)
        self.assertEqual(eitem['user_login'], 'user')
        self.assertEqual(eitem['user_name'], 'Cristian Aravena Romero')
        self.assertEqual(eitem['user_joined'], '2005-06-15T02:17:43.115113+00:00')
        self.assertEqual(eitem['user_karma'], 1841)
        self.assertEqual(eitem['user_time_zone'], 'America/Santiago')
        self.assertEqual(eitem['assignee_login'], 'user')
        self.assertEqual(eitem['assignee_name'], 'Cristian Aravena Romero')
        self.assertEqual(eitem['assignee_joined'], '2005-06-15T02:17:43.115113+00:00')
        self.assertEqual(eitem['assignee_karma'], 1841)
        self.assertEqual(eitem['assignee_time_zone'], 'America/Santiago')
        self.assertEqual(eitem['bug_name'], None)
        self.assertEqual(eitem['bug_id'], 1)
        self.assertEqual(eitem['latest_patch_uploaded'], None)
        self.assertEqual(eitem['security_related'], False)
        self.assertEqual(eitem['private'], False)
        self.assertEqual(eitem['users_affected_count'], 3)
        self.assertIn('description', eitem)
        self.assertEqual(eitem['tags'], [])
        self.assertEqual(eitem['date_last_updated'], '2011-10-17T21:12:59.354332+00:00')
        self.assertEqual(eitem['message_count'], 2)
        self.assertEqual(eitem['heat'], 22)
        self.assertEqual(eitem['time_to_first_attention'], None)
        self.assertEqual(eitem['activity_count'], 1)
        self.assertEqual(eitem['is_launchpad_issue'], 1)
        self.assertEqual(eitem['time_created_to_last_update_days'], 2041.07)

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['offset'], None)
        self.assertEqual(eitem['origin'], 'https://launchpad.net/mydistribution')
        self.assertEqual(eitem['tag'], 'https://launchpad.net/mydistribution')
        self.assertEqual(eitem['uuid'], 'c99e2f572749ea280b7f05cd5ea78d39bcd17a00')
        self.assertIn('title', eitem)
        self.assertEqual(eitem['web_link'], 'https://bugs.launchpad.net/mydistribution/+source/mypackage/+bug/2')
        self.assertEqual(eitem['date_created'], '2006-03-16T19:25:37.186986+00:00')
        self.assertEqual(eitem['date_incomplete'], None)
        self.assertEqual(eitem['is_complete'], False)
        self.assertEqual(eitem['status'], 'Confirmed')
        self.assertEqual(eitem['bug_target_name'], 'synaptic (Ubuntu)')
        self.assertEqual(eitem['importance'], 'Medium')
        self.assertEqual(eitem['date_triaged'], None)
        self.assertEqual(eitem['date_left_new'], '2010-11-01T00:30:06.794525+00:00')
        self.assertEqual(eitem['time_to_last_update_days'], 4175.79)
        self.assertEqual(eitem['reopened'], 1)
        self.assertEqual(eitem['time_to_fix_commit'], None)
        self.assertEqual(eitem['time_worked_on'], None)
        self.assertEqual(eitem['time_to_confirm'], 1690.21)
        self.assertEqual(eitem['user_login'], 'user')
        self.assertEqual(eitem['user_name'], 'Cristian Aravena Romero')
        self.assertEqual(eitem['user_joined'], '2005-06-15T02:17:43.115113+00:00')
        self.assertEqual(eitem['user_karma'], 1841)
        self.assertEqual(eitem['user_time_zone'], 'America/Santiago')
        self.assertEqual(eitem['bug_name'], None)
        self.assertEqual(eitem['bug_id'], 2)
        self.assertEqual(eitem['latest_patch_uploaded'], None)
        self.assertEqual(eitem['security_related'], False)
        self.assertEqual(eitem['private'], False)
        self.assertEqual(eitem['users_affected_count'], 6)
        self.assertEqual(eitem['description'], 'Posting here what gnome says ')
        self.assertEqual(eitem['tags'], ['amd64', 'apport-bug', 'artful', 'wayland', 'wayland-session'])
        self.assertEqual(eitem['date_last_updated'], '2017-09-16T01:57:40.183918+00:00')
        self.assertEqual(eitem['message_count'], 14)
        self.assertEqual(eitem['heat'], 120)
        self.assertEqual(eitem['time_to_first_attention'], None)
        self.assertEqual(eitem['activity_count'], 1)
        self.assertEqual(eitem['is_launchpad_issue'], 1)
        self.assertEqual(eitem['time_created_to_last_update_days'], 25.48)

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['offset'], None)
        self.assertEqual(eitem['origin'], 'https://launchpad.net/mydistribution')
        self.assertEqual(eitem['tag'], 'https://launchpad.net/mydistribution')
        self.assertEqual(eitem['uuid'], 'affb86ac0f679f9df3b4b345db330b2c3d7df49d')
        self.assertIn('title', eitem)
        self.assertEqual(eitem['web_link'], 'https://bugs.launchpad.net/mydistribution/+source/mypackage/+bug/3')
        self.assertEqual(eitem['date_created'], '2006-07-28T09:10:11.023558+00:00')
        self.assertEqual(eitem['date_incomplete'], None)
        self.assertEqual(eitem['is_complete'], False)
        self.assertEqual(eitem['status'], 'Confirmed')
        self.assertEqual(eitem['bug_target_name'], 'synaptic (Ubuntu)')
        self.assertEqual(eitem['importance'], 'Medium')
        self.assertEqual(eitem['date_triaged'], None)
        self.assertEqual(eitem['date_left_new'], None)
        self.assertEqual(eitem['reopened'], 1)
        self.assertEqual(eitem['time_to_fix_commit'], None)
        self.assertEqual(eitem['time_worked_on'], None)
        self.assertEqual(eitem['time_to_confirm'], 1681.92)
        self.assertEqual(eitem['user_login'], 'user')
        self.assertEqual(eitem['user_name'], 'Cristian Aravena Romero')
        self.assertEqual(eitem['user_joined'], '2005-06-15T02:17:43.115113+00:00')
        self.assertEqual(eitem['user_karma'], 1841)
        self.assertEqual(eitem['user_time_zone'], 'America/Santiago')
        self.assertEqual(eitem['bug_name'], None)
        self.assertEqual(eitem['bug_id'], 3)
        self.assertEqual(eitem['latest_patch_uploaded'], None)
        self.assertEqual(eitem['security_related'], False)
        self.assertEqual(eitem['private'], False)
        self.assertEqual(eitem['users_affected_count'], 3)
        self.assertEqual(eitem['tags'], [])
        self.assertEqual(eitem['date_last_updated'], '2011-10-17T21:12:59.354332+00:00')
        self.assertEqual(eitem['message_count'], 13)
        self.assertEqual(eitem['heat'], 22)
        self.assertEqual(eitem['time_to_first_attention'], None)
        self.assertEqual(eitem['activity_count'], 0)
        self.assertEqual(eitem['is_launchpad_issue'], 1)
        self.assertEqual(eitem['time_created_to_last_update_days'], 2041.07)

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(eitem['offset'], None)
        self.assertEqual(eitem['origin'], 'https://launchpad.net/ubuntu/+source/python-defaults')
        self.assertEqual(eitem['tag'], 'https://launchpad.net/ubuntu/+source/python-defaults')
        self.assertEqual(eitem['uuid'], '252bb297c901a660f8a9752997840a6882bbd2f2')
        self.assertIn('title', eitem)
        self.assertEqual(eitem['web_link'], 'https://bugs.launchpad.net/ubuntu/+source/python-defaults/+bug/9560')
        self.assertEqual(eitem['date_created'], '2004-10-28T20:45:27+00:00')
        self.assertEqual(eitem['date_incomplete'], None)
        self.assertEqual(eitem['is_complete'], True)
        self.assertEqual(eitem['status'], 'Fix Released')
        self.assertEqual(eitem['bug_target_name'], 'python-defaults (Ubuntu)')
        self.assertEqual(eitem['importance'], 'Medium')
        self.assertEqual(eitem['date_triaged'], None)
        self.assertEqual(eitem['date_left_new'], None)
        self.assertEqual(eitem['time_to_close_days'], 0.0)
        self.assertEqual(eitem['reopened'], 0)
        self.assertEqual(eitem['time_to_fix_commit'], None)
        self.assertEqual(eitem['time_worked_on'], None)
        self.assertEqual(eitem['time_to_confirm'], None)
        self.assertEqual(eitem['user_login'], 'debzilla')
        self.assertEqual(eitem['user_name'], 'Debian Bug Importer')
        self.assertEqual(eitem['user_joined'], '2006-01-13T12:51:52.192089+00:00')
        self.assertEqual(eitem['user_karma'], 0)
        self.assertEqual(eitem['user_time_zone'], 'UTC')
        self.assertEqual(eitem['assignee_login'], 'doko')
        self.assertEqual(eitem['assignee_name'], 'Matthias Klose')
        self.assertEqual(eitem['assignee_joined'], '2005-06-15T02:17:43.115113+00:00')
        self.assertEqual(eitem['assignee_karma'], 261688)
        self.assertEqual(eitem['assignee_time_zone'], 'Europe/Berlin')
        self.assertEqual(eitem['bug_name'], None)
        self.assertEqual(eitem['bug_id'], 9560)
        self.assertEqual(eitem['latest_patch_uploaded'], None)
        self.assertEqual(eitem['security_related'], False)
        self.assertEqual(eitem['private'], False)
        self.assertEqual(eitem['users_affected_count'], 0)
        self.assertIn('description', eitem)
        self.assertEqual(eitem['tags'], [])
        self.assertEqual(eitem['date_last_updated'], '2004-10-28T20:45:27+00:00')
        self.assertEqual(eitem['message_count'], 2)
        self.assertEqual(eitem['heat'], 6)
        self.assertEqual(eitem['time_to_first_attention'], 13.12)
        self.assertEqual(eitem['activity_count'], 0)
        self.assertEqual(eitem['is_launchpad_issue'], 1)
        self.assertEqual(eitem['time_created_to_last_update_days'], 0.0)

    def test_raw_to_enrich_sorting_hat(self):
        """Test enrich with SortingHat"""

        result = self._test_raw_to_enrich(sortinghat=True)
        self.assertEqual(result['raw'], 4)
        self.assertEqual(result['enrich'], 4)

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
                self.assertIn('owner_data_gender', source)
                self.assertIn('owner_data_gender_acc', source)
                self.assertIn('owner_data_multi_org_names', source)
                self.assertIn('owner_data_name', source)
                self.assertIn('owner_data_user_name', source)
                self.assertIn('owner_data_uuid', source)

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        self.assertEqual(result['raw'], 4)
        self.assertEqual(result['enrich'], 4)

        res = requests.get(self.es_con + "/" + self.enrich_index + "/_search", verify=False)
        for eitem in res.json()['hits']['hits']:
            self.assertEqual(eitem['_source']['project'], "Main")

    def test_perceval_params(self):
        """Test the extraction of perceval params from an URL"""

        url1 = "https://launchpad.net/ubuntu/+source/python-defaults"
        url2 = "https://launchpad.net/launchpad"
        expected_params = [
            ['ubuntu', '--package', 'python-defaults'],
            ['launchpad']
        ]
        self.assertListEqual(LaunchpadOcean.get_perceval_params_from_url(url1), expected_params[0])
        self.assertListEqual(LaunchpadOcean.get_perceval_params_from_url(url2), expected_params[1])

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

    def test_refresh_identities(self):
        """Test refresh identities"""

        result = self._test_refresh_identities()
        # ... ?

    def test_refresh_project(self):
        """Test refresh project field for all sources"""

        result = self._test_refresh_project()
        # ... ?


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
