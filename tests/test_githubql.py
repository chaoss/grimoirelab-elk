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
#     Valerio Cosentino <valcos@bitergia.com>
#     Miguel Ángel Fernández <mafesan@bitergia.com>
#

import logging
import time
import unittest

from base import TestBaseBackend
from grimoire_elk.enriched.utils import REPO_LABELS, anonymize_url
from grimoire_elk.raw.githubql import GitHubQLOcean
from grimoire_elk.enriched.githubql import logger


class TestGitHubQL(TestBaseBackend):
    """Test GitHubQL backend"""

    connector = "githubql"
    ocean_index = "test_" + connector
    enrich_index = "test_" + connector + "_enrich"

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
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'LabeledEvent')
        self.assertEqual(eitem['created_at'], '2020-04-07T11:21:12Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'Issue 2')
        self.assertEqual(eitem['label'], 'bug')
        self.assertEqual(eitem['label_description'], "Something isn't working")
        self.assertTrue(eitem['label_is_default'])
        self.assertEqual(eitem['label_created_at'], '2020-04-07T10:30:46Z')
        self.assertEqual(eitem['label_updated_at'], '2020-04-07T10:30:46Z')
        self.assertEqual(eitem['reporter_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['reporter_name'], 'Unknown')
        self.assertEqual(eitem['reporter_user_name'], 'valeriocos')
        self.assertIsNone(eitem['reporter_domain'])

        item = self.items[1]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'UnlabeledEvent')
        self.assertEqual(eitem['created_at'], '2020-04-09T11:21:32Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'Issue 2')
        self.assertEqual(eitem['label'], 'bug')
        self.assertEqual(eitem['label_description'], "Something isn't working")
        self.assertTrue(eitem['label_is_default'])
        self.assertEqual(eitem['label_created_at'], '2020-04-07T10:30:46Z')
        self.assertEqual(eitem['label_updated_at'], '2020-04-07T10:30:46Z')
        self.assertEqual(eitem['reporter_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['reporter_name'], 'Unknown')
        self.assertEqual(eitem['reporter_user_name'], 'valeriocos')
        self.assertIsNone(eitem['reporter_domain'])

        item = self.items[2]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'AddedToProjectEvent')
        self.assertEqual(eitem['created_at'], '2020-04-07T13:22:25Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'Issue 2')
        self.assertEqual(eitem['board_column'], 'backlog')
        self.assertEqual(eitem['board_name'], "my fantastic board")
        self.assertEqual(eitem['board_url'], 'https://github.com/valeriocos/test-issues-update/projects/1')
        self.assertEqual(eitem['board_created_at'], '2020-04-07T11:41:41Z')
        self.assertEqual(eitem['board_updated_at'], '2020-04-09T18:54:08Z')
        self.assertIsNone(eitem['board_closed_at'])
        self.assertEqual(eitem['board_state'], 'open')
        self.assertEqual(eitem['reporter_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['reporter_name'], 'Unknown')
        self.assertEqual(eitem['reporter_user_name'], 'valeriocos')
        self.assertIsNone(eitem['reporter_domain'])

        item = self.items[3]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'MovedColumnsInProjectEvent')
        self.assertEqual(eitem['created_at'], '2020-04-07T13:22:51Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'Issue 2')
        self.assertEqual(eitem['board_column'], 'analysis')
        self.assertEqual(eitem['board_previous_column'], 'backlog')
        self.assertEqual(eitem['board_name'], "my fantastic board")
        self.assertEqual(eitem['board_url'], 'https://github.com/valeriocos/test-issues-update/projects/1')
        self.assertEqual(eitem['board_created_at'], '2020-04-07T11:41:41Z')
        self.assertEqual(eitem['board_updated_at'], '2020-04-09T18:54:08Z')
        self.assertIsNone(eitem['board_closed_at'])
        self.assertEqual(eitem['board_state'], 'open')
        self.assertEqual(eitem['reporter_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['reporter_name'], 'Unknown')
        self.assertEqual(eitem['reporter_user_name'], 'valeriocos')
        self.assertIsNone(eitem['reporter_domain'])

        item = self.items[4]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'RemovedFromProjectEvent')
        self.assertEqual(eitem['created_at'], '2020-04-09T18:54:08Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'Issue 2')
        self.assertEqual(eitem['board_column'], 'done')
        self.assertEqual(eitem['board_name'], "my fantastic board")
        self.assertEqual(eitem['board_url'], 'https://github.com/valeriocos/test-issues-update/projects/1')
        self.assertEqual(eitem['board_created_at'], '2020-04-07T11:41:41Z')
        self.assertEqual(eitem['board_updated_at'], '2020-04-09T18:54:08Z')
        self.assertIsNone(eitem['board_closed_at'])
        self.assertEqual(eitem['board_state'], 'open')
        self.assertEqual(eitem['reporter_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['reporter_name'], 'Unknown')
        self.assertEqual(eitem['reporter_user_name'], 'valeriocos')
        self.assertIsNone(eitem['reporter_domain'])

        item = self.items[5]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'CrossReferencedEvent')
        self.assertEqual(eitem['created_at'], '2020-04-07T10:36:16Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'First issue')
        self.assertFalse(eitem['reference_cross_repo'])
        self.assertFalse(eitem['reference_will_close_target'])
        self.assertEqual(eitem['reference_source_type'], "Issue")
        self.assertEqual(eitem['reference_source_number'], 2)
        self.assertEqual(eitem['reference_source_repo'], 'valeriocos/test-issues-update')
        self.assertEqual(eitem['reference_source_created_at'], '2020-04-07T10:35:56Z')
        self.assertEqual(eitem['reference_source_updated_at'], '2020-04-07T11:21:32Z')
        self.assertIsNone(eitem['reference_source_closed_at'])
        self.assertFalse(eitem['reference_source_closed'])
        self.assertIsNone(eitem['reference_source_merged'])
        self.assertEqual(eitem['reporter_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['reporter_name'], 'Unknown')
        self.assertEqual(eitem['reporter_user_name'], 'valeriocos')
        self.assertIsNone(eitem['reporter_domain'])

        item = self.items[6]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'ClosedEvent')
        self.assertEqual(eitem['created_at'], '2020-04-12T07:51:05Z')
        self.assertEqual(eitem['author_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'valeriocos')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/valeriocos/test-issues-update')
        self.assertEqual(eitem['title'], 'First issue')
        self.assertEqual(eitem['closer_type'], "PullRequest")
        self.assertEqual(eitem['closer_number'], 3)
        self.assertEqual(eitem['closer_repo'], "valeriocos/test-issues-update")
        self.assertEqual(eitem['closer_created_at'], '2020-04-12T07:50:25Z')
        self.assertEqual(eitem['closer_updated_at'], '2020-04-12T07:51:05Z')
        self.assertEqual(eitem['closer_closed_at'], '2020-04-12T07:51:05Z')
        self.assertTrue(eitem['closer_closed'])
        self.assertTrue(eitem['closer_merged'])
        self.assertEqual(eitem['reporter_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['reporter_name'], 'Unknown')
        self.assertEqual(eitem['reporter_user_name'], 'valeriocos')
        self.assertIsNone(eitem['reporter_domain'])
        self.assertEqual(eitem['submitter_uuid'], 'ee5d85148ccdeab3efc341cb12fc70ae6b3236ae')
        self.assertEqual(eitem['submitter_name'], 'Unknown')
        self.assertEqual(eitem['submitter_user_name'], 'valeriocos')
        self.assertIsNone(eitem['submitter_domain'])
        self.assertEqual(eitem['closer_pull_submitter'], 'valeriocos')

        item = self.items[13]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'MergedEvent')
        self.assertEqual(eitem['created_at'], '2020-10-23T18:45:14Z')
        self.assertEqual(eitem['author_uuid'], '09c94d1fdc9523c9db2244d7ae4d1ab9603cd683')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'zhquan')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/zhquan/test-merged-event')
        self.assertEqual(eitem['title'], 'Initial directory setup')
        self.assertTrue(eitem['merge_closed'])
        self.assertEqual(eitem['merge_closed_at'], '2020-10-23T18:45:14Z')
        self.assertEqual(eitem['merge_created_at'], '2020-10-23T18:45:06Z')
        self.assertTrue(eitem['merge_merged'])
        self.assertEqual(eitem['merge_merged_at'], '2020-10-23T18:45:14Z')
        self.assertEqual(eitem['merge_updated_at'], '2020-10-23T18:45:14Z')
        self.assertEqual(eitem['merge_url'], 'https://github.com/zhquan/test-merged-event/pull/3')
        self.assertNotIn('merge_approved', eitem)

        item = self.items[15]
        eitem = enrich_backend.get_rich_item(item)
        self.assertEqual(item['category'], 'event')
        self.assertEqual(eitem['event_type'], 'PullRequestReview')
        self.assertEqual(eitem['created_at'], '2021-08-06T10:33:30Z')
        self.assertEqual(eitem['author_uuid'], '09c94d1fdc9523c9db2244d7ae4d1ab9603cd683')
        self.assertEqual(eitem['author_name'], 'Unknown')
        self.assertEqual(eitem['author_user_name'], 'zhquan')
        self.assertIsNone(eitem['author_domain'])
        self.assertEqual(eitem['repository'], 'https://github.com/zhquan/test-merged-event')
        self.assertEqual(eitem['title'], 'commit')
        self.assertTrue(eitem['merge_closed'])
        self.assertEqual(eitem['merge_closed_at'], '2021-08-06T10:34:32Z')
        self.assertEqual(eitem['merge_created_at'], '2021-08-03T10:52:58Z')
        self.assertTrue(eitem['merge_merged'])
        self.assertEqual(eitem['merge_merged_at'], '2021-08-06T10:34:31Z')
        self.assertEqual(eitem['merge_updated_at'], '2021-08-06T10:34:32Z')
        self.assertEqual(eitem['merge_url'], 'https://github.com/zhquan/test-merged-event/pull/442')
        self.assertEqual(eitem['merge_state'], 'APPROVED')
        self.assertEqual(eitem['merge_approved'], 1)

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
        self.assertGreater(result['raw'], 0)
        self.assertGreater(result['enrich'], 0)
        self.assertEqual(result['raw'], result['enrich'])

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
            if 'reporter_uuid' in source:
                self.assertIn('reporter_domain', source)
                self.assertIn('reporter_gender', source)
                self.assertIn('reporter_gender_acc', source)
                self.assertIn('reporter_org_name', source)
                self.assertIn('reporter_bot', source)
                self.assertIn('reporter_multi_org_names', source)
            if 'submitter_uuid' in source:
                self.assertIn('submitter_domain', source)
                self.assertIn('submitter_gender', source)
                self.assertIn('submitter_gender_acc', source)
                self.assertIn('submitter_org_name', source)
                self.assertIn('submitter_bot', source)
                self.assertIn('submitter_multi_org_names', source)

    def test_raw_to_enrich_projects(self):
        """Test enrich with Projects"""

        result = self._test_raw_to_enrich(projects=True)
        self.assertGreater(result['raw'], 0)
        self.assertGreater(result['enrich'], 0)
        self.assertEqual(result['raw'], result['enrich'])

        enrich_backend = self.connectors[self.connector][2]()

        url = self.es_con + "/" + self.enrich_index + "/_search"
        response = enrich_backend.requests.get(url, verify=False).json()
        for hit in response['hits']['hits']:
            source = hit['_source']
            self.assertEqual(source['project'], 'Main')
            self.assertEqual(source['project_1'], 'Main')

    def test_refresh_identities(self):
        """Test refresh identities"""

        result = self._test_refresh_identities()
        # ... ?

    def test_perceval_params(self):
        """Test the extraction of perceval params from an URL"""

        url = "https://github.com/chaoss/grimoirelab-perceval"
        expected_params = [
            'chaoss', 'grimoirelab-perceval'
        ]
        self.assertListEqual(GitHubQLOcean.get_perceval_params_from_url(url), expected_params)

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

    def test_duration_analysis(self):
        """Test that the geolocation study works correctly"""

        study, ocean_backend, enrich_backend = self._test_study('enrich_duration_analysis')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_duration_analysis":
                study(ocean_backend, enrich_backend,
                      start_event_type="UnlabeledEvent", target_attr="label",
                      fltr_attr="label", fltr_event_types=["LabeledEvent"])

            self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.githubql:[githubql] Duration analysis '
                                           'starting study %s/test_githubql_enrich'
                             % anonymize_url(self.es_con))
            self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.githubql:[githubql] Duration analysis '
                                            'ending study %s/test_githubql_enrich'
                             % anonymize_url(self.es_con))

        time.sleep(5)  # HACK: Wait until github enrich index has been written
        items = [item for item in enrich_backend.fetch() if item['event_type'] == 'UnlabeledEvent']
        self.assertEqual(len(items), 1)
        for item in items:
            self.assertEqual(item['previous_event_uuid'], 'f371d54454d297f86f08ab52a440ae5f9e4afeb1')
            self.assertEqual(item['duration_from_previous_event'], 2.0)

    def test_reference_analysis(self):
        """Test that the cross reference study works correctly"""

        study, ocean_backend, enrich_backend = self._test_study('enrich_reference_analysis')

        with self.assertLogs(logger, level='INFO') as cm:

            if study.__name__ == "enrich_reference_analysis":
                study(ocean_backend, enrich_backend)

            self.assertEqual(cm.output[0], 'INFO:grimoire_elk.enriched.githubql:[githubql] Cross reference analysis '
                                           'starting study %s/test_githubql_enrich'
                             % anonymize_url(self.es_con))
            self.assertEqual(cm.output[-1], 'INFO:grimoire_elk.enriched.githubql:[githubql] Cross reference analysis '
                                            'ending study %s/test_githubql_enrich'
                             % anonymize_url(self.es_con))

        time.sleep(5)  # HACK: Wait until github enrich index has been written
        referenced_items = []
        for item in enrich_backend.fetch():
            if ('referenced_by_issues' or 'referenced_by_prs') in item.keys():
                referenced_items.append(item)

        self.assertEqual(len(referenced_items), 7)

        for item in referenced_items:
            self.assertIn('referenced_by_issues', item)
            self.assertIn('referenced_by_prs', item)
            self.assertIn('referenced_by_merged_prs', item)
            self.assertIn('referenced_by_external_issues', item)
            self.assertIn('referenced_by_external_prs', item)
            self.assertIn('referenced_by_external_merged_prs', item)

            ref_issues = item['referenced_by_issues']
            self.assertEqual(len(ref_issues), 1)

            ref = ref_issues[0]
            self.assertEqual(ref, 'https://github.com/valeriocos/test-issues-update/issues/2')

            ref_prs = item['referenced_by_prs']
            self.assertEqual(len(ref_prs), 1)

            ref = ref_prs[0]
            self.assertEqual(ref, 'https://github.com/valeriocos/test-issues-update/pull/3')

            ref_merged_prs = item['referenced_by_merged_prs']
            self.assertEqual(len(ref_merged_prs), 1)

            ref = ref_merged_prs[0]
            self.assertEqual(ref, 'https://github.com/valeriocos/test-issues-update/pull/3')

            ref_ext_issues = item['referenced_by_external_issues']
            self.assertEqual(len(ref_ext_issues), 0)

            ref_ext_prs = item['referenced_by_external_prs']
            self.assertEqual(len(ref_ext_prs), 0)

            ref_ext_merged_prs = item['referenced_by_external_merged_prs']
            self.assertEqual(len(ref_ext_merged_prs), 0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
