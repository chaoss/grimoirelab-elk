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

import configparser
import logging
import json
import os
import unittest

import requests

from grimoire_elk.elastic import ElasticSearch
from grimoire_elk.elastic_items import (ElasticItems,
                                        logger)
from grimoirelab_toolkit.datetime import str_to_datetime
from grimoire_elk.raw.kitsune import KitsuneOcean
from grimoire_elk.raw.git import GitOcean
from grimoire_elk.raw.meetup import MeetupOcean
from perceval.backends.mozilla.kitsune import Kitsune
from perceval.backends.core.git import Git
from perceval.backends.core.meetup import Meetup


CONFIG_FILE = 'tests.conf'


def read_file(filename, mode='r'):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), filename), mode) as f:
        content = f.read()
    return content


class TestElasticItems(unittest.TestCase):
    """Unit tests for ElasticItems class"""

    target_index = "elastic_items_test"

    @classmethod
    def setUpClass(cls):
        cls.perceval_backend = Git('http://example.com', '/tmp/foo')
        cls.config = configparser.ConfigParser()
        cls.config.read(CONFIG_FILE)
        cls.es_con = dict(cls.config.items('ElasticSearch'))['url']

    def tearDown(self):
        target_index_url = self.es_con + "/" + self.target_index
        requests.delete(target_index_url, verify=False)

    def test_init(self):
        """Test whether attributes are initialized"""

        eitems = ElasticItems(self.perceval_backend)
        self.assertIsNone(eitems.from_date)
        self.assertIsNone(eitems.offset)
        self.assertIsInstance(eitems.requests, requests.Session)

        from_date = str_to_datetime("2019-01-01")
        eitems = ElasticItems(self.perceval_backend, from_date=from_date)
        self.assertEqual(eitems.from_date, from_date)
        self.assertIsNone(eitems.offset)
        self.assertIsInstance(eitems.requests, requests.Session)
        self.assertFalse(eitems.requests.verify)

        offset = 10
        eitems = ElasticItems(self.perceval_backend, offset=offset)
        self.assertIsNone(eitems.from_date)
        self.assertEqual(eitems.offset, offset)
        self.assertIsInstance(eitems.requests, requests.Session)
        self.assertFalse(eitems.requests.verify)

        insecure = False
        eitems = ElasticItems(self.perceval_backend, insecure=insecure)
        self.assertIsNone(eitems.from_date)
        self.assertIsNone(eitems.offset)
        self.assertIsInstance(eitems.requests, requests.Session)
        self.assertTrue(eitems.requests.verify)

    def test_get_repository_filter_raw(self):
        """Test whether the repository filter raw works properly"""

        expected_filter = {
            'name': 'origin',
            'value': 'http://example.com'
        }
        eitems = ElasticItems(self.perceval_backend)
        fltr = eitems.get_repository_filter_raw()
        self.assertDictEqual(fltr, expected_filter)

        expected_filter = {
            'name': 'tag',
            'value': 'test'
        }
        perceval_backend_meetup = Meetup('mygroup', 'aaaa', tag='test')
        eitems = MeetupOcean(perceval_backend_meetup)
        fltr = eitems.get_repository_filter_raw()
        self.assertDictEqual(fltr, expected_filter)

        expected_filter = {}
        perceval_backend_meetup = Meetup('mygroup', 'aaaa', tag='https://meetup.com/')
        eitems = MeetupOcean(perceval_backend_meetup)
        fltr = eitems.get_repository_filter_raw()
        self.assertDictEqual(fltr, expected_filter)

    def test_get_field_date(self):
        """Test whether the field date is correctly returned"""

        eitems = ElasticItems(self.perceval_backend)
        self.assertEqual(eitems.get_field_date(), 'metadata__updated_on')

    def test_get_incremental_date(self):
        """Test whether the incremental date is correctly returned"""

        eitems = ElasticItems(self.perceval_backend)
        self.assertEqual(eitems.get_incremental_date(), 'metadata__timestamp')

    def test_set_projects_json_repo(self):
        """Test whether the projects json repo is correctly set"""

        expected_repo = "http://example.com --filter-raw=data.commit:51a3b654f"

        eitems = ElasticItems(self.perceval_backend)
        self.assertIsNone(eitems.projects_json_repo)
        eitems.set_projects_json_repo(expected_repo)
        self.assertEqual(eitems.projects_json_repo, expected_repo)

    def test_set_repo_labels(self):
        """Test whether the repo labels are correctly set"""

        expected_labels = ["A", "B", "C"]

        eitems = ElasticItems(self.perceval_backend)
        self.assertIsNone(eitems.repo_labels)
        eitems.set_repo_labels(expected_labels)
        self.assertListEqual(eitems.repo_labels, expected_labels)

    def test_extract_repo_labels(self):
        """Test whether the labels are correctly extracted from a URL repo"""

        eitems = ElasticItems(self.perceval_backend)
        processed_repo, label_lst = eitems.extract_repo_labels("http://example.com --labels=[A, B, C]")

        self.assertEqual(processed_repo, "http://example.com")
        self.assertListEqual(label_lst, ['A', 'B', 'C'])

    def test_set_filter_raw(self):
        """Test whether the filter raw is properly set"""

        ei = ElasticItems(None)

        filter_raws = [
            "data.product:Firefox, for Android,data.component:Logins, Passwords and Form Fill",
            "data.product:Add-on SDK",
            "data.product:Add-on SDK,    data.component:Documentation",
            "data.product:Add-on SDK, data.component:General",
            "data.product:addons.mozilla.org Graveyard,       data.component:API",
            "data.product:addons.mozilla.org Graveyard,   data.component:Add-on Builder",
            "data.product:Firefox for Android,data.component:Build Config & IDE Support",
            "data.product:Firefox for Android,data.component:Logins, Passwords and Form Fill",
            "data.product:Mozilla Localizations,data.component:nb-NO / Norwegian Bokm\u00e5l",
            "data.product:addons.mozilla.org Graveyard,data.component:Add-on Validation"
        ]

        expected = [
            [
                {
                    "name": "data.product",
                    "value": "Firefox, for Android"
                },
                {
                    "name": "data.component",
                    "value": "Logins, Passwords and Form Fill"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Add-on SDK"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Add-on SDK"
                },
                {
                    "name": "data.component",
                    "value": "Documentation"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Add-on SDK"
                },
                {
                    "name": "data.component",
                    "value": "General"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "addons.mozilla.org Graveyard"
                },
                {
                    "name": "data.component",
                    "value": "API"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "addons.mozilla.org Graveyard"
                },
                {
                    "name": "data.component",
                    "value": "Add-on Builder"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Firefox for Android"
                },
                {
                    "name": "data.component",
                    "value": "Build Config & IDE Support"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Firefox for Android"
                },
                {
                    "name": "data.component",
                    "value": "Logins, Passwords and Form Fill"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Mozilla Localizations"
                },
                {
                    "name": "data.component",
                    "value": "nb-NO / Norwegian Bokm\u00e5l"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "addons.mozilla.org Graveyard"
                },
                {
                    "name": "data.component",
                    "value": "Add-on Validation"
                }
            ]
        ]

        for i, filter_raw in enumerate(filter_raws):
            ei.set_filter_raw(filter_raw)

            self.assertDictEqual(ei.filter_raw_dict[0], expected[i][0])

            if len(ei.filter_raw_dict) > 1:
                self.assertDictEqual(ei.filter_raw_dict[1], expected[i][1])

    def test_get_connector_name(self):
        """Test whether the connector name is correctly returned"""

        eitems = ElasticItems(self.perceval_backend)
        self.assertIsNone(eitems.get_connector_name())

        eitems = GitOcean(self.perceval_backend)
        self.assertEqual(eitems.get_connector_name(), 'git')

    def test_set_cfg_section_name(self):
        """Test whether the section name is correctly set"""

        expected_section = "git"

        eitems = ElasticItems(self.perceval_backend)
        self.assertIsNone(eitems.cfg_section_name)
        eitems.set_cfg_section_name(expected_section)
        self.assertEqual(eitems.cfg_section_name, expected_section)

    def test_set_from_date(self):
        """Test whether the from date is correctly set"""

        expected_from_date = str_to_datetime("2019-01-01")

        eitems = ElasticItems(self.perceval_backend)
        self.assertIsNone(eitems.from_date)
        eitems.set_from_date(expected_from_date)
        self.assertEqual(eitems.from_date, expected_from_date)

    def test_fetch(self):
        """Test whether the fetch method properly works"""

        perceval_backend = Git('/tmp/perceval_mc84igfc/gittest', '/tmp/foo')
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)

        # Load items
        items = json.loads(read_file('data/git.json'))
        ocean = GitOcean(perceval_backend)
        ocean.elastic = elastic
        ocean.feed_items(items)

        eitems = ElasticItems(perceval_backend)
        eitems.scroll_size = 2
        eitems.elastic = elastic

        items = [ei for ei in eitems.fetch()]
        self.assertEqual(len(items), 9)

    def test_fetch_from_date(self):
        """Test whether the fetch method with from_date properly works"""

        perceval_backend = Git('/tmp/perceval_mc84igfc/gittest', '/tmp/foo')
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)

        # Load items
        items = json.loads(read_file('data/git.json'))
        ocean = GitOcean(perceval_backend)
        ocean.elastic = elastic
        ocean.feed_items(items)

        # Fetch total items
        eitems = ElasticItems(perceval_backend)
        eitems.elastic = elastic
        items = [ei for ei in eitems.fetch()]
        self.assertEqual(len(items), 9)

        # Fetch with from date
        from_date = str_to_datetime("2018-02-09T08:33:22.699+00:00")
        eitems = ElasticItems(perceval_backend, from_date=from_date)
        eitems.elastic = elastic
        items = [ei for ei in eitems.fetch()]
        self.assertEqual(len(items), 2)

    def test_fetch_from_offset(self):
        """Test whether the fetch method with offset properly works"""

        perceval_backend = Kitsune('http://example.com')
        elastic = ElasticSearch(self.es_con, self.target_index, KitsuneOcean.mapping)

        # Load items
        items = json.loads(read_file('data/kitsune.json'))
        ocean = KitsuneOcean(perceval_backend)
        ocean.elastic = elastic
        ocean.feed_items(items)

        # Fetch total items
        eitems = ElasticItems(perceval_backend)
        eitems.elastic = elastic
        items = [ei for ei in eitems.fetch()]
        self.assertEqual(len(items), 4)

        # Fetch with offset
        eitems = ElasticItems(perceval_backend, offset=2)
        eitems.elastic = elastic
        items = [ei for ei in eitems.fetch()]
        self.assertEqual(len(items), 2)

    def test_fetch_no_results(self):
        """Test whether a message is logged when no results are found"""

        perceval_backend = Git('/tmp/perceval_mc84igfc/gittest-not_found', '/tmp/foo')
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)

        # Load items
        items = json.loads(read_file('data/git.json'))
        ocean = GitOcean(perceval_backend)
        ocean.elastic = elastic
        ocean.feed_items(items)

        eitems = ElasticItems(perceval_backend)
        eitems.elastic = elastic

        with self.assertLogs(logger, level='DEBUG') as cm:
            items = [ei for ei in eitems.fetch()]
            self.assertEqual(len(items), 0)
            self.assertRegex(cm.output[-2], 'DEBUG:grimoire_elk.elastic_items:No results found.*')
            self.assertRegex(cm.output[-1], 'DEBUG:grimoire_elk.elastic_items:Releasing scroll_id=*')

    def test_fetch_empty(self):
        """Test whether the fetch method returns an empty list when the index is empty"""

        eitems = ElasticItems(self.perceval_backend)
        items = [ei for ei in eitems.fetch()]
        self.assertEqual(items, [])

    def test_fetch_filter_raw(self):
        """Test whether the fetch with filter raw properly works"""

        perceval_backend = Git('/tmp/perceval_mc84igfc/gittest', '/tmp/foo')
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)

        # Load items
        items = json.loads(read_file('data/git.json'))
        ocean = GitOcean(perceval_backend)
        ocean.elastic = elastic
        ocean.feed_items(items)

        eitems = ElasticItems(perceval_backend)
        eitems.set_filter_raw("data.commit:87783129c3f00d2c81a3a8e585eb86a47e39891a")
        eitems.elastic = elastic
        items = [ei for ei in eitems.fetch()]
        self.assertEqual(len(items), 1)

    def test_get_elastic_items(self):
        """Test whether the elastic method works properly"""

        perceval_backend = Git('/tmp/perceval_mc84igfc/gittest', '/tmp/foo')
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)

        # Load items
        items = json.loads(read_file('data/git.json'))
        ocean = GitOcean(perceval_backend)
        ocean.elastic = elastic
        ocean.feed_items(items)

        eitems = ElasticItems(perceval_backend)
        eitems.elastic = elastic
        r_json = eitems.get_elastic_items()

        total = r_json['hits']['total']
        total = total['value'] if isinstance(total, dict) else total
        self.assertEqual(total, 9)

    def test_get_elastic_items_filter(self):
        """Test whether the elastic method works properly with filter"""

        perceval_backend = Git('/tmp/perceval_mc84igfc/gittest', '/tmp/foo')
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)

        # Load items
        items = json.loads(read_file('data/git.json'))
        ocean = GitOcean(perceval_backend)
        ocean.elastic = elastic
        ocean.feed_items(items)

        filter = {
            "name": "uuid",
            "value": [
                "43f217b2f678a5691fdbc5c6c5302243e79e5a90",
                "00ee6902e34b309cd05706c26e3e195a62492f60"
            ]
        }

        eitems = ElasticItems(perceval_backend)
        eitems.elastic = elastic
        r_json = eitems.get_elastic_items(_filter=filter)
        hits = r_json['hits']['hits']

        self.assertEqual(len(hits), 2)
        self.assertEqual(hits[0]['_source']['uuid'], "43f217b2f678a5691fdbc5c6c5302243e79e5a90")
        self.assertEqual(hits[1]['_source']['uuid'], "00ee6902e34b309cd05706c26e3e195a62492f60")

    def test_get_elastic_items_error(self):
        """Test whether a message is logged if an error occurs when getting items from an index"""

        items = json.loads(read_file('data/git.json'))
        perceval_backend = Git('/tmp/perceval_mc84igfc/gittest', '/tmp/foo')
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        elastic.bulk_upload(items, field_id="uuid")

        # Load items
        eitems = ElasticItems(perceval_backend)
        eitems.elastic = elastic

        with self.assertLogs(logger, level='DEBUG') as cm:
            r_json = eitems.get_elastic_items()
            self.assertIsNone(r_json)
            self.assertRegex(cm.output[-1], 'DEBUG:grimoire_elk.elastic_items:No results found from*')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    unittest.main()
