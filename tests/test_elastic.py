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
#

import configparser
import json
import os
import random
import string
import unittest
import unittest.mock

import httpretty
import requests

from grimoire_elk.elastic import (ElasticSearch,
                                  ElasticError,
                                  logger)
from grimoire_elk.raw.git import GitOcean
from grimoire_elk.raw.kitsune import KitsuneOcean
from grimoire_elk.elastic_mapping import Mapping
from grimoirelab_toolkit.datetime import unixtime_to_datetime

CONFIG_FILE = 'tests.conf'
ES_DISTRIBUTION = 'elasticsearch'
OS_DISTRIBUTION = 'opensearch'


class MockElasticSearch(ElasticSearch):

    def __init__(self, url, index, major=None, distribution=None, mock_list_alias=False):
        self.requests = requests
        self.url = url
        self.index = index
        self.major = major
        self.distribution = distribution
        self.index_url = self.url + "/" + self.index
        self.mock_list_aliases = mock_list_alias

    def list_aliases(self):
        if not self.mock_list_aliases:
            return super().list_aliases()
        else:
            return []


def read_file(filename, mode='r'):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), filename), mode) as f:
        content = f.read()
    return content


class TestElastic(unittest.TestCase):
    """Test Elastic"""

    target_index = "elastic_raw_test"

    @classmethod
    def setUpClass(cls):
        cls.config = configparser.ConfigParser()
        cls.config.read(CONFIG_FILE)
        cls.es_con = dict(cls.config.items('ElasticSearch'))['url']
        re = requests.get(cls.es_con, verify=False).json()
        cls.es_major = re['version']['number'].split('.')[0]
        cls.es_distribution = re['version'].get('distribution', ES_DISTRIBUTION)

    def tearDown(self):
        target_index_url = self.es_con + "/" + self.target_index
        requests.delete(target_index_url, verify=False)

    def test_init(self):
        """Test whether attributes are initialized"""

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        self.assertEqual(elastic.url, self.es_con)
        self.assertEqual(elastic.index, self.target_index)
        self.assertEqual(elastic.index_url, self.es_con + "/" + self.target_index)
        self.assertIsNone(elastic.aliases)

    @httpretty.activate
    def test_check_instance_es_major_6(self):
        """Test whether the major version is correctly calculated for ElasticSearch 6.x"""

        body = """{
            "name" : "44BPNNH",
            "cluster_name" : "elasticsearch",
            "cluster_uuid" : "fIa1j8AQRfSrmuhTwb9a0Q",
            "version" : {
                "number" : "6.1.0",
                "build_hash" : "c0c1ba0",
                "build_date" : "2017-12-12T12:32:54.550Z",
                "build_snapshot" : false,
                "lucene_version" : "7.1.0",
                "minimum_wire_compatibility_version" : "5.6.0",
                "minimum_index_compatibility_version" : "5.0.0"
            },
            "tagline" : "You Know, for Search"
        }"""

        es_con = "http://es6.com"
        httpretty.register_uri(httpretty.GET,
                               es_con,
                               body=body,
                               status=200)

        major, distribution = ElasticSearch.check_instance(es_con, insecure=False)
        self.assertEqual(major, '6')
        self.assertEqual(distribution, ES_DISTRIBUTION)

    @httpretty.activate
    def test_check_instance_es_major_5(self):
        """Test whether the major version is correctly calculated for ElasticSearch 5.x"""

        body = """{
            "name" : "Amber Hunt",
            "cluster_name" : "jgbarah",
            "version" : {
                "number" : "5.0.0-alpha2",
                "build_hash" : "e3126df",
                "build_date" : "2016-04-26T12:08:58.960Z",
                "build_snapshot" : false,
                "lucene_version" : "6.0.0"
            },
            "tagline" : "You Know, for Search"
        }"""

        es_con = "http://es5.com"
        httpretty.register_uri(httpretty.GET,
                               es_con,
                               body=body,
                               status=200)

        major, distribution = ElasticSearch.check_instance(es_con, insecure=False)
        self.assertEqual(major, '5')
        self.assertEqual(distribution, ES_DISTRIBUTION)

    @httpretty.activate
    def test_check_instance_os_major_1(self):
        """Test whether the major version is correctly calculated for OpenSearch 1.x"""

        body = """{
            "name" : "AAHAA",
            "cluster_name" : "docker-cluster",
            "cluster_uuid" : "XABU6jucSkG48cd7ccUTxQ",
            "version" : {
                "distribution" : "opensearch",
                "number" : "1.2.4",
                "build_type" : "tar",
                "build_hash" : "e505b10357c03ae8d26d675172402f2f2144ef0f",
                "build_date" : "2022-01-14T03:38:06.881862Z",
                "build_snapshot" : false,
                "lucene_version" : "8.10.1",
                "minimum_wire_compatibility_version" : "6.8.0",
                "minimum_index_compatibility_version" : "6.0.0-beta1"
            },
            "tagline" : "The OpenSearch Project: https://opensearch.org/"
        }"""

        os_con = "http://os1.com"
        httpretty.register_uri(httpretty.GET,
                               os_con,
                               body=body,
                               status=200)

        major, distribution = ElasticSearch.check_instance(os_con, insecure=False)
        self.assertEqual(major, '1')
        self.assertEqual(distribution, OS_DISTRIBUTION)

    @httpretty.activate
    def test_check_instance_es_major_error(self):
        """Test whether an exception is thrown when the ElasticSearch version number is not retrieved"""

        body = """{
                "name" : "Amber Hunt",
                "cluster_name" : "jgbarah",
                "version" : {
                    "build_hash" : "e3126df",
                    "build_date" : "2016-04-26T12:08:58.960Z",
                    "build_snapshot" : false,
                    "lucene_version" : "6.0.0"
                },
                "tagline" : "You Know, for Search"
            }"""

        es_con = "http://es_err.com"
        httpretty.register_uri(httpretty.GET,
                               es_con,
                               body=body,
                               status=200)

        with self.assertRaises(ElasticError):
            _, _ = ElasticSearch.check_instance(es_con, insecure=False)

    @httpretty.activate
    def test_check_instance_not_reachable(self):
        """Test whether an exception is thrown when the ElasticSearch is not reachable"""

        es_con = "http://es_err.com"
        httpretty.register_uri(httpretty.GET,
                               es_con,
                               body={},
                               status=400)

        with self.assertRaises(ElasticError):
            _ = ElasticSearch.check_instance(es_con, insecure=False)

    def test_init_clean(self):
        """Test whether the index is recreated if `clean` is True"""

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        r = elastic.requests.get(elastic.index_url)
        self.assertEqual(r.status_code, 200)

        with self.assertLogs(logger, level='INFO') as cm:
            elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping, clean=True)
            self.assertRegex(cm.output[0], 'INFO:grimoire_elk.elastic:Deleted and created index*')

        r = elastic.requests.get(elastic.index_url)
        self.assertEqual(r.status_code, 200)

    def test_init_aliases(self):
        """Test whether aliases are correctly set"""

        expected_aliases = {
            'A': {},
            'B': {}
        }

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping, aliases=["A", "B"])
        self.assertEqual(elastic.url, self.es_con)
        self.assertEqual(elastic.index, self.target_index)
        self.assertEqual(elastic.index_url, self.es_con + "/" + self.target_index)

        r = elastic.requests.get(elastic.index_url + '/_alias')
        aliases = r.json()[self.target_index]['aliases']

        self.assertDictEqual(aliases, expected_aliases)

    def test_init_duplicated_aliases(self):
        """Test whether duplicated aliases are ignored"""

        expected_aliases = {
            'A': {},
            'B': {}
        }

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping, aliases=["A", "B", "A"])
        self.assertEqual(elastic.url, self.es_con)
        self.assertEqual(elastic.index, self.target_index)
        self.assertEqual(elastic.index_url, self.es_con + "/" + self.target_index)

        r = elastic.requests.get(elastic.index_url + '/_alias')
        aliases = r.json()[self.target_index]['aliases']

        self.assertDictEqual(aliases, expected_aliases)

    def test_safe_index(self):
        """Test whether the index name is correctly defined"""

        expected_index = "good_index"
        index_name = ElasticSearch.safe_index("good_index")
        self.assertEqual(index_name, expected_index)

        expected_index = "goodindex"
        index_name = ElasticSearch.safe_index("goodindex")
        self.assertEqual(index_name, expected_index)

        expected_index = "bad_index"
        index_name = ElasticSearch.safe_index("bad/index")
        self.assertEqual(index_name, expected_index)

    def test_create_index(self):
        """Test whether an index is created"""

        expected_response = 200
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)

        response = elastic.requests.get(elastic.index_url).status_code
        self.assertEqual(response, expected_response)

    def test_create_index_clean(self):
        """Test whether an index is created"""

        ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        with self.assertLogs(logger, level='INFO') as cm:
            ElasticSearch(self.es_con, self.target_index, GitOcean.mapping, clean=True)
            self.assertRegex(cm.output[0], 'INFO:grimoire_elk.elastic:Deleted and created index*')

    @httpretty.activate
    def test_create_index_error(self):
        """Test whether an error is thrown when the index isn't created"""

        url = self.es_con + '/' + self.target_index
        httpretty.register_uri(httpretty.GET,
                               url,
                               body={},
                               status=400,
                               forcing_headers={
                                   "Content-Type": "application/json"
                               })
        httpretty.register_uri(httpretty.PUT,
                               url,
                               body={},
                               status=400,
                               forcing_headers={
                                   "Content-Type": "application/json"
                               })

        elastic = MockElasticSearch(self.es_con, self.target_index)
        with self.assertRaises(ElasticError):
            _ = elastic.create_index()

    def test_create_mappings(self):
        """Test whether a mapping is correctly created"""

        elastic = ElasticSearch(self.es_con, self.target_index)
        elastic.create_mappings(GitOcean.mapping.get_elastic_mappings(elastic.major))

        r = elastic.requests.get(elastic.index_url + '/_mapping')
        mapping = r.json()
        self.assertIsNotNone(mapping[self.target_index]['mappings'])

    @httpretty.activate
    def test_create_mappings_error(self):
        """Test whether an error is thrown when the mapping cannot be created"""

        elastic = MockElasticSearch(self.es_con, self.target_index)

        if not elastic.is_legacy():
            url = elastic.index_url + "/_mapping"
        else:
            url = elastic.index_url + "/items/_mapping"

        httpretty.register_uri(httpretty.PUT,
                               url,
                               body={},
                               status=400,
                               forcing_headers={
                                   "Content-Type": "application/json"
                               })

        with self.assertLogs(logger, level='ERROR') as cm:
            elastic.create_mappings(GitOcean.mapping.get_elastic_mappings(elastic.major))
            self.assertRegex(cm.output[0], 'ERROR:grimoire_elk.elastic:Error creating ES mappings*')

    @httpretty.activate
    def test_create_mappings_templates_error(self):
        """Test whether an error is thrown when the templates cannot be created"""

        elastic = MockElasticSearch(self.es_con, self.target_index)

        if not elastic.is_legacy():
            url = elastic.index_url + "/_mapping"
        else:
            url = elastic.index_url + "/items/_mapping"

        httpretty.register_uri(httpretty.PUT,
                               url,
                               body={},
                               status=400,
                               forcing_headers={
                                   "Content-Type": "application/json"
                               })

        with self.assertLogs(logger, level='ERROR') as cm:
            elastic.create_mappings(Mapping.get_elastic_mappings(elastic.major))
            self.assertRegex(cm.output[0], 'ERROR:grimoire_elk.elastic:Can\'t add mapping*')

    def test_all_es_aliases(self):
        """Test whether all aliases are correctly returned"""

        expected_aliases = ['A', 'B', 'C']
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping, aliases=['A', 'B', 'C'])
        aliases = elastic.all_es_aliases()

        for ea in expected_aliases:
            self.assertIn(ea, aliases)

    @httpretty.activate
    def test_all_es_aliases_error(self):
        """Test whether a warning message is logged when the aliases are not returned"""

        url = self.es_con + "/_aliases"
        httpretty.register_uri(httpretty.GET,
                               url,
                               body={},
                               status=400,
                               forcing_headers={
                                   "Content-Type": "application/json"
                               })

        elastic = MockElasticSearch(self.es_con, self.target_index)
        with self.assertLogs(logger, level='WARNING') as cm:
            _ = elastic.all_es_aliases()
            self.assertRegex(cm.output[0],
                             'WARNING:grimoire_elk.elastic:Something went wrong when retrieving aliases*')

    def test_list_aliases(self):
        """Test whether the aliases of a given index are correctly listed"""

        expected_aliases = {
            'A': {},
            'B': {},
            'C': {}
        }

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping, aliases=['A', 'B', 'C'])
        aliases = elastic.list_aliases()
        self.assertDictEqual(aliases, expected_aliases)

    @httpretty.activate
    def test_list_aliases_warning(self):
        """Test whether a warning message is logged when the aliases for a given index are not returned"""

        url = self.es_con + '/' + self.target_index + "/_alias"
        httpretty.register_uri(httpretty.GET,
                               url,
                               body={},
                               status=400,
                               forcing_headers={
                                   "Content-Type": "application/json"
                               })

        elastic = MockElasticSearch(self.es_con, self.target_index)
        with self.assertLogs(logger, level='WARNING') as cm:
            _ = elastic.list_aliases()
            self.assertRegex(cm.output[0],
                             'WARNING:grimoire_elk.elastic:Something went wrong when retrieving aliases*')

    def test_add_aliases(self):
        """Test whether an alias is added to a given index"""

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping, aliases=['A', 'B', 'C'])

        expected_aliases = {
            'A': {},
            'B': {},
            'C': {}
        }
        aliases = elastic.list_aliases()
        self.assertDictEqual(aliases, expected_aliases)

        expected_aliases = {
            'A': {},
            'B': {},
            'C': {},
            'D': {}
        }
        elastic.add_alias('D')
        aliases = elastic.list_aliases()
        self.assertDictEqual(aliases, expected_aliases)

    def test_add_aliases_duplicated(self):
        """Test whether an alias isn't added when already present in a given index"""

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping, aliases=['A', 'B', 'C'])

        expected_aliases = {
            'A': {},
            'B': {},
            'C': {}
        }
        aliases = elastic.list_aliases()
        self.assertDictEqual(aliases, expected_aliases)

        elastic.add_alias('C')
        aliases = elastic.list_aliases()
        self.assertDictEqual(aliases, expected_aliases)

    @httpretty.activate
    def test_add_alias_warning(self):
        """Test whether a warning message is logged when the aliases for a given index are not returned"""

        url = self.es_con + "/_aliases"
        httpretty.register_uri(httpretty.POST,
                               url,
                               body={},
                               status=400,
                               forcing_headers={
                                   "Content-Type": "application/json"
                               })

        elastic = MockElasticSearch(self.es_con, self.target_index, mock_list_alias=True)
        with self.assertLogs(logger, level='WARNING') as cm:
            _ = elastic.add_alias('A')
            self.assertRegex(cm.output[0],
                             'WARNING:grimoire_elk.elastic:Something went wrong when adding an alias*')

    def test_bulk_upload(self):
        """Test whether items are correctly uploaded to an index"""

        items = json.loads(read_file('data/git.json'))
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)

        new_items = elastic.bulk_upload(items, field_id="uuid")
        self.assertEqual(new_items, 11)

    def test_bulk_upload_max_bulk(self):
        """Test whether items are correctly uploaded to an index"""

        items = json.loads(read_file('data/git.json'))
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        elastic.max_items_bulk = 2

        new_items = elastic.bulk_upload(items, field_id="uuid")
        self.assertEqual(new_items, 11)

    def test_bulk_upload_no_items(self):
        """Test whether items are correctly uploaded to an index"""

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        new_items = elastic.bulk_upload([], field_id="uuid")

        self.assertEqual(new_items, 0)

    def test_safe_put_bulk(self):
        """Test whether items are correctly stored to the index"""

        items = json.loads(read_file('data/git.json'))
        data_json = items[0]
        bulk_json = '{{"index" : {{"_id" : "{}" }} }}\n'.format(data_json['uuid'])
        bulk_json += json.dumps(data_json) + "\n"

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        bulk_url = elastic.get_bulk_url()
        inserted_items = elastic.safe_put_bulk(bulk_url, bulk_json)

        self.assertEqual(inserted_items, 1)

    def test_safe_put_bulk_errors(self):
        """Test whether an error message is logged when an item isn't inserted"""

        items = json.loads(read_file('data/git.json'))
        data_json = items[0]
        data_json['origin'] = ''.join(random.choice(string.ascii_letters) for x in range(66000))
        bulk_json = '{{"index" : {{"_id" : "{}" }} }}\n'.format(data_json['uuid'])
        bulk_json += json.dumps(data_json) + "\n"

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        bulk_url = elastic.get_bulk_url()

        with self.assertLogs(logger, level='ERROR') as cm:
            inserted_items = elastic.safe_put_bulk(bulk_url, bulk_json)
            self.assertRegex(cm.output[0], "ERROR:grimoire_elk.elastic:Failed to insert data to ES*")

        self.assertEqual(inserted_items, 0)

    def test_get_bulk_url(self):
        """Test that the bulk_url is correctly formed"""

        elastic = MockElasticSearch(self.es_con, self.target_index,
                                    major='7', distribution=ES_DISTRIBUTION)

        expected_url = elastic.url + '/' + elastic.index + '/_bulk'
        self.assertEqual(elastic.get_bulk_url(), expected_url)

        elastic = MockElasticSearch(self.es_con, self.target_index,
                                    major='6', distribution=ES_DISTRIBUTION)
        expected_url = elastic.url + '/' + elastic.index + '/items/_bulk'
        self.assertEqual(elastic.get_bulk_url(), expected_url)

        elastic = MockElasticSearch(self.es_con, self.target_index,
                                    major='1', distribution=OS_DISTRIBUTION)

        expected_url = elastic.url + '/' + elastic.index + '/_bulk'
        self.assertEqual(elastic.get_bulk_url(), expected_url)

    def test_get_mapping_url(self):
        """Test that the mapping_url is correctly formed"""

        elastic = MockElasticSearch(self.es_con, self.target_index,
                                    major='7', distribution=ES_DISTRIBUTION)

        expected_url = elastic.url + '/' + elastic.index + '/_mapping'
        self.assertEqual(elastic.get_mapping_url(), expected_url)

        elastic = MockElasticSearch(self.es_con, self.target_index,
                                    major='6', distribution=ES_DISTRIBUTION)
        expected_url = elastic.url + '/' + elastic.index + '/items/_mapping'
        self.assertEqual(elastic.get_mapping_url(_type='items'), expected_url)

        elastic = MockElasticSearch(self.es_con, self.target_index,
                                    major='1', distribution=OS_DISTRIBUTION)

        expected_url = elastic.url + '/' + elastic.index + '/_mapping'
        self.assertEqual(elastic.get_mapping_url(), expected_url)

    def test_all_properties(self):
        """Test whether all index properties are correctly returned"""

        expected_properties = {
            'data': {
                'properties': {
                    'message': {
                        'type': 'text'
                    },
                    'AuthorDate': {
                        'type': 'date',
                        'format': 'EEE MMM d HH:mm:ss yyyy Z||EEE MMM d HH:mm:ss yyyy||strict_date_optional_time||epoch_millis'
                    },
                    'CommitDate': {
                        'type': 'date',
                        'format': 'EEE MMM d HH:mm:ss yyyy Z||EEE MMM d HH:mm:ss yyyy||strict_date_optional_time||epoch_millis'
                    }
                }
            }
        }
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        properties = elastic.all_properties()

        self.assertDictEqual(properties, expected_properties)

    def test_all_properties_no_properties(self):
        """Test whether no properties are returned if they don't exist in the mapping"""

        elastic = ElasticSearch(self.es_con, self.target_index, Mapping)
        properties = elastic.all_properties()

        self.assertDictEqual(properties, {})

    @httpretty.activate
    def test_all_properties_error(self):
        """Test whether an error message is logged when the properties aren't retrieved"""

        if not ElasticSearch.is_legacy_static(self.es_major, self.es_distribution):
            url = self.es_con + '/' + self.target_index + '/_mapping'
        else:
            url = self.es_con + '/' + self.target_index + '/items/_mapping'

        httpretty.register_uri(httpretty.GET,
                               url,
                               body={},
                               status=400,
                               forcing_headers={
                                   "Content-Type": "application/json"
                               })

        elastic = MockElasticSearch(self.es_con, self.target_index,
                                    major=self.es_major, distribution=self.es_distribution)
        with self.assertLogs(logger, level='ERROR') as cm:
            elastic.all_properties()
            self.assertRegex(cm.output[0], 'ERROR:grimoire_elk.elastic:Error all attributes*')

    def test_get_last_date(self):
        """Test whether the last date is correctly returned"""

        items = json.loads(read_file('data/git.json'))
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        new_items = elastic.bulk_upload(items, field_id="uuid")
        self.assertEqual(new_items, 11)

        # no filter
        last_date = elastic.get_last_date('updated_on')
        self.assertEqual(last_date.isoformat(), '2019-10-01T18:05:52+00:00')

        # filter including all items
        fltr = {
            'name': 'origin',
            'value': '/tmp/perceval_mc84igfc/gittest'
        }
        last_date = elastic.get_last_date('updated_on', filters_=[fltr])
        self.assertEqual(last_date.isoformat(), '2014-02-12T06:11:12+00:00')

        # filter including a sub-set og items
        fltr = {
            'name': 'perceval_version',
            'value': '0.9.11'
        }
        last_date = elastic.get_last_date('updated_on', filters_=[fltr])
        self.assertEqual(last_date.isoformat(), '2014-02-12T06:09:04+00:00')

    def test_get_last_offset(self):
        """Test whether the last offset is correctly returned"""

        items = json.loads(read_file('data/kitsune.json'))
        elastic = ElasticSearch(self.es_con, self.target_index, KitsuneOcean.mapping)
        new_items = elastic.bulk_upload(items, field_id="uuid")
        self.assertEqual(new_items, 4)

        # no filter
        last_offset = elastic.get_last_offset('offset')
        self.assertEqual(last_offset, 3)

        # filter including all items
        fltr = {
            'name': 'origin',
            'value': 'http://example.com'
        }
        last_offset = elastic.get_last_offset('offset', filters_=[fltr])
        self.assertEqual(last_offset, 3)

        # filter including a sub-set og items
        fltr = {
            'name': 'perceval_version',
            'value': '0.9.11'
        }
        last_offset = elastic.get_last_offset('offset', filters_=[fltr])
        self.assertEqual(last_offset, 1)

    def test_get_last_item_field(self):
        """Test whether the date/offset of the last item is correctly returned"""

        items = json.loads(read_file('data/git.json'))
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        new_items = elastic.bulk_upload(items, field_id="uuid")
        self.assertEqual(new_items, 11)

        # no filter
        last_date = elastic.get_last_item_field('updated_on')
        self.assertEqual(last_date.isoformat(), '2019-10-01T18:05:52+00:00')

        # None filter
        last_date = elastic.get_last_item_field('updated_on', filters_=None)
        self.assertEqual(last_date.isoformat(), '2019-10-01T18:05:52+00:00')

        # Multiple filters
        fltrs = [
            {
                'name': 'origin',
                'value': '/tmp/perceval_mc84igfc/gittest'
            },
            {
                'name': 'perceval_version',
                'value': '0.9.11'
            }
        ]
        last_date = elastic.get_last_item_field('updated_on', filters_=fltrs)
        self.assertEqual(last_date.isoformat(), '2014-02-12T06:09:04+00:00')

        # Handle None filter
        fltrs = [
            {
                'name': 'origin',
                'value': '/tmp/perceval_mc84igfc/gittest'
            },
            {
                'name': 'perceval_version',
                'value': '0.9.11'
            },
            None
        ]
        last_date = elastic.get_last_item_field('updated_on', filters_=fltrs)
        self.assertEqual(last_date.isoformat(), '2014-02-12T06:09:04+00:00')

    def test_get_last_item_field_handle_invalid_date_error(self):
        """Test whether long timestamps are properly handled"""

        items = json.loads(read_file('data/git.json'))
        items[-1]['updated_on'] = items[-1]['updated_on'] * 1000
        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        new_items = elastic.bulk_upload(items, field_id="uuid")
        self.assertEqual(new_items, 11)

        last_date = elastic.get_last_item_field('updated_on')
        self.assertEqual(last_date.isoformat(), '2019-10-01T18:05:53.024000+00:00')

    def test_delete_items(self):
        """Test whether items are correctly deleted"""

        items = json.loads(read_file('data/git.json'))
        for item in items:
            timestamp = unixtime_to_datetime(item['timestamp'])
            item['timestamp'] = timestamp.isoformat()

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        new_items = elastic.bulk_upload(items, field_id="uuid")
        self.assertEqual(new_items, 11)

        url = self.es_con + '/' + self.target_index + '/_count'

        elastic.delete_items(retention_time=90000000, time_field='timestamp')
        left_items = elastic.requests.get(url).json()['count']
        self.assertEqual(left_items, 11)

        elastic.delete_items(retention_time=1, time_field='timestamp')
        left_items = elastic.requests.get(url).json()['count']
        self.assertEqual(left_items, 0)

    def test_delete_items_wrong_retention(self):
        """Test whether no items are deleted if retention isn't defined or negative"""

        items = json.loads(read_file('data/git.json'))
        for item in items:
            timestamp = unixtime_to_datetime(item['timestamp'])
            item['timestamp'] = timestamp.isoformat()

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        new_items = elastic.bulk_upload(items, field_id="uuid")
        self.assertEqual(new_items, 11)

        url = self.es_con + '/' + self.target_index + '/_count'

        elastic.delete_items(retention_time=None, time_field='timestamp')
        left_items = elastic.requests.get(url).json()['count']
        self.assertEqual(left_items, 11)

        elastic.delete_items(retention_time=-1, time_field='timestamp')
        left_items = elastic.requests.get(url).json()['count']
        self.assertEqual(left_items, 11)

    def test_delete_items_error(self):
        """Test whether an error message is logged if the items aren't deleted"""

        items = json.loads(read_file('data/git.json'))

        elastic = ElasticSearch(self.es_con, self.target_index, GitOcean.mapping)
        new_items = elastic.bulk_upload(items, field_id="uuid")
        self.assertEqual(new_items, 11)

        with self.assertLogs(logger, level='ERROR') as cm:
            elastic.delete_items(retention_time=1, time_field='timestamp')
            self.assertRegex(cm.output[0], 'ERROR:grimoire_elk.elastic:\\[items retention\\] Error deleted items*')


if __name__ == '__main__':
    unittest.main()
