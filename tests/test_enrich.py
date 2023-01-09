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
#     Alberto Perez Garcia-Plaza <alpgarcia@bitergia.com>
#

import configparser
import json

import httpretty
import requests
import sys
import unittest
from unittest.mock import MagicMock

from grimoire_elk.elastic import logger
from grimoire_elk.enriched.enrich import (Enrich,
                                          HEADER_JSON,
                                          anonymize_url)
from grimoire_elk.utils import get_connectors, get_elastic

# Make sure we use our code and not any other could we have installed
sys.path.insert(0, '..')

CONFIG_FILE = 'tests.conf'


def setup_http_server(url, not_handle_status_code=False):
    """Setup a mock HTTP server"""

    http_requests = []

    body_content_1 = read_file("data/author_min_max_dates_1.json")
    body_content_2 = read_file("data/author_min_max_dates_2.json")
    body_content_empty = read_file("data/author_min_max_dates_empty.json")

    def request_callback(method, uri, headers):

        status_code = 200
        composite = method.parsed_body['aggs']['author']['composite']
        if "after" in composite:
            if composite["after"]["author_uuid"] == "007a56d0322c518859dde2a0c6ed9143fa141c61":
                body = body_content_2
            else:
                body = body_content_empty
        else:
            body = body_content_1
        http_requests.append(httpretty.last_request())

        return status_code, headers, body

    httpretty.register_uri(httpretty.POST,
                           url,
                           match_querystring=True,
                           responses=[
                               httpretty.Response(body=request_callback)
                           ])

    return http_requests


def read_file(filename):
    with open(filename) as f:
        return f.read()


class TestEnrich(unittest.TestCase):

    def setUp(self):
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        es_con = dict(config.items('ElasticSearch'))['url']
        git_enriched = "git_enriched"
        enrich_backend = get_connectors()["git"][2]()
        elastic_enrich = get_elastic(es_con, git_enriched, True, enrich_backend)
        self._enrich = Enrich()
        self._enrich.elastic = elastic_enrich

        self.empty_item = {
            "author_id": "",
            "author_uuid": "",
            "author_name": "",
            "author_user_name": "",
            "author_domain": "",
            "author_gender": "",
            "author_gender_acc": None,
            "author_org_name": "",
            "author_bot": False,
            "author_multi_org_names": [""]
        }

    def test_get_item_sh_fields_identity(self):
        """Test sortinghat fields retrieval for a given identity"""

        # Method Parameter: Identity
        identity = {
            "name": "pepe",
            "username": "pepepotamo"
        }

        expected = {
            'author_id': "11111",
            'author_uuid': "aaaaa",
            'author_name': "Pepe",
            'author_user_name': identity["username"],
            'author_domain': "local",
            'author_gender': "male",
            'author_gender_acc': 100,
            'author_org_name': "Bitergia",
            'author_bot': False,
            'author_multi_org_names': ['Bitergia']
        }

        empty_item_by_rol = {'author': self.empty_item}

        def rol_side_effect(rol):
            return empty_item_by_rol[rol]
        self._enrich.__get_item_sh_fields_empty = MagicMock(side_effect=rol_side_effect)

        entity = {
            'id': expected['author_id'],
            'uuid': expected['author_uuid'],
            'name': expected['author_name'],
            'username': identity["username"],
            'email': None,  # None to verify email is from profile
            'profile': {
                "name": expected['author_name'],
                "email": "pepepotamo@local.com",
                "gender": expected['author_gender'],
                "gender_acc": expected['author_gender_acc'],
                "is_bot": expected['author_bot']
            },
            'enrollments': [
                {
                    'group': {
                        'parent_org': None,
                        'name': expected['author_org_name'],
                        'type': 'organization',
                        'start': "1900-01-01T00:00:00+00:00",
                        'end': "2100-01-01T00:00:00+00:00"
                    }
                }
            ]
        }
        self._enrich.get_sh_item_from_identity = MagicMock(return_value=entity)

        email_doms = {'pepepotamo@local.com': expected['author_domain']}

        def email_domain_side_effect(email):
            return email_doms[email]
        self._enrich.get_email_domain = MagicMock(side_effect=email_domain_side_effect)

        # 1. Author role

        # Method to test
        eitem_sh = self._enrich.get_item_sh_fields(identity=identity)
        self.assertEqual(eitem_sh['author_id'], expected['author_id'])
        self.assertEqual(eitem_sh['author_uuid'], expected['author_uuid'])
        self.assertEqual(eitem_sh['author_name'], expected['author_name'])
        self.assertEqual(eitem_sh['author_user_name'], expected['author_user_name'])
        self.assertEqual(eitem_sh['author_domain'], expected['author_domain'])
        self.assertEqual(eitem_sh['author_gender'], expected['author_gender'])
        self.assertEqual(eitem_sh['author_gender_acc'], expected['author_gender_acc'])
        self.assertEqual(eitem_sh['author_org_name'], expected['author_org_name'])
        self.assertEqual(eitem_sh['author_bot'], expected['author_bot'])
        self.assertEqual(eitem_sh['author_multi_org_names'], expected['author_multi_org_names'])

        # 2. Change role

        # Method to test
        eitem_sh = self._enrich.get_item_sh_fields(identity=identity, rol='assignee')
        self.assertEqual(eitem_sh['assignee_id'], expected['author_id'])
        self.assertEqual(eitem_sh['assignee_uuid'], expected['author_uuid'])
        self.assertEqual(eitem_sh['assignee_name'], expected['author_name'])
        self.assertEqual(eitem_sh['assignee_user_name'], expected['author_user_name'])
        self.assertEqual(eitem_sh['assignee_domain'], expected['author_domain'])
        self.assertEqual(eitem_sh['assignee_gender'], expected['author_gender'])
        self.assertEqual(eitem_sh['assignee_gender_acc'], expected['author_gender_acc'])
        self.assertEqual(eitem_sh['assignee_org_name'], expected['author_org_name'])
        self.assertEqual(eitem_sh['assignee_bot'], expected['author_bot'])
        self.assertEqual(eitem_sh['assignee_multi_org_names'], expected['author_multi_org_names'])

    def test_get_item_sh_fields_identity_no_uuid(self):
        """uuid is None (not found in sortinghat) or does not exist"""

        # Method Parameter: Identity
        identity = {
            "name": "pepe",
            "username": "pepepotamo"
        }

        # In this test 'empty_item' is also the expected result

        # 1. uuid is an empty string

        empty_item_by_rol = {'author': self.empty_item}

        def rol_side_effect(rol):
            return empty_item_by_rol[rol]
        self._enrich.__get_item_sh_fields_empty = MagicMock(side_effect=rol_side_effect)

        sh_ids = {
            "id": "",
            "uuid": ""
        }
        self._enrich.get_sh_item_from_identity = MagicMock(return_value=sh_ids)

        # Method to test
        eitem_sh = self._enrich.get_item_sh_fields(identity=identity)
        self.assertEqual(eitem_sh['author_id'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_uuid'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_name'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_user_name'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_domain'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_gender'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_gender_acc'], None)
        self.assertEqual(eitem_sh['author_org_name'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_multi_org_names'], ['-- UNDEFINED --'])

        # 2. No uuid field
        sh_ids = {
            "id": ""
        }
        self._enrich.get_sh_item_from_identity = MagicMock(return_value=sh_ids)

        # Method to test
        eitem_sh = self._enrich.get_item_sh_fields(identity=identity)
        self.assertEqual(eitem_sh['author_id'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_uuid'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_name'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_user_name'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_domain'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_gender'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_gender_acc'], None)
        self.assertEqual(eitem_sh['author_org_name'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_bot'], False)
        self.assertEqual(eitem_sh['author_multi_org_names'], ['-- UNDEFINED --'])

    def test_get_item_sh_fields_identity_no_profile(self):
        """Test retrieval when no profile data is found or data is not what we expected"""

        # Method Parameter: Identity
        identity = {
            "name": "pepe",
            "username": "pepepotamo"
        }

        # As there is no profile, name should come from the identity,
        # domain should be None as specified above in self._enrich.get_identity_domain
        # mock, and gender fields will have values coming from the
        # initial empty item.
        expected = {
            'author_id': "11111",
            'author_uuid': "aaaaa",
            'author_name': "pepe",
            'author_user_name': identity["username"],
            'author_domain': None,
            'author_gender': self._enrich.unknown_gender,
            'author_gender_acc': 0,
            'author_org_name': "Bitergia",
            'author_bot': False,
            'author_multi_org_names': ['Bitergia']
        }

        # 1. Profile is None

        empty_item_by_rol = {'author': self.empty_item}

        def rol_side_effect(rol):
            return empty_item_by_rol[rol]
        self._enrich.__get_item_sh_fields_empty = MagicMock(side_effect=rol_side_effect)

        entity = {
            'id': expected['author_id'],
            'uuid': expected['author_uuid'],
            'name': expected['author_name'],
            'username': identity["username"],
            'email': None,  # None to verify email is from profile
            'profile': None,
            'enrollments': [
                {
                    'group': {
                        'parent_org': None,
                        'name': expected['author_org_name'],
                        'type': 'organization',
                        'start': "1900-01-01T00:00:00+00:00",
                        'end': "2100-01-01T00:00:00+00:00"
                    }
                }
            ]
        }
        self._enrich.get_sh_item_from_identity = MagicMock(return_value=entity)

        email_doms = {'pepepotamo@local.com': expected['author_domain']}

        def email_domain_side_effect(email):
            return email_doms[email]
        self._enrich.get_email_domain = MagicMock(side_effect=email_domain_side_effect)

        # Method to test
        eitem_sh = self._enrich.get_item_sh_fields(identity=identity)
        self.assertEqual(eitem_sh['author_id'], expected['author_id'])
        self.assertEqual(eitem_sh['author_uuid'], expected['author_uuid'])
        self.assertEqual(eitem_sh['author_name'], expected['author_name'])
        self.assertEqual(eitem_sh['author_user_name'], expected['author_user_name'])
        self.assertEqual(eitem_sh['author_domain'], expected['author_domain'])
        self.assertEqual(eitem_sh['author_gender'], expected['author_gender'])
        self.assertEqual(eitem_sh['author_gender_acc'], expected['author_gender_acc'])
        self.assertEqual(eitem_sh['author_org_name'], expected['author_org_name'])
        self.assertEqual(eitem_sh['author_bot'], expected['author_bot'])
        self.assertEqual(eitem_sh['author_multi_org_names'], expected['author_multi_org_names'])

        # 2. Profile as empty dict

        entity['profile'] = {}

        # Method to test
        eitem_sh = self._enrich.get_item_sh_fields(identity=identity)
        # Same as 1.3.
        self.assertEqual(eitem_sh['author_id'], expected['author_id'])
        self.assertEqual(eitem_sh['author_uuid'], expected['author_uuid'])
        self.assertEqual(eitem_sh['author_name'], expected['author_name'])
        self.assertEqual(eitem_sh['author_user_name'], expected['author_user_name'])
        self.assertEqual(eitem_sh['author_domain'], expected['author_domain'])
        self.assertEqual(eitem_sh['author_gender'], expected['author_gender'])
        self.assertEqual(eitem_sh['author_gender_acc'], expected['author_gender_acc'])
        self.assertEqual(eitem_sh['author_org_name'], expected['author_org_name'])
        self.assertEqual(eitem_sh['author_bot'], expected['author_bot'])
        self.assertEqual(eitem_sh['author_multi_org_names'], expected['author_multi_org_names'])

        # 3 Profile with other fields

        entity['profile'] = {
            "first": expected['author_name'],
            "second": "pepepotamo@local.com",
        }

        # Method to test
        self.assertEqual(eitem_sh['author_id'], expected['author_id'])
        self.assertEqual(eitem_sh['author_uuid'], expected['author_uuid'])
        self.assertEqual(eitem_sh['author_name'], expected['author_name'])
        self.assertEqual(eitem_sh['author_user_name'], expected['author_user_name'])
        self.assertEqual(eitem_sh['author_domain'], expected['author_domain'])
        self.assertEqual(eitem_sh['author_gender'], expected['author_gender'])
        self.assertEqual(eitem_sh['author_gender_acc'], expected['author_gender_acc'])
        self.assertEqual(eitem_sh['author_org_name'], expected['author_org_name'])
        self.assertEqual(eitem_sh['author_bot'], expected['author_bot'])
        self.assertEqual(eitem_sh['author_multi_org_names'], expected['author_multi_org_names'])

    def test_get_item_sh_fields_identity_no_gender(self):
        """Profile with no gender values"""

        # Method Parameter: Identity
        identity = {
            "name": "pepe",
            "username": "pepepotamo"
        }

        expected = {
            'author_id': "11111",
            'author_uuid': "aaaaa",
            'author_name': "Pepe",
            'author_user_name': identity["username"],
            'author_domain': "local",
            'author_gender': self._enrich.unknown_gender,
            'author_gender_acc': 0,
            'author_org_name': "Bitergia",
            'author_bot': False,
            'author_multi_org_names': ['Bitergia']
        }

        empty_item_by_rol = {'author': self.empty_item}

        def rol_side_effect(rol):
            return empty_item_by_rol[rol]
        self._enrich.__get_item_sh_fields_empty = MagicMock(side_effect=rol_side_effect)

        entity = {
            'id': expected['author_id'],
            'uuid': expected['author_uuid'],
            'name': expected['author_name'],
            'username': identity["username"],
            'email': None,  # None to verify email is from profile
            'profile': {
                "name": expected['author_name'],
                "email": "pepepotamo@local.com",
                "is_bot": expected['author_bot']
            },
            'enrollments': [
                {
                    'group': {
                        'parent_org': None,
                        'name': expected['author_org_name'],
                        'type': 'organization',
                        'start': "1900-01-01T00:00:00+00:00",
                        'end': "2100-01-01T00:00:00+00:00"
                    }
                }
            ]
        }
        self._enrich.get_sh_item_from_identity = MagicMock(return_value=entity)

        email_doms = {'pepepotamo@local.com': expected['author_domain']}

        def email_domain_side_effect(email):
            return email_doms[email]
        self._enrich.get_email_domain = MagicMock(side_effect=email_domain_side_effect)

        # Method to test
        eitem_sh = self._enrich.get_item_sh_fields(identity=identity)
        self.assertEqual(eitem_sh['author_id'], expected['author_id'])
        self.assertEqual(eitem_sh['author_uuid'], expected['author_uuid'])
        self.assertEqual(eitem_sh['author_name'], expected['author_name'])
        self.assertEqual(eitem_sh['author_user_name'], expected['author_user_name'])
        self.assertEqual(eitem_sh['author_domain'], expected['author_domain'])
        self.assertEqual(eitem_sh['author_gender'], expected['author_gender'])
        self.assertEqual(eitem_sh['author_gender_acc'], expected['author_gender_acc'])
        self.assertEqual(eitem_sh['author_org_name'], expected['author_org_name'])
        self.assertEqual(eitem_sh['author_bot'], expected['author_bot'])
        self.assertEqual(eitem_sh['author_multi_org_names'], expected['author_multi_org_names'])

    def test_get_item_sh_fields_sh_id(self):
        """Test retrieval from sortinghat id"""

        sh_id = "11111"

        expected = {
            'author_id': sh_id,
            'author_uuid': "aaaaa",
            'author_name': "Pepe",
            'author_user_name': "",
            'author_domain': "local",
            'author_gender': "male",
            'author_gender_acc': 100,
            'author_org_name': "Bitergia",
            'author_bot': False,
            'author_multi_org_names': ['Bitergia']
        }

        empty_item_by_rol = {'author': self.empty_item}

        def rol_side_effect(rol):
            return empty_item_by_rol[rol]
        self._enrich.__get_item_sh_fields_empty = MagicMock(side_effect=rol_side_effect)

        entity = {
            'id': expected['author_id'],
            'uuid': expected['author_uuid'],
            'profile': {
                "name": expected['author_name'],
                "email": "pepepotamo@local.com",
                "gender": expected['author_gender'],
                "gender_acc": expected['author_gender_acc'],
                "is_bot": expected['author_bot']
            },
            'enrollments': [
                {
                    'group': {
                        'parent_org': None,
                        'name': expected['author_org_name'],
                        'type': 'organization',
                        'start': "1900-01-01T00:00:00+00:00",
                        'end': "2100-01-01T00:00:00+00:00"
                    }
                }
            ]
        }
        self._enrich.get_sh_item_from_id = MagicMock(return_value=entity)

        email_doms = {'pepepotamo@local.com': expected['author_domain']}

        def email_domain_side_effect(email):
            return email_doms[email]
        self._enrich.get_email_domain = MagicMock(side_effect=email_domain_side_effect)

        # Method to test
        eitem_sh = self._enrich.get_item_sh_fields(sh_id=sh_id)
        self.assertEqual(eitem_sh['author_id'], expected['author_id'])
        self.assertEqual(eitem_sh['author_uuid'], expected['author_uuid'])
        self.assertEqual(eitem_sh['author_name'], expected['author_name'])
        self.assertEqual(eitem_sh['author_user_name'], expected['author_user_name'])
        self.assertEqual(eitem_sh['author_domain'], expected['author_domain'])
        self.assertEqual(eitem_sh['author_gender'], expected['author_gender'])
        self.assertEqual(eitem_sh['author_gender_acc'], expected['author_gender_acc'])
        self.assertEqual(eitem_sh['author_org_name'], expected['author_org_name'])
        self.assertEqual(eitem_sh['author_bot'], expected['author_bot'])
        self.assertEqual(eitem_sh['author_org_name'], expected['author_org_name'])
        self.assertEqual(eitem_sh['author_bot'], expected['author_bot'])
        self.assertEqual(eitem_sh['author_multi_org_names'], expected['author_multi_org_names'])

    def test_get_item_sh_fields_sh_id_no_uuid(self):
        """Test retrieval from sortinghat id when there is no uuid"""

        sh_id = "11111"

        # 1. uuid is None

        empty_item_by_rol = {'author': self.empty_item}

        def rol_side_effect(rol):
            return empty_item_by_rol[rol]
        self._enrich.__get_item_sh_fields_empty = MagicMock(side_effect=rol_side_effect)

        entity = {
            'id': sh_id,
            'uuid': None
        }
        self._enrich.get_sh_item_from_id = MagicMock(return_value=entity)

        # Method to test
        eitem_sh = self._enrich.get_item_sh_fields(sh_id=sh_id)
        self.assertEqual(eitem_sh['author_id'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_uuid'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_name'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_user_name'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_domain'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_gender'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_gender_acc'], None)
        self.assertEqual(eitem_sh['author_org_name'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_bot'], False)
        self.assertEqual(eitem_sh['author_multi_org_names'], ['-- UNDEFINED --'])

        # 2. uuid is an empty string

        entity['uuid'] = ""

        # Method to test
        eitem_sh = self._enrich.get_item_sh_fields(sh_id=sh_id)
        self.assertEqual(eitem_sh['author_id'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_uuid'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_name'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_user_name'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_domain'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_gender'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_gender_acc'], None)
        self.assertEqual(eitem_sh['author_org_name'], '-- UNDEFINED --')
        self.assertEqual(eitem_sh['author_bot'], False)
        self.assertEqual(eitem_sh['author_multi_org_names'], ['-- UNDEFINED --'])

    def test_get_main_enrollments(self):
        """Test get the main enrollment given the list of enrollments"""
        enrollments = ['Bitergia::Eng', 'Chaoss']
        main_enrolls = self._enrich.get_main_enrollments(enrollments)
        self.assertListEqual(main_enrolls, ['Bitergia', 'Chaoss'])

    def test_remove_prefix_enrollments(self):
        """Test remove the prefix enrollment given the list of enrollments"""
        enrollments = ['Bitergia::Eng', 'Chaoss']
        enrolls = self._enrich.remove_prefix_enrollments(enrollments)
        self.assertListEqual(enrolls, ['Chaoss', 'Eng'])

    def test_no_params(self):
        """Neither identity nor sh_id are passed as arguments"""

        empty_item_by_rol = {'author': self.empty_item}

        def rol_side_effect(rol):
            return empty_item_by_rol[rol]
        self._enrich.__get_item_sh_fields_empty = MagicMock(side_effect=rol_side_effect)

        # Method to test
        eitem_sh = self._enrich.get_item_sh_fields()
        self.assertEqual(eitem_sh['author_id'], self.empty_item['author_id'])
        self.assertEqual(eitem_sh['author_uuid'], self.empty_item['author_uuid'])
        self.assertEqual(eitem_sh['author_name'], self.empty_item['author_name'])
        self.assertEqual(eitem_sh['author_user_name'], self.empty_item['author_user_name'])
        self.assertEqual(eitem_sh['author_domain'], self.empty_item['author_domain'])
        self.assertEqual(eitem_sh['author_gender'], self.empty_item['author_gender'])
        self.assertEqual(eitem_sh['author_gender_acc'], self.empty_item['author_gender_acc'])
        self.assertEqual(eitem_sh['author_org_name'], self.empty_item['author_org_name'])
        self.assertEqual(eitem_sh['author_bot'], self.empty_item['author_bot'])
        self.assertEqual(eitem_sh['author_multi_org_names'], self.empty_item['author_multi_org_names'])

    def test_add_alias(self):
        """Test whether add_alias properly works"""

        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        es_con = dict(config.items('ElasticSearch'))['url']

        tmp_index = "test-add-aliases"
        tmp_index_url = es_con + "/" + tmp_index

        enrich_backend = get_connectors()["git"][2]()
        elastic_enrich = get_elastic(es_con, tmp_index, True, enrich_backend)
        self._enrich.set_elastic(elastic_enrich)

        # add alias
        alias = "demographics"
        with self.assertLogs(logger, level='INFO') as cm:
            self._enrich.elastic.add_alias(alias)

        self.assertEqual(cm.output[0],
                         'INFO:grimoire_elk.elastic:Alias %s created on %s.'
                         % (alias, anonymize_url(tmp_index_url)))

        r = self._enrich.requests.get(self._enrich.elastic.index_url + "/_alias", headers=HEADER_JSON, verify=False)
        self.assertIn(alias, r.json()[self._enrich.elastic.index]['aliases'])

        # add alias again
        with self.assertLogs(logger, level='DEBUG') as cm:
            self._enrich.elastic.add_alias(alias)

        self.assertEqual(cm.output[0],
                         'DEBUG:grimoire_elk.elastic:Alias %s already exists on %s.'
                         % (alias, anonymize_url(tmp_index_url)))

        requests.delete(tmp_index_url, verify=False)

    def test_copy_raw_fields(self):
        """Test whether raw fields are copied properly"""

        raw_fields = ["metadata__updated_on", "metadata__timestamp",
                      "offset", "origin", "tag", "uuid", "extra"]
        source = {
            "metadata__updated_on": "2020-03-19T18:08:45.480000+00:00",
            "metadata__timestamp": "2020-04-11T12:45:37.229535+00:00",
            "offset": None,
            "origin": "https://gitter.im/jenkinsci/jenkins",
            "tag": "https://gitter.im/jenkinsci/jenkins",
            "uuid": "a9f92bf99785ad838df6736b7825c6f7ae16ac58",
        }

        expected = {
            "metadata__updated_on": "2020-03-19T18:08:45.480000+00:00",
            "metadata__timestamp": "2020-04-11T12:45:37.229535+00:00",
            "offset": None,
            "origin": "https://gitter.im/jenkinsci/jenkins",
            "tag": "https://gitter.im/jenkinsci/jenkins",
            "uuid": "a9f92bf99785ad838df6736b7825c6f7ae16ac58",
            "extra": None
        }

        eitem = {}
        self._enrich.copy_raw_fields(raw_fields, source, eitem)

        self.assertEqual(eitem['metadata__updated_on'], expected['metadata__updated_on'])
        self.assertEqual(eitem['metadata__timestamp'], expected['metadata__timestamp'])
        self.assertEqual(eitem['offset'], expected['offset'])
        self.assertEqual(eitem['origin'], expected['origin'])
        self.assertEqual(eitem['tag'], expected['tag'])
        self.assertEqual(eitem['uuid'], expected['uuid'])
        self.assertEqual(eitem['extra'], expected['extra'])

    def test_authors_min_max_dates(self):
        expected_es_query = """
        {
          "size": 0,
          "aggs": {
            "author": {
              "composite": {
                "sources": [
                  {
                    "author_uuid": {
                      "terms": {
                        "field": "author_uuid"
                      }
                    }
                  }
                ],
                "size": 10000
              },
              "aggs": {
                "min": {
                  "min": {
                    "field": "grimoire_creation_date"
                  }
                },
                "max": {
                  "max": {
                    "field": "grimoire_creation_date"
                  }
                }
              }
            }
          }
        }
        """
        es_query = self._enrich.authors_min_max_dates("grimoire_creation_date",
                                                      author_field="author_uuid",
                                                      contribution_type=None,
                                                      after=None)
        self.assertDictEqual(json.loads(es_query), json.loads(expected_es_query))

        expected_es_query = """
        {
          "size": 0,
          "aggs": {
            "author": {
              "composite": {
                "sources": [
                  {
                    "author_uuid": {
                      "terms": {
                        "field": "author_uuid"
                      }
                    }
                  }
                ],
                "after": {
                  "author_uuid": "uuid"
                },
                "size": 10000
              },
              "aggs": {
                "min": {
                  "min": {
                    "field": "grimoire_creation_date"
                  }
                },
                "max": {
                  "max": {
                    "field": "grimoire_creation_date"
                  }
                }
              }
            }
          }
        }
        """
        es_query = self._enrich.authors_min_max_dates("grimoire_creation_date",
                                                      author_field="author_uuid",
                                                      contribution_type=None,
                                                      after="uuid")
        self.assertDictEqual(json.loads(es_query), json.loads(expected_es_query))

    @httpretty.activate
    def test_fetch_authors_min_max_dates(self):

        es_search_url = "{}/_search".format(self._enrich.elastic.index_url)
        _ = setup_http_server(es_search_url)

        log_prefix = "[git] Demography"
        author_field = "author_uuid"
        date_field = "grimoire_creation_date"

        expected = [
            {
                'key': {'author_uuid': '00032fabbbf033467d7bd307df81b654c0fa53d8'},
                'doc_count': 1,
                'min': {'value': 1623225379000.0, 'value_as_string': '2021-06-09T07:56:19.000Z'},
                'max': {'value': 1623225379000.0, 'value_as_string': '2021-06-09T07:56:19.000Z'}
            },
            {
                'key': {'author_uuid': '007a56d0322c518859dde2a0c6ed9143fa141c61'},
                'doc_count': 1,
                'min': {'value': 1626183289000.0, 'value_as_string': '2021-07-13T13:34:49.000Z'},
                'max': {'value': 1626183289000.0, 'value_as_string': '2021-07-13T13:34:49.000Z'}
            },
            {
                'key': {'author_uuid': '00cc95a5950523a42c969f15c7c36c4530417f13'},
                'doc_count': 1,
                'min': {'value': 1474160034000.0, 'value_as_string': '2016-09-18T00:53:54.000Z'},
                'max': {'value': 1474160034000.0, 'value_as_string': '2016-09-18T00:53:54.000Z'}
            },
            {
                'key': {'author_uuid': '00d36515f739794b941586e5d0a102b5ff3a0cc2'},
                'doc_count': 1,
                'min': {'value': 1526521972000.0, 'value_as_string': '2018-05-17T01:52:52.000Z'},
                'max': {'value': 1526521972000.0, 'value_as_string': '2018-05-17T01:52:52.000Z'}
            }
        ]
        authors_min_max_data = self._enrich.fetch_authors_min_max_dates(log_prefix, author_field,
                                                                        None, date_field)
        all_authors = []
        for author_key in authors_min_max_data:
            all_authors.append(author_key)
        self.assertListEqual(all_authors, expected)

    def test_get_field_unique_id(self):
        self.assertEqual(self._enrich.get_field_unique_id(), 'uuid')

    def test_get_field_event_unique_id(self):
        with self.assertRaises(NotImplementedError):
            self._enrich.get_field_event_unique_id()

    def test_get_rich_item(self):
        with self.assertRaises(NotImplementedError):
            self._enrich.get_rich_item({})

    def test_get_rich_events(self):
        with self.assertRaises(NotImplementedError):
            self._enrich.get_rich_events({})

    def test_get_field_author(self):
        with self.assertRaises(NotImplementedError):
            self._enrich.get_field_author()

    def test_get_field_date(self):
        self.assertEqual(self._enrich.get_field_date(), 'metadata__updated_on')

    def test_get_identities(self):
        with self.assertRaises(NotImplementedError):
            self._enrich.get_identities(item=None)

    def test_add_repository_labels(self):
        item = {}
        self._enrich.add_repository_labels(item)
        self.assertIsNone(item['repository_labels'])

    def test_add_metadata_filter_raw(self):
        item = {}
        self._enrich.add_metadata_filter_raw(item)
        self.assertIsNone(item['metadata__filter_raw'])

    def test_get_item_id(self):
        item = {
            "_id": "0000"
        }
        self.assertEqual(self._enrich.get_item_id(item), item['_id'])


if __name__ == '__main__':
    unittest.main()
