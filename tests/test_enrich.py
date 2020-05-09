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
#     Alberto Perez Garcia-Plaza <alpgarcia@bitergia.com>
#

import configparser
import requests
import sys
import unittest
from unittest.mock import MagicMock

from grimoire_elk.elastic import logger
from grimoire_elk.enriched.enrich import (Enrich,
                                          DEMOGRAPHICS_ALIAS,
                                          HEADER_JSON,
                                          anonymize_url)
from sortinghat.db.model import UniqueIdentity, Profile
from grimoire_elk.utils import get_connectors, get_elastic

# Make sure we use our code and not any other could we have installed
sys.path.insert(0, '..')

CONFIG_FILE = 'tests.conf'


class TestEnrich(unittest.TestCase):

    def setUp(self):
        self._enrich = Enrich()

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

    def test_get_profile_sh(self):
        """Test whether a profile from sortinghat model is correctly retrieved as a dict"""

        p = Profile()
        p.name = 'pepe'
        p.email = 'pepe@host.com'
        p.gender = 'male'
        p.gender_acc = 100
        uidentity = UniqueIdentity()
        uidentity.profile = p

        vals = {'00000': uidentity}

        def side_effect(uuid):
            return vals[uuid]
        self._enrich.get_unique_identity = MagicMock(side_effect=side_effect)

        profile = self._enrich.get_profile_sh('00000')
        self.assertEqual(profile['name'], uidentity.profile.name)
        self.assertEqual(profile['email'], uidentity.profile.email)
        self.assertEqual(profile['gender'], uidentity.profile.gender)
        self.assertEqual(profile['gender_acc'], uidentity.profile.gender_acc)

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

        sh_ids = {
            "id": expected['author_id'],
            "uuid": expected['author_uuid']
        }
        self._enrich.get_sh_ids = MagicMock(return_value=sh_ids)

        # Return None to verify we are taking this value from sortinghat profile
        # (see self._enrich.get_profile_sh below)
        self._enrich.get_identity_domain = MagicMock(return_value=None)

        p1 = {
            "name": expected['author_name'],
            "email": "pepepotamo@local.com",
            "gender": expected['author_gender'],
            "gender_acc": expected['author_gender_acc']
        }
        sh_prof_vals = {expected['author_uuid']: p1}

        def profile_side_effect(uuid):
            return sh_prof_vals[uuid]
        self._enrich.get_profile_sh = MagicMock(side_effect=profile_side_effect)

        email_doms = {'pepepotamo@local.com': expected['author_domain']}

        def email_domain_side_effect(email):
            return email_doms[email]
        self._enrich.get_email_domain = MagicMock(side_effect=email_domain_side_effect)

        enrollments = {(expected['author_uuid'], None): expected['author_org_name']}

        def enrollments_side_effect(uuid, item_date):
            return enrollments[(uuid, item_date)]
        self._enrich.get_enrollment = MagicMock(side_effect=enrollments_side_effect)

        multi_enrollments = {(expected['author_uuid'], None): expected['author_multi_org_names']}

        def multi_enrollments_side_effect(uuid, item_date):
            return multi_enrollments[(uuid, item_date)]
        self._enrich.get_multi_enrollment = MagicMock(side_effect=multi_enrollments_side_effect)

        bots = {'aaaaa': False}

        def bots_side_effect(uuid):
            return bots[uuid]
        self._enrich.is_bot = MagicMock(side_effect=bots_side_effect)

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
        self._enrich.get_sh_ids = MagicMock(return_value=sh_ids)

        # Return None to verify we are taking this value from sortinghat profile
        # (see self._enrich.get_profile_sh below)
        self._enrich.get_identity_domain = MagicMock(return_value=None)

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
        self._enrich.get_sh_ids = MagicMock(return_value=sh_ids)

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

        sh_ids = {
            "id": expected['author_id'],
            "uuid": expected['author_uuid']
        }
        self._enrich.get_sh_ids = MagicMock(return_value=sh_ids)

        # Return None to verify we are taking this value from sortinghat profile
        # (see self._enrich.get_profile_sh below)
        self._enrich.get_identity_domain = MagicMock(return_value=None)

        self._enrich.get_profile_sh = MagicMock(return_value=None)

        email_doms = {'pepepotamo@local.com': expected['author_domain']}

        def email_domain_side_effect(email):
            return email_doms[email]
        self._enrich.get_email_domain = MagicMock(side_effect=email_domain_side_effect)

        enrollments = {(expected['author_uuid'], None): expected['author_org_name']}

        def enrollments_side_effect(uuid, item_date):
            return enrollments[(uuid, item_date)]
        self._enrich.get_enrollment = MagicMock(side_effect=enrollments_side_effect)

        multi_enrollments = {(expected['author_uuid'], None): expected['author_multi_org_names']}

        def multi_enrollments_side_effect(uuid, item_date):
            return multi_enrollments[(uuid, item_date)]

        self._enrich.get_multi_enrollment = MagicMock(side_effect=multi_enrollments_side_effect)

        bots = {'aaaaa': False}

        def bots_side_effect(uuid):
            return bots[uuid]
        self._enrich.is_bot = MagicMock(side_effect=bots_side_effect)

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

        self._enrich.get_profile_sh = MagicMock(return_value={})

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

        p1 = {
            "first": expected['author_name'],
            "second": "pepepotamo@local.com",
        }
        sh_prof_vals = {expected['author_uuid']: p1}

        def profile_side_effect(uuid):
            return sh_prof_vals[uuid]
        self._enrich.get_profile_sh = MagicMock(side_effect=profile_side_effect)

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

        sh_ids = {
            "id": expected['author_id'],
            "uuid": expected['author_uuid']
        }
        self._enrich.get_sh_ids = MagicMock(return_value=sh_ids)

        # Return None to verify we are taking this value from sortinghat profile
        # (see self._enrich.get_profile_sh below)
        self._enrich.get_identity_domain = MagicMock(return_value=None)

        p1 = {
            "name": expected['author_name'],
            "email": "pepepotamo@local.com",
            "gender": expected['author_gender'],
            "gender_acc": expected['author_gender_acc']
        }
        sh_prof_vals = {expected['author_uuid']: p1}

        def profile_side_effect(uuid):
            return sh_prof_vals[uuid]
        self._enrich.get_profile_sh = MagicMock(side_effect=profile_side_effect)

        email_doms = {'pepepotamo@local.com': expected['author_domain']}

        def email_domain_side_effect(email):
            return email_doms[email]
        self._enrich.get_email_domain = MagicMock(side_effect=email_domain_side_effect)

        enrollments = {(expected['author_uuid'], None): expected['author_org_name']}

        def enrollments_side_effect(uuid, item_date):
            return enrollments[(uuid, item_date)]
        self._enrich.get_enrollment = MagicMock(side_effect=enrollments_side_effect)

        multi_enrollments = {(expected['author_uuid'], None): expected['author_multi_org_names']}

        def multi_enrollments_side_effect(uuid, item_date):
            return multi_enrollments[(uuid, item_date)]

        self._enrich.get_multi_enrollment = MagicMock(side_effect=multi_enrollments_side_effect)

        bots = {'aaaaa': False}

        def bots_side_effect(uuid):
            return bots[uuid]
        self._enrich.is_bot = MagicMock(side_effect=bots_side_effect)

        p1 = {
            "name": expected['author_name'],
            "email": "pepepotamo@local.com",
        }
        sh_prof_vals = {expected['author_uuid']: p1}

        def profile_side_effect(uuid):
            return sh_prof_vals[uuid]
        self._enrich.get_profile_sh = MagicMock(side_effect=profile_side_effect)

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

        uuid_vals = {expected['author_id']: expected['author_uuid']}

        def uuid_side_effect(from_id):
            return uuid_vals[from_id]
        self._enrich.get_uuid_from_id = MagicMock(side_effect=uuid_side_effect)

        p1 = {
            "name": expected['author_name'],
            "email": "pepepotamo@local.com",
            "gender": expected['author_gender'],
            "gender_acc": expected['author_gender_acc']
        }
        sh_prof_vals = {expected['author_uuid']: p1}

        def profile_side_effect(uuid):
            return sh_prof_vals[uuid]
        self._enrich.get_profile_sh = MagicMock(side_effect=profile_side_effect)

        email_doms = {'pepepotamo@local.com': expected['author_domain']}

        def email_domain_side_effect(email):
            return email_doms[email]
        self._enrich.get_email_domain = MagicMock(side_effect=email_domain_side_effect)

        enrollments = {(expected['author_uuid'], None): expected['author_org_name']}

        def enrollments_side_effect(uuid, item_date):
            return enrollments[(uuid, item_date)]
        self._enrich.get_enrollment = MagicMock(side_effect=enrollments_side_effect)

        multi_enrollments = {(expected['author_uuid'], None): expected['author_multi_org_names']}

        def multi_enrollments_side_effect(uuid, item_date):
            return multi_enrollments[(uuid, item_date)]

        self._enrich.get_multi_enrollment = MagicMock(side_effect=multi_enrollments_side_effect)

        bots = {'aaaaa': False}

        def bots_side_effect(uuid):
            return bots[uuid]
        self._enrich.is_bot = MagicMock(side_effect=bots_side_effect)

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

        self._enrich.get_uuid_from_id = MagicMock(return_value=None)

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

        self._enrich.get_uuid_from_id = MagicMock(return_value="")

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

    def test_has_identities(self):
        """Test whether has_identities works"""

        self.assertTrue(self._enrich.has_identities())

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
        with self.assertLogs(logger, level='INFO') as cm:
            self._enrich.elastic.add_alias(DEMOGRAPHICS_ALIAS)

        self.assertEqual(cm.output[0],
                         'INFO:grimoire_elk.elastic:Alias %s created on %s.'
                         % (DEMOGRAPHICS_ALIAS, anonymize_url(tmp_index_url)))

        r = self._enrich.requests.get(self._enrich.elastic.index_url + "/_alias", headers=HEADER_JSON, verify=False)
        self.assertIn(DEMOGRAPHICS_ALIAS, r.json()[self._enrich.elastic.index]['aliases'])

        # add alias again
        with self.assertLogs(logger, level='DEBUG') as cm:
            self._enrich.elastic.add_alias(DEMOGRAPHICS_ALIAS)

        self.assertEqual(cm.output[0],
                         'DEBUG:grimoire_elk.elastic:Alias %s already exists on %s.'
                         % (DEMOGRAPHICS_ALIAS, anonymize_url(tmp_index_url)))

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


if __name__ == '__main__':
    unittest.main()
