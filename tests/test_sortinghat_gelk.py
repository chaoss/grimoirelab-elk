#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021-2023 Bitergia
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
#     Quan Zhou <quan@bitergia.com>
#


import configparser
import copy
import json
import logging
import unittest

from grimoire_elk.enriched.sortinghat_gelk import SortingHat
from sgqlc.operation import Operation
from sortinghat.cli.client import SortingHatClient, SortingHatSchema


CONFIG_FILE = 'tests.conf'
REMOTE_IDENTITIES_FILE = 'data/remote_identities_sortinghat.json'


def read_file(filename, mode='r'):
    with open(filename, mode) as f:
        content = f.read()
    return content


class TestSortinghatGelk(unittest.TestCase):
    @staticmethod
    def get_organizations(client):
        args = {
            "page": 1,
            "page_size": 10
        }
        op = Operation(SortingHatSchema.Query)
        org = op.organizations(**args)
        org.entities().name()
        result = client.execute(op)
        organizations = result['data']['organizations']['entities']

        return organizations

    @staticmethod
    def delete_identity(client, args):
        op = Operation(SortingHatSchema.SortingHatMutation)
        identity = op.delete_identity(**args)
        identity.uuid()
        client.execute(op)

    @staticmethod
    def delete_organization(client, args):
        op = Operation(SortingHatSchema.SortingHatMutation)
        org = op.delete_organization(**args)
        org.organization.name()
        client.execute(op)

    @staticmethod
    def add_identity(client, args):
        op = Operation(SortingHatSchema.SortingHatMutation)
        identity = op.add_identity(**args)
        identity.uuid()
        client.execute(op)

    @staticmethod
    def add_organization(client, args):
        op = Operation(SortingHatSchema.SortingHatMutation)
        org = op.add_organization(**args)
        org.organization.name()
        client.execute(op)

    @staticmethod
    def add_domain(client, args):
        op = Operation(SortingHatSchema.SortingHatMutation)
        dom = op.add_domain(**args)
        dom.domain.domain()
        client.execute(op)

    def setUp(self):
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)

        # Sorting hat settings
        db_user = config.get('Database', 'user', fallback='')
        db_password = config.get('Database', 'password', fallback='')
        db_host = config.get('Database', 'host', fallback='127.0.0.1')
        db_port = config.get('Database', 'port', fallback=None)
        db_path = config.get('Database', 'path', fallback=None)
        db_ssl = config.getboolean('Database', 'ssl', fallback=False)
        db_verify_ssl = config.getboolean('Database', 'verify_ssl', fallback=True)
        db_tenant = config.get('Database', 'tenant', fallback=None)

        self.sh_db = SortingHatClient(host=db_host,
                                      port=db_port,
                                      user=db_user,
                                      password=db_password,
                                      path=db_path,
                                      ssl=db_ssl,
                                      verify_ssl=db_verify_ssl,
                                      tenant=db_tenant)
        self.sh_db.connect()
        # Clean database
        # Remove identities
        entities = SortingHat.unique_identities(self.sh_db)
        if entities:
            mks = [e['mk'] for e in entities]
            for i in mks:
                arg = {
                    'uuid': i
                }
                self.delete_identity(self.sh_db, arg)

            # Remove organization
            organizations = self.get_organizations(self.sh_db)
            for org in organizations:
                self.delete_organization(self.sh_db, org)

        # Load test data
        data = json.loads(read_file("data/task-identities-data.json"))

        identities = data['identities']
        for identity in identities:
            self.add_identity(self.sh_db, identity)

        organizations = data['organizations']
        for org in organizations:
            self.add_organization(self.sh_db, {"name": org['organization']})
            self.add_domain(self.sh_db, org)

    def test_add_enrollment(self):
        uuid = "39cfb450296d57f61bf75b07b9da49123943c111"
        org = "org1"
        self.assertEqual(SortingHat.add_enrollment(self.sh_db, uuid, org), org)

        _ = SortingHat.get_enrollments(self.sh_db, uuid)
        args = {
            'filters': {
                'uuid': uuid
            }
        }
        op = Operation(SortingHatSchema.Query)
        op.individuals(**args)
        individual = op.individuals().entities()
        individual.enrollments().group().name()
        result = self.sh_db.execute(op)
        entity = result['data']['individuals']['entities'][0]
        self.assertEqual(entity['enrollments'][0]['group']['name'], org)

    def test_add_id(self):
        new_identity = {
            'source': 'git',
            'email': 'test@test.com',
            'name': 'test_add_id',
            'username': 'test_add_id'
        }

        args = copy.deepcopy(new_identity)
        args['db'] = self.sh_db

        uuid = SortingHat.add_id(**args)

        ind_args = {
            'filters': {
                'uuid': uuid
            }
        }
        op = Operation(SortingHatSchema.Query)
        op.individuals(**ind_args)
        individual = op.individuals().entities().identities()
        individual.name()
        individual.email()
        individual.username()
        individual.source()
        result = self.sh_db.execute(op)
        identity = result['data']['individuals']['entities'][0]['identities'][0]

        self.assertDictEqual(identity, new_identity)

    def test_add_organization(self):
        new_org = "test_org"
        self.assertEqual(SortingHat.add_organization(self.sh_db, new_org), new_org)

        args = {
            'page': 1,
            'page_size': 1,
            'filters': {
                'name': new_org
            }
        }
        op = Operation(SortingHatSchema.Query)
        op.organizations(**args)
        op.organizations().entities().name()
        result = self.sh_db.execute(op)
        org = result['data']['organizations']['entities'][0]['name']
        self.assertEqual(org, new_org)

    def test_update_profile(self):
        uuid = "39cfb450296d57f61bf75b07b9da49123943c111"
        origin_args = {
            'filters': {
                'uuid': uuid
            }
        }
        op = Operation(SortingHatSchema.Query)
        op.individuals(**origin_args)
        individual = op.individuals().entities().profile()
        individual.name()
        individual.email()
        result = self.sh_db.execute(op)
        origin_identity = result['data']['individuals']['entities'][0]['profile']

        update = {
            'name': 'new_name',
            'email': 'new@new.com'
        }
        SortingHat.update_profile(self.sh_db, uuid, update)
        op = Operation(SortingHatSchema.Query)
        op.individuals(**origin_args)
        individual = op.individuals().entities().profile()
        individual.name()
        individual.email()
        result = self.sh_db.execute(op)
        new_identity = result['data']['individuals']['entities'][0]['profile']

        self.assertDictEqual(new_identity, update)
        self.assertNotEqual(new_identity['name'], origin_identity['name'])
        self.assertNotEqual(new_identity['email'], origin_identity['email'])

    def test_get_enrollements(self):
        uuid = "39cfb450296d57f61bf75b07b9da49123943c111"
        org = "org1"
        self.assertEqual(SortingHat.add_enrollment(self.sh_db, uuid, org), org)
        enroll = SortingHat.get_enrollments(self.sh_db, uuid)

        self.assertEqual(enroll[0]['group']['name'], org)

    def test_get_entity(self):
        from_uuid = "3942d2eb291ec68dee659692f5d07cb2df9c74ce"
        to_uuid = "39cfb450296d57f61bf75b07b9da49123943c111"

        op = Operation(SortingHatSchema.SortingHatMutation)
        merge = op.merge(from_uuids=[from_uuid], to_uuid=to_uuid)
        merge.uuid()
        self.sh_db.execute(op)

        entity = SortingHat.get_entity(self.sh_db, from_uuid)
        self.assertEqual(entity['mk'], to_uuid)
        self.assertEqual(len(entity['identities']), 2)

    def test_get_unique_identity(self):
        uuid = "39cfb450296d57f61bf75b07b9da49123943c111"
        name = 'user9'
        email = 'user9@org2.com'
        unique_identity = SortingHat.get_unique_identity(self.sh_db, uuid)
        self.assertEqual(unique_identity['name'], name)
        self.assertEqual(unique_identity['email'], email)

    def test_get_uuid_from_id(self):
        from_uuid = "3942d2eb291ec68dee659692f5d07cb2df9c74ce"
        to_uuid = "39cfb450296d57f61bf75b07b9da49123943c111"

        op = Operation(SortingHatSchema.SortingHatMutation)
        merge = op.merge(from_uuids=[from_uuid], to_uuid=to_uuid)
        merge.uuid()
        self.sh_db.execute(op)

        uuid = SortingHat.get_uuid_from_id(self.sh_db, from_uuid)
        self.assertEqual(uuid, to_uuid)

    def test_is_bot(self):
        uuid = "39cfb450296d57f61bf75b07b9da49123943c111"
        self.assertFalse(SortingHat.is_bot(self.sh_db, uuid))

        # Update the profile and set "is_bot = True"
        update = {
            'is_bot': True,
        }
        SortingHat.update_profile(self.sh_db, uuid, update)
        self.assertTrue(SortingHat.is_bot(self.sh_db, uuid))

    def test_remove_identity(self):
        uuid = "39cfb450296d57f61bf75b07b9da49123943c111"
        entities_before = SortingHat.unique_identities(self.sh_db)

        SortingHat.remove_identity(self.sh_db, uuid)
        entities_after = SortingHat.unique_identities(self.sh_db)

        mks_before = [mk['mk'] for mk in entities_before]
        mks_after = [mk['mk'] for mk in entities_after]

        self.assertLess(len(entities_after), len(entities_before))
        self.assertIn(uuid, mks_before)
        self.assertNotIn(uuid, mks_after)

    def test_unique_identities(self):
        entities_before = SortingHat.unique_identities(self.sh_db)

        from_uuid = "3942d2eb291ec68dee659692f5d07cb2df9c74ce"
        to_uuid = "39cfb450296d57f61bf75b07b9da49123943c111"
        op = Operation(SortingHatSchema.SortingHatMutation)
        merge = op.merge(from_uuids=[from_uuid], to_uuid=to_uuid)
        merge.uuid()
        self.sh_db.execute(op)

        entities_after = SortingHat.unique_identities(self.sh_db)

        mks_before = [mk['mk'] for mk in entities_before]
        mks_after = [mk['mk'] for mk in entities_after]

        self.assertLess(len(entities_after), len(entities_before))
        self.assertIn(from_uuid, mks_before)
        self.assertNotIn(from_uuid, mks_after)

    def test_get_uuids_from_profile_name(self):
        user = "user9"
        mks = SortingHat.get_uuids_from_profile_name(self.sh_db, user)
        expected_uuid = "39cfb450296d57f61bf75b07b9da49123943c111"
        self.assertEqual(mks[0], expected_uuid)

        updated_uuid = "4fa3da3bd918171fd667592b7bdebc6438768b60"
        update = {
            'name': 'user9'
        }
        SortingHat.update_profile(self.sh_db, updated_uuid, update)

        mks = SortingHat.get_uuids_from_profile_name(self.sh_db, user)
        self.assertEqual(len(mks), 2)
        self.assertListEqual(mks, [expected_uuid, updated_uuid])

    def test_do_affiliation(self):
        self.assertIsNone(SortingHat.do_affiliate(self.sh_db))

        args = {
            'page': 1,
            'page_size': 10
        }
        op = Operation(SortingHatSchema.Query)
        op.individuals(**args)
        individual = op.individuals().entities()
        individual.profile().name()
        individual.enrollments().group().name()
        result = self.sh_db.execute(op)
        entities = result['data']['individuals']['entities']

        enrolls = {}
        for e in entities:
            name = e['profile']['name']
            enroll = e['enrollments'][0]['group']['name']
            if name in enrolls:
                enrolls[name].append(enroll)
            else:
                enrolls[name] = [enroll]

        expected_enrolls = {
            "user1": ["org1"],
            "user2": ["org2"],
            "user3": ["org1"],
            "user4": ["org2", "org3"],
            "user6": ["org6"],
            "user7": ["org1"],
            "user8": ["org8"],
            "user9": ["org2"],
            "user10": ["org3"]
        }

        self.assertDictEqual(enrolls, expected_enrolls)

    def test_do_unify(self):
        kwargs = {
            'matching': "email",
            'fast_matching': True
        }
        self.assertIsNone(SortingHat.do_unify(self.sh_db, kwargs))

        args = {
            'page': 1,
            'page_size': 10
        }
        op = Operation(SortingHatSchema.Query)
        op.individuals(**args)
        individual = op.individuals().entities()
        individual.mk()
        result = self.sh_db.execute(op)
        entities = result['data']['individuals']['entities']

        self.assertEqual(len(entities), 9)

    def test_do_unify_wrong_criteria(self):
        kwargs = {
            'matching': "email-name",
            'fast_matching': True
        }
        with self.assertLogs() as captured:
            SortingHat.do_unify(self.sh_db, kwargs)
        expected_log = "[sortinghat] Error unify criteria: ['email-name']"
        first_line_error = captured.records[1].getMessage().split("\n")[0]
        self.assertEqual(first_line_error, expected_log)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
    unittest.main()
