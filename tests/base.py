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
import requests
import sys
import unittest
from datetime import datetime

from elasticsearch import Elasticsearch

from grimoire_elk.enriched.sortinghat_gelk import SortingHat

if '..' not in sys.path:
    sys.path.insert(0, '..')

from grimoire_elk.elk import load_identities
from grimoire_elk.utils import get_connectors, get_elastic

from tests.model import ESMapping

CONFIG_FILE = 'tests.conf'
DB_SORTINGHAT = "test_sh"
DB_PROJECTS = "test_projects"
DB_HOST = '127.0.0.1'
FILE_PROJECTS = "data/projects-release.json"
SCHEMA_DIR = '../schema/'


def load_mapping(enrich_index, csv_name):

    cvs_path = os.path.join(SCHEMA_DIR, csv_name + '.csv')
    cvs_mapping = ESMapping.from_csv(enrich_index, cvs_path)

    return cvs_mapping


def data2es(items, ocean):
    def ocean_item(item):
        # Hack until we decide when to drop this field
        if 'updated_on' in item:
            updated = datetime.fromtimestamp(item['updated_on'])
            item['metadata__updated_on'] = updated.isoformat()
        if 'timestamp' in item:
            ts = datetime.fromtimestamp(item['timestamp'])
            item['metadata__timestamp'] = ts.isoformat()

        # the _fix_item does not apply to the test data for Twitter
        try:
            ocean._fix_item(item)
        except KeyError:
            pass

        if ocean.anonymize:
            ocean.identities.anonymize_item(item)

        return item

    items_pack = []  # to feed item in packs

    for item in items:
        item = ocean_item(item)
        if len(items_pack) >= ocean.elastic.max_items_bulk:
            ocean._items_to_es(items_pack)
            items_pack = []
        items_pack.append(item)
    inserted = ocean._items_to_es(items_pack)

    return inserted


def refresh_identities(enrich_backend):
    total = 0

    after = datetime.fromtimestamp(0)
    individuals = []
    for indivs in SortingHat.search_last_modified_identities(enrich_backend.sh_db, after=after):
        individuals.extend(indivs)

    for eitem in enrich_backend.fetch():
        roles = None
        try:
            roles = enrich_backend.roles
        except AttributeError:
            pass
        new_identities = enrich_backend.get_item_sh_from_id(eitem, roles, individuals)
        eitem.update(new_identities)
        total += 1

    return total


def refresh_projects(enrich_backend):
    total = 0

    for eitem in enrich_backend.fetch():
        new_project = enrich_backend.get_item_project(eitem)
        eitem.update(new_project)
        total += 1

    return total


class TestBaseBackend(unittest.TestCase):
    """Functional tests for GrimoireELK Backends"""

    @classmethod
    def setUpClass(cls):
        cls.config = configparser.ConfigParser()
        cls.config.read(CONFIG_FILE)
        cls.es_con = dict(cls.config.items('ElasticSearch'))['url']
        cls.connectors = get_connectors()
        cls.maxDiff = None

        # Sorting hat settings
        cls.db_user = cls.config.get('Database', 'user', fallback='')
        cls.db_password = cls.config.get('Database', 'password', fallback='')
        cls.db_host = cls.config.get('Database', 'host', fallback=DB_HOST)
        cls.db_port = cls.config.get('Database', 'port', fallback=None)
        cls.db_path = cls.config.get('Database', 'path', fallback=None)
        cls.db_ssl = cls.config.getboolean('Database', 'ssl', fallback=False)
        cls.db_verify_ssl = cls.config.getboolean('Database', 'verify_ssl', fallback=True)
        cls.db_tenant = cls.config.get('Database', 'tenant', fallback=None)

    def setUp(self):
        with open(os.path.join("data", self.connector + ".json")) as f:
            self.items = json.load(f)

        self.ocean_backend = None
        self.enrich_backend = None
        self.ocean_aliases = []
        self.enrich_aliases = []

    def tearDown(self):
        delete_test_idx = self.es_con + "/" + 'test*'
        requests.delete(delete_test_idx, verify=False)

    def _test_items_to_raw(self):
        """Test whether fetched items are properly loaded to ES"""

        clean = True
        perceval_backend = None
        self.ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, self.ocean_backend, self.ocean_aliases)
        self.ocean_backend.set_elastic(elastic_ocean)

        raw_items = data2es(self.items, self.ocean_backend)

        return {'items': len(self.items), 'raw': raw_items}

    def _test_raw_to_enrich(self, sortinghat=False, projects=False, pair_programming=False):
        """Test whether raw indexes are properly enriched"""

        # populate raw index
        perceval_backend = None
        clean = True
        self.ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, self.ocean_backend)
        self.ocean_backend.set_elastic(elastic_ocean)
        data2es(self.items, self.ocean_backend)

        # populate enriched index
        if not sortinghat and not projects:
            self.enrich_backend = self.connectors[self.connector][2]()
        elif sortinghat and not projects:
            self.enrich_backend = self.connectors[self.connector][2](db_sortinghat=DB_SORTINGHAT,
                                                                     db_user=self.db_user,
                                                                     db_password=self.db_password,
                                                                     db_host=self.db_host,
                                                                     db_port=self.db_port,
                                                                     db_path=self.db_path,
                                                                     db_ssl=self.db_ssl,
                                                                     db_verify_ssl=self.db_verify_ssl,
                                                                     db_tenant=self.db_tenant)
        elif sortinghat and projects:
            self.enrich_backend = self.connectors[self.connector][2](json_projects_map=FILE_PROJECTS,
                                                                     db_sortinghat=DB_SORTINGHAT,
                                                                     db_user=self.db_user,
                                                                     db_password=self.db_password,
                                                                     db_host=self.db_host,
                                                                     db_port=self.db_port,
                                                                     db_path=self.db_path,
                                                                     db_ssl=self.db_ssl,
                                                                     db_verify_ssl=self.db_verify_ssl,
                                                                     db_tenant=self.db_tenant)

        elif not sortinghat and projects:
            self.enrich_backend = self.connectors[self.connector][2](json_projects_map=FILE_PROJECTS,
                                                                     db_user=self.db_user,
                                                                     db_password=self.db_password,
                                                                     db_host=self.db_host,
                                                                     db_port=self.db_port,
                                                                     db_path=self.db_path,
                                                                     db_ssl=self.db_ssl,
                                                                     db_verify_ssl=self.db_verify_ssl,
                                                                     db_tenant=self.db_tenant)
        if pair_programming:
            self.enrich_backend.pair_programming = pair_programming

        elastic_enrich = get_elastic(self.es_con, self.enrich_index, clean, self.enrich_backend, self.enrich_aliases)
        self.enrich_backend.set_elastic(elastic_enrich)

        # Load SH identities
        if sortinghat:
            load_identities(self.ocean_backend, self.enrich_backend)

        raw_count = len([item for item in self.ocean_backend.fetch()])
        enrich_count = self.enrich_backend.enrich_items(self.ocean_backend)
        # self._test_csv_mappings(sortinghat)

        return {'raw': raw_count, 'enrich': enrich_count}

    def _test_csv_mappings(self, sortinghat):
        """Test whether the mappings in the CSV are successfully met"""

        result = {}

        if not sortinghat:
            return result

        csv_mapping = load_mapping(self.enrich_index, self.connector)
        client = Elasticsearch(self.es_con, timeout=30)
        mapping_json = client.indices.get_mapping(index=self.enrich_index)
        es_mapping = ESMapping.from_json(index_name=self.enrich_index,
                                         mapping_json=mapping_json)

        result = csv_mapping.compare_properties(es_mapping)
        self.assertEqual(result['msg'], "")

    def _test_refresh_identities(self):
        """Test refresh identities"""

        # populate raw index
        perceval_backend = None
        clean = True
        self.ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, self.ocean_backend)
        self.ocean_backend.set_elastic(elastic_ocean)
        data2es(self.items, self.ocean_backend)

        # populate enriched index
        self.enrich_backend = self.connectors[self.connector][2]()
        load_identities(self.ocean_backend, self.enrich_backend)
        self.enrich_backend = self.connectors[self.connector][2](db_sortinghat=DB_SORTINGHAT,
                                                                 db_user=self.db_user,
                                                                 db_password=self.db_password,
                                                                 db_host=self.db_host,
                                                                 db_port=self.db_port,
                                                                 db_path=self.db_path,
                                                                 db_ssl=self.db_ssl,
                                                                 db_verify_ssl=self.db_verify_ssl,
                                                                 db_tenant=self.db_tenant)
        elastic_enrich = get_elastic(self.es_con, self.enrich_index, clean, self.enrich_backend)
        self.enrich_backend.set_elastic(elastic_enrich)
        self.enrich_backend.enrich_items(self.ocean_backend)

        total = refresh_identities(self.enrich_backend)
        return total

    def _test_study(self, test_study, projects_json_repo=None, projects_json=None, prjs_map=None):
        """Test the execution of a study"""

        # populate raw index
        perceval_backend = None
        clean = True
        self.ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, self.ocean_backend)
        self.ocean_backend.set_elastic(elastic_ocean)
        data2es(self.items, self.ocean_backend)

        # populate enriched index
        self.enrich_backend = self.connectors[self.connector][2](db_sortinghat=DB_SORTINGHAT,
                                                                 db_user=self.db_user,
                                                                 db_password=self.db_password,
                                                                 json_projects_map=FILE_PROJECTS,
                                                                 db_host=self.db_host,
                                                                 db_port=self.db_port,
                                                                 db_path=self.db_path,
                                                                 db_ssl=self.db_ssl,
                                                                 db_verify_ssl=self.db_verify_ssl,
                                                                 db_tenant=self.db_tenant)

        elastic_enrich = get_elastic(self.es_con, self.enrich_index, clean, self.enrich_backend)
        self.enrich_backend.set_elastic(elastic_enrich)

        if projects_json:
            self.enrich_backend.json_projects = projects_json

        if projects_json_repo:
            self.enrich_backend.projects_json_repo = projects_json_repo

        if prjs_map:
            self.enrich_backend.prjs_map = prjs_map

        self.enrich_backend.enrich_items(self.ocean_backend)

        for study in self.enrich_backend.studies:
            if test_study == study.__name__:
                found = (study, self.ocean_backend, self.enrich_backend)
                break

        return found

    def _test_items_to_raw_anonymized(self):
        clean = True
        perceval_backend = None
        self.ocean_backend = self.connectors[self.connector][1](perceval_backend, anonymize=True)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index_anonymized, clean, self.ocean_backend,
                                    self.ocean_aliases)
        self.ocean_backend.set_elastic(elastic_ocean)

        raw_items = data2es(self.items, self.ocean_backend)

        return {'items': len(self.items), 'raw': raw_items}

    def _test_raw_to_enrich_anonymized(self, sortinghat=False, projects=False):
        """Test whether raw indexes are properly enriched"""

        # populate raw index
        perceval_backend = None
        clean = True
        self.ocean_backend = self.connectors[self.connector][1](perceval_backend, anonymize=True)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index_anonymized, clean, self.ocean_backend)
        self.ocean_backend.set_elastic(elastic_ocean)
        data2es(self.items, self.ocean_backend)

        # populate enriched index
        self.enrich_backend = self.connectors[self.connector][2](db_sortinghat=DB_SORTINGHAT,
                                                                 db_user=self.db_user,
                                                                 db_password=self.db_password,
                                                                 db_host=self.db_host,
                                                                 db_port=self.db_port,
                                                                 db_path=self.db_path,
                                                                 db_ssl=self.db_ssl,
                                                                 db_verify_ssl=self.db_verify_ssl,
                                                                 db_tenant=self.db_tenant)

        elastic_enrich = get_elastic(self.es_con, self.enrich_index_anonymized, clean, self.enrich_backend, self.enrich_aliases)
        self.enrich_backend.set_elastic(elastic_enrich)

        raw_count = len([item for item in self.ocean_backend.fetch()])
        enrich_count = self.enrich_backend.enrich_items(self.ocean_backend)

        return {'raw': raw_count, 'enrich': enrich_count}
