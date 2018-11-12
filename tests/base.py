#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2016 Bitergia
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
#     Valerio Cosentino <valcos@bitergia.com>
#

import configparser
import json
import os
import requests
import sys
import unittest

from elasticsearch import Elasticsearch

if '..' not in sys.path:
    sys.path.insert(0, '..')

from grimoire_elk.elk import load_identities
from grimoire_elk.utils import get_connectors, get_elastic
from tests.model import ESMapping

CONFIG_FILE = 'tests.conf'
DB_SORTINGHAT = "test_sh"
DB_PROJECTS = "test_projects"
FILE_PROJECTS = "data/projects-release.json"
SCHEMA_DIR = '../schema/'


def load_mapping(enrich_index, csv_name):

    cvs_path = os.path.join(SCHEMA_DIR, csv_name + '.csv')
    cvs_mapping = ESMapping.from_csv(enrich_index, cvs_path)

    return cvs_mapping


def refresh_identities(enrich_backend):
    total = 0

    for eitem in enrich_backend.fetch():
        roles = None
        try:
            roles = enrich_backend.roles
        except AttributeError:
            pass
        new_identities = enrich_backend.get_item_sh_from_id(eitem, roles)
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
        cls.db_user = ''
        cls.db_password = ''
        if 'Database' in cls.config:
            if 'user' in cls.config['Database']:
                cls.db_user = cls.config['Database']['user']
            if 'password' in cls.config['Database']:
                cls.db_password = cls.config['Database']['password']

    def setUp(self):
        with open(os.path.join("data", self.connector + ".json")) as f:
            self.items = json.load(f)

    def tearDown(self):
        delete_raw = self.es_con + "/" + self.ocean_index
        requests.delete(delete_raw, verify=False)

        delete_enrich = self.es_con + "/" + self.enrich_index
        requests.delete(delete_enrich, verify=False)

    def _test_items_to_raw(self):
        """Test whether fetched items are properly loaded to ES"""

        clean = True
        perceval_backend = None
        ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)

        raw_items = ocean_backend.feed_items(self.items)
        return {'items': len(self.items), 'raw': raw_items}

    def _test_raw_to_enrich(self, sortinghat=False, projects=False):
        """Test whether raw indexes are properly enriched"""

        # populate raw index
        perceval_backend = None
        clean = True
        ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)
        ocean_backend.feed_items(self.items)

        # populate enriched index
        if not sortinghat and not projects:
            enrich_backend = self.connectors[self.connector][2]()
        elif sortinghat and not projects:
            enrich_backend = self.connectors[self.connector][2](db_sortinghat=DB_SORTINGHAT,
                                                                db_user=self.db_user,
                                                                db_password=self.db_password)
        elif not sortinghat and projects:
            enrich_backend = self.connectors[self.connector][2](json_projects_map=FILE_PROJECTS,
                                                                db_user=self.db_user,
                                                                db_password=self.db_password)

        elastic_enrich = get_elastic(self.es_con, self.enrich_index, clean, enrich_backend)
        enrich_backend.set_elastic(elastic_enrich)

        # Load SH identities
        if sortinghat:
            load_identities(ocean_backend, enrich_backend)

        raw_count = len([item for item in ocean_backend.fetch()])
        enrich_count = enrich_backend.enrich_items(ocean_backend)
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
        ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)
        ocean_backend.feed_items(self.items)

        # populate enriched index
        enrich_backend = self.connectors[self.connector][2]()
        load_identities(ocean_backend, enrich_backend)
        enrich_backend = self.connectors[self.connector][2](db_sortinghat=DB_SORTINGHAT,
                                                            db_user=self.db_user,
                                                            db_password=self.db_password)
        elastic_enrich = get_elastic(self.es_con, self.enrich_index, clean, enrich_backend)
        enrich_backend.set_elastic(elastic_enrich)
        enrich_backend.enrich_items(ocean_backend)

        total = refresh_identities(enrich_backend)
        return total

    def _test_refresh_project(self):
        """Test refresh project field"""

        # populate raw index
        perceval_backend = None
        clean = True
        ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)
        ocean_backend.feed_items(self.items)

        # populate enriched index
        enrich_backend = self.connectors[self.connector][2](db_projects_map=DB_PROJECTS,
                                                            db_user=self.db_user,
                                                            db_password=self.db_password)

        elastic_enrich = get_elastic(self.es_con, self.enrich_index, clean, enrich_backend)
        enrich_backend.set_elastic(elastic_enrich)
        enrich_backend.enrich_items(ocean_backend)

        total = refresh_projects(enrich_backend)
        return total

    def _test_study(self, test_study):
        """Test the execution of a study"""

        # populate raw index
        perceval_backend = None
        clean = True
        ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)
        ocean_backend.feed_items(self.items)

        # populate enriched index
        enrich_backend = self.connectors[self.connector][2](db_sortinghat=DB_SORTINGHAT,
                                                            db_user=self.db_user,
                                                            db_password=self.db_password)

        elastic_enrich = get_elastic(self.es_con, self.enrich_index, clean, enrich_backend)
        enrich_backend.set_elastic(elastic_enrich)
        enrich_backend.enrich_items(ocean_backend)

        found = None
        for study in enrich_backend.studies:
            if test_study == study.__name__:
                found = (study, ocean_backend, enrich_backend)
                break

        return found
