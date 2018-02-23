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
#     Alvaro del Castillo <acs@bitergia.com>
#

import configparser
import json
import logging
import os
from os.path import isfile, join
import requests
import sys
import unittest

from datetime import datetime

if '..' not in sys.path:
    sys.path.insert(0, '..')

from grimoire_elk.arthur import load_identities
from grimoire_elk.utils import get_connectors, get_elastic


CONFIG_FILE = 'tests.conf'
DB_SORTINGHAT = "test_sh"
DB_PROJECTS = "test_projects"


def test_connector(all, connector):
    decision = False
    if all == 'true':
        decision = True
    elif all == 'false' and connector == 'true':
        decision = True

    return decision


def data2es(items, ocean):
    def ocean_item(item):
        # Hack until we decide the final id to use
        if 'uuid' in item:
            item['ocean-unique-id'] = item['uuid']
        else:
            # twitter comes from logstash and uses id
            item['uuid'] = item['id']
            item['ocean-unique-id'] = item['id']

        # Hack until we decide when to drop this field
        if 'updated_on' in item:
            updated = datetime.fromtimestamp(item['updated_on'])
            item['metadata__updated_on'] = updated.isoformat()
        if 'timestamp' in item:
            ts = datetime.fromtimestamp(item['timestamp'])
            item['metadata__timestamp'] = ts.isoformat()

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

    for eitem in enrich_backend.fetch():
        roles = None
        try:
            roles = enrich_backend.roles
        except AttributeError:
            pass
        new_identities = enrich_backend.get_item_sh_from_id(eitem, roles)
        eitem.update(new_identities)
        total += 1

    logging.info("Identities refreshed for %i eitems", total)


def refresh_projects(enrich_backend):
    total = 0

    for eitem in enrich_backend.fetch():
        new_project = enrich_backend.get_item_project(eitem)
        eitem.update(new_project)
        total += 1

    logging.info("Project refreshed for %i eitems", total)


class TestBackends(unittest.TestCase):
    """Functional tests for GrimoireELK Backends"""

    @classmethod
    def setUpClass(cls):
        cls.config = configparser.ConfigParser()
        cls.config.read(CONFIG_FILE)
        cls.es_con = dict(cls.config.items('ElasticSearch'))['url']
        cls.connectors = get_connectors()

        # Sorting hat settings
        cls.db_user = ''
        cls.db_password = ''
        if 'Database' in cls.config:
            if 'user' in cls.config['Database']:
                cls.db_user = cls.config['Database']['user']
            if 'password' in cls.config['Database']:
                cls.db_password = cls.config['Database']['password']

    @classmethod
    def tearDownClass(cls):
        """Delete indexes"""

        es_command = cls.es_con + "/test*"
        requests.delete(es_command)

    def test_connectors_presence(self):
        """Test whether all ocean connectors have their corresponding elk ones"""

        ocean_path = "../grimoire_elk/ocean"
        elk_path = "../grimoire_elk/elk"

        excluded = ['utils.py', "__init__.py", "sortinghat.py",
                    "mbox_study_kip.py", "elastic.py", "projects.py",
                    "enrich.py", "database.py", "conf.py",
                    "launchpad.py", "gitlab.py", "puppetforge.py"]

        ocean_connectors = [f.replace(".py", "") for f in os.listdir(ocean_path)
                            if isfile(join(ocean_path, f)) and f not in excluded]
        elk_connectors = [f.replace(".py", "") for f in os.listdir(elk_path)
                          if isfile(join(elk_path, f)) and f not in excluded]

        self.assertTrue(all([oc in self.connectors.keys() for oc in ocean_connectors]))
        self.assertTrue(all([ec in self.connectors.keys() for ec in elk_connectors]))

    def test_connectors_data(self):
        """Test all connectors have test data"""

        for con in sorted(self.connectors.keys()):
            with open(os.path.join("data", con + ".json")) as f:
                json.load(f)

    def test_items_to_es(self):
        """Test whether JSON items are properly inserted into ES"""

        check_connectors = dict(self.config.items('Connectors'))

        logging.info("Loading data in: %s", self.es_con)
        for con in sorted(self.connectors.keys()):

            if not test_connector(check_connectors['all'], check_connectors[con]):
                continue

            with open(os.path.join("data", con + ".json")) as f:
                items = json.load(f)
                es_index = "test_" + con
                clean = True
                perceval_backend = None
                ocean_backend = self.connectors[con][1](perceval_backend)
                elastic_ocean = get_elastic(self.es_con, es_index, clean, ocean_backend)
                ocean_backend.set_elastic(elastic_ocean)

                inserted = data2es(items, ocean_backend)
                self.assertEqual(len(items), inserted)

    def test_enrich_items(self, sortinghat=False, projects=False):
        """Test whether raw indexes are properly enriched"""

        check_connectors = dict(self.config.items('Connectors'))

        logging.info("Enriching data in: %s", self.es_con)
        for con in sorted(self.connectors.keys()):

            if not test_connector(check_connectors['all'], check_connectors[con]):
                continue

            perceval_backend = None
            ocean_index = "test_" + con
            enrich_index = "test_" + con + "_enrich"

            clean = False
            ocean_backend = self.connectors[con][1](perceval_backend)
            elastic_ocean = get_elastic(self.es_con, ocean_index, clean, ocean_backend)
            ocean_backend.set_elastic(elastic_ocean)
            clean = True

            if not sortinghat and not projects:
                enrich_backend = self.connectors[con][2]()
            elif sortinghat and not projects:
                enrich_backend = self.connectors[con][2](db_sortinghat=DB_SORTINGHAT,
                                                         db_user=self.db_user,
                                                         db_password=self.db_password)
            elif not sortinghat and projects:
                enrich_backend = self.connectors[con][2](db_projects_map=DB_PROJECTS,
                                                         db_user=self.db_user,
                                                         db_password=self.db_password)
            elastic_enrich = get_elastic(self.es_con, enrich_index, clean, enrich_backend)
            enrich_backend.set_elastic(elastic_enrich)

            # Load SH identities
            if sortinghat:
                load_identities(ocean_backend, enrich_backend)

            raw_count = len([item for item in ocean_backend.fetch()])
            enrich_count = enrich_backend.enrich_items(ocean_backend)

            self.assertEqual(raw_count, enrich_count)

    def test_enrich_sh(self):
        """Test enrich all sources with SortingHat"""

        self.test_enrich_items(sortinghat=True)

    def test_enrich_projects(self):
        """Test enrich all sources with Projects"""

        self.test_enrich_items(projects=True)

    def test_refresh_identities(self):
        """Test refresh identities for all sources"""

        logging.info("Refreshing data in: %s", self.es_con)
        for con in sorted(self.connectors.keys()):
            enrich_index = "test_" + con + "_enrich"
            enrich_backend = self.connectors[con][2](db_sortinghat=DB_SORTINGHAT,
                                                     db_user=self.db_user,
                                                     db_password=self.db_password)
            clean = False
            elastic_enrich = get_elastic(self.es_con, enrich_index, clean, enrich_backend)
            enrich_backend.set_elastic(elastic_enrich)

            logging.info("Refreshing identities fields in enriched index %s", elastic_enrich.index_url)
            refresh_identities(enrich_backend)

    def test_refresh_project(self):
        """Test refresh project field for all sources"""

        logging.info("Refreshing data in: %s", self.es_con)
        for con in sorted(self.connectors.keys()):
            enrich_index = "test_" + con + "_enrich"
            enrich_backend = self.connectors[con][2](db_projects_map=DB_PROJECTS,
                                                     db_user=self.db_user,
                                                     db_password=self.db_password)
            clean = False
            elastic_enrich = get_elastic(self.es_con, enrich_index, clean, enrich_backend)
            enrich_backend.set_elastic(elastic_enrich)
            logging.info("Refreshing projects fields in enriched index %s", elastic_enrich.index_url)
            refresh_projects(enrich_backend)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    unittest.main(warnings='ignore')
