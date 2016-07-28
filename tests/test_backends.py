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
import sys
import unittest

from datetime import datetime

if not '..' in sys.path:
    sys.path.insert(0, '..')

from grimoire.utils import get_connectors, get_elastic

CONFIG_FILE = 'tests.conf'
NUMBER_BACKENDS = 19
DB_SORTINGHAT = "test_sh"
DB_PROJECTS = "test_projects"

class TestBackends(unittest.TestCase):
    """Functional tests for GrimoireELK Backends"""

    def test_init(self):
        """Test whether the backends can be loaded """
        self.assertEqual(len(get_connectors()), NUMBER_BACKENDS)

    def test_read_data(self):
        """Test load all sources JSON"""
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        connectors = get_connectors()
        # Check we have data for all the data sources
        for con in sorted(connectors.keys()):
            with open(os.path.join("data", con + ".json")) as f:
                json.load(f)

    def __ocean_item(self, item):
        # Hack until we decide the final id to use
        if 'uuid' in item:
            item['ocean-unique-id'] = item['uuid']
        else:
            # twitter comes from logstash and uses id
            item['ocean-unique-id'] = item['id']

        # Hack until we decide when to drop this field
        if 'updated_on' in item:
            updated = datetime.fromtimestamp(item['updated_on'])
            item['metadata__updated_on'] = updated.isoformat()
        return item

    def __data2es(self, items, ocean):
        items_pack = []  # to feed item in packs

        for item in items:
            item = self.__ocean_item(item)
            if len(items_pack) >= ocean.elastic.max_items_bulk:
                ocean._items_to_es(items_pack)
                items_pack = []
            items_pack.append(item)
        ocean._items_to_es(items_pack)

    def __enrich_items(self, items, enrich_backend):
        total = 0

        items_pack = []

        for item in items:
            item = self.__ocean_item(item)
            if len(items_pack) >= enrich_backend.elastic.max_items_bulk:
                logging.info("Adding %i (%i done) enriched items to %s",
                             enrich_backend.elastic.max_items_bulk, total,
                             enrich_backend.elastic.index_url)
                enrich_backend.enrich_items(items_pack)
                items_pack = []
            items_pack.append(item)
            total += 1
        enrich_backend.enrich_items(items_pack)

        return total

    def test_data_load(self):
        """Test load all sources JSON data into ES"""
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        es_con = dict(config.items('ElasticSearch'))['url']
        logging.info("Loading data in: %s", es_con)
        connectors = get_connectors()
        for con in sorted(connectors.keys()):
            with open(os.path.join("data", con + ".json")) as f:
                items = json.load(f)
                es_index = "test_"+con
                clean = True
                perceval_backend = None
                ocean_backend = connectors[con][1](perceval_backend)
                elastic_ocean = get_elastic(es_con, es_index, clean, ocean_backend)
                ocean_backend.set_elastic(elastic_ocean)
                self.__data2es(items, ocean_backend)

    def test_enrich(self, sortinghat=False, projects=False):
        """Test enrich all sources"""
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        es_con = dict(config.items('ElasticSearch'))['url']
        logging.info("Enriching data in: %s", es_con)
        connectors = get_connectors()
        for con in sorted(connectors.keys()):
            perceval_backend = None
            ocean_index = "test_"+con
            enrich_index = "test_"+con+"_enrich"
            clean = False
            ocean_backend = connectors[con][1](perceval_backend)
            elastic_ocean = get_elastic(es_con, ocean_index, clean, ocean_backend)
            ocean_backend.set_elastic(elastic_ocean)
            clean = True
            if not sortinghat and not projects:
                enrich_backend = connectors[con][2](perceval_backend)
            elif sortinghat and not projects:
                enrich_backend = connectors[con][2](perceval_backend,
                                                    db_sortinghat=DB_SORTINGHAT)
            elif not sortinghat and projects:
                enrich_backend = connectors[con][2](perceval_backend,
                                                    db_projects_map=DB_PROJECTS)
            elastic_enrich = get_elastic(es_con, enrich_index, clean, enrich_backend)
            enrich_backend.set_elastic(elastic_enrich)
            enrich_count = self.__enrich_items(ocean_backend, enrich_backend)
            logging.info("Total items enriched %i ", enrich_count)

    def test_enrich_sh(self):
        """Test enrich all sources with SortingHat"""

        self.test_enrich(sortinghat=True)

    def test_enrich_projects(self):
        """Test enrich all sources with Projects"""

        self.test_enrich(projects=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    unittest.main()
