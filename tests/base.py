#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2017 Bitergia
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
# Foundation, 51 Franklin Street, Fifth Floor, Boston, MA 02110-1335, USA.
#
# Authors:
#     Alvaro del Castillo <acs@bitergia.com>
#     Valerio Cosentino <valcos@bitergia.com>
#

import configparser
import logging
import unittest

from datetime import datetime

CONFIG_FILE = 'tests.conf.sample'
DB_SORTINGHAT = "test_sh"
DB_PROJECTS = "test_projects"


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


def data2es(items, ocean):
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


class TestCaseBackend(unittest.TestCase):
    """Unit tests for Backend"""

    def setUp(self):
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        self.es_con = dict(config.items('ElasticSearch'))['url']

        self.db_user = ''
        self.db_password = ''
        if 'Database' in config:
            if 'user' in config['Database']:
                self.db_user = config['Database']['user']
            if 'password' in config['Database']:
                self.db_password = config['Database']['password']
