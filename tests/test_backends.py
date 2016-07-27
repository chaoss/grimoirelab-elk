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
import os
import sys
import unittest

if not '..' in sys.path:
    sys.path.insert(0, '..')

from grimoire.utils import get_connectors

CONFIG_FILE = 'tests.conf'
NUMBER_BACKENDS = 19

class TestBackends(unittest.TestCase):
    """Functional tests for GrimoireELK Backends"""

    def test_init(self):
        """Test whether the backends can be loaded """
        self.assertEqual(len(get_connectors()), NUMBER_BACKENDS)

    def test_load(self):
        """Test load all sources JSON data into ES"""
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        es = dict(config.items('ElasticSearch'))
        connectors = get_connectors()
        # Check we have config for all the connectors
        for con in sorted(connectors.keys()):
            with open(os.path.join("data", con + ".json")) as f:
                data_json = json.load(f)

    def test_enrich(self):
        """Test enrich all sources"""
        pass




if __name__ == "__main__":
    unittest.main()
