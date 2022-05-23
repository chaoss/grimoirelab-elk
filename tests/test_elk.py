# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 Bitergia
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
#   Quan Zhou <quan@bitergia.com>
#

import configparser
import unittest

from grimoire_elk.elk import anonymize_params


CONFIG_FILE = 'tests.conf'


class TestElk(unittest.TestCase):
    """Unit tests for elk class"""

    @classmethod
    def setUpClass(cls):
        cls.config = configparser.ConfigParser()
        cls.config.read(CONFIG_FILE)
        cls.es_con = dict(cls.config.items('ElasticSearch'))['url']

    def test_anonymize_params(self):
        """Test anonymize parameters"""

        expected_params = ("repo", "--api-token", "xxxxx", "xxxxx", "--sleep-for-rate")
        params = ("repo", "--api-token", "token1", "token2", "--sleep-for-rate")
        anonymized_params = anonymize_params(params)
        self.assertTupleEqual(anonymized_params, expected_params)

        expected_params = ("--backend-password", "xxxxx", "--no-archive", "--api-token", "xxxxx")
        params = ("--backend-password", "mypassword", "--no-archive", "--api-token", "token")
        anonymized_params = anonymize_params(params)
        self.assertTupleEqual(anonymized_params, expected_params)
