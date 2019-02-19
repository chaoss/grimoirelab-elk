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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
#

import logging
import sys
import unittest

import httpretty

if '..' not in sys.path:
    sys.path.insert(0, '..')

from grimoire_elk.elastic import ElasticSearch, ElasticConnectException


class TestElasticSearch(unittest.TestCase):
    """Functional unit tests for ElasticSearch class"""

    def setUp(self):

        self.body_es5 = """{
            "name" : "Amber Hunt",
            "cluster_name" : "jgbarah",
            "version" : {
              "number" : "5.0.0-alpha2",
              "build_hash" : "e3126df",
              "build_date" : "2016-04-26T12:08:58.960Z",
              "build_snapshot" : false,
              "lucene_version" : "6.0.0"
              },
              "tagline" : "You Know, for Search"
            }"""

        self.body_es6 = """{
          "name" : "44BPNNH",
          "cluster_name" : "elasticsearch",
          "cluster_uuid" : "fIa1j8AQRfSrmuhTwb9a0Q",
          "version" : {
            "number" : "6.1.0",
            "build_hash" : "c0c1ba0",
            "build_date" : "2017-12-12T12:32:54.550Z",
            "build_snapshot" : false,
            "lucene_version" : "7.1.0",
            "minimum_wire_compatibility_version" : "5.6.0",
            "minimum_index_compatibility_version" : "5.0.0"
          },
          "tagline" : "You Know, for Search"
        }"""

        status_err = 400
        self.url_es5 = 'http://es5.com'
        self.url_es5_err = 'http://es5_err.com'
        self.url_es6 = 'http://es6.com'
        self.url_es6_err = 'http://es6_err.com'

        httpretty.enable()
        httpretty.register_uri(httpretty.GET, self.url_es5,
                               body=self.body_es5)
        httpretty.register_uri(httpretty.GET, self.url_es5_err,
                               status=status_err)
        httpretty.register_uri(httpretty.GET, self.url_es6,
                               body=self.body_es6)
        httpretty.register_uri(httpretty.GET, self.url_es6_err,
                               status=status_err)

    def tearDown(self):

        httpretty.disable()

    def test_check_instance(self):
        """Test _check_instance function"""

        major = ElasticSearch._check_instance(self.url_es5, False)
        self.assertEqual(major, '5')
        major = ElasticSearch._check_instance(self.url_es6, False)
        self.assertEqual(major, '6')

        with self.assertRaises(ElasticConnectException):
            major = ElasticSearch._check_instance(self.url_es6_err, False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    unittest.main()
