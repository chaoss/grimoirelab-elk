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
#     Valerio Cosentino <valcos@bitergia.com>
#

import unittest

from grimoire_elk.raw.elastic import ElasticOcean, logger
from grimoire_elk.errors import ELKError


class TestElasticOcean(unittest.TestCase):

    def test_get_p2o_params_from_url(self):
        """Test whether a URL without params is correctly parsed"""

        params = ElasticOcean.get_p2o_params_from_url("https://finosfoundation.atlassian.net/wiki/")

        self.assertEqual(len(params), 1)
        self.assertEqual(params['url'], "https://finosfoundation.atlassian.net/wiki/")

    def test_get_p2o_params_from_url_filter(self):
        """Test whether a URL with a filter is correctly parsed"""

        params = ElasticOcean.get_p2o_params_from_url("https://bugzilla.mozilla.org "
                                                      "--filter-raw=data.product:Add-on SDK,data.component:General")

        self.assertEqual(len(params), 2)
        self.assertEqual(params['url'], "https://bugzilla.mozilla.org")
        self.assertEqual(params['filter-raw'], "data.product:Add-on SDK,data.component:General")

    def test_get_p2o_params_from_url_more_filters(self):
        """Test whether a warning is logged in """

        with self.assertLogs(logger, level='WARNING') as cm:
            params = ElasticOcean.get_p2o_params_from_url("https://finosfoundation.atlassian.net/wiki/ "
                                                          "--filter-raw=data.project:openstack/stx-clients "
                                                          "--filter-raw-prefix=data.project:https://github.com/")

            self.assertEqual(len(params), 2)
            self.assertEqual(params['url'], "https://finosfoundation.atlassian.net/wiki/")
            self.assertEqual(params['filter-raw'], "data.project:openstack/stx-clients")

            self.assertEqual(cm.output[0],
                             'WARNING:grimoire_elk.raw.elastic:Too many filters defined '
                             'for https://finosfoundation.atlassian.net/wiki/ '
                             '--filter-raw=data.project:openstack/stx-clients '
                             '--filter-raw-prefix=data.project:https://github.com/, '
                             'only the first one is considered')

    def test_get_p2o_params_from_url_error(self):
        """Test whether an exception is thrown when the tokens obtained when parsing the filter are not 2"""

        with self.assertRaises(ELKError):
            _ = ElasticOcean.get_p2o_params_from_url("https://finosfoundation.atlassian.net/wiki/ "
                                                     "--filter-raw=data.project:openstack/stx-clients=xxx")

        with self.assertRaises(ELKError):
            _ = ElasticOcean.get_p2o_params_from_url("https://finosfoundation.atlassian.net/wiki/ "
                                                     "--filter-raw")


if __name__ == '__main__':
    unittest.main()
