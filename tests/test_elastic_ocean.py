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

import unittest

from grimoire_elk.raw.elastic import ElasticOcean
from perceval.backends.core.git import Git
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

    def test_get_p2o_params_from_url_error(self):
        """Test whether an exception is thrown when the tokens obtained when parsing the filter are not 2"""

        with self.assertRaises(ELKError):
            _ = ElasticOcean.get_p2o_params_from_url("https://finosfoundation.atlassian.net/wiki/ "
                                                     "--filter-raw=data.project:openstack/stx-clients=xxx")

        with self.assertRaises(ELKError):
            _ = ElasticOcean.get_p2o_params_from_url("https://finosfoundation.atlassian.net/wiki/ "
                                                     "--filter-raw")

    def test_get_field_date(self):
        """Test whether the field date is correctly returned"""

        perceval_backend = Git('http://example.com', '/tmp/foo')
        eitems = ElasticOcean(perceval_backend)
        self.assertEqual(eitems.get_field_date(), 'metadata__updated_on')


if __name__ == '__main__':
    unittest.main()
