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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
#

import json
import logging
import os.path
import sys
import unittest

if '..' not in sys.path:
    sys.path.insert(0, '..')

from grimoire_elk.enriched.jira import JiraEnrich


class TestEnrichJira(unittest.TestCase):
    """Functional unit tests for GrimoireELK Enrichment for Jira"""

    def setUp(self):
        self.ritems = []
        self.eitems = []
        with open(os.path.join("data", "jira_raw_fields.json")) as f:
            for line in f:
                self.ritems.append(json.loads(line))
        with open(os.path.join("data", "jira_enriched_fields.json")) as f:
            for line in f:
                self.eitems.append(json.loads(line))

    def test_enrich_fields(self):
        """Test enrich_fields function"""

        for ritem, eitem in zip(self.ritems, self.eitems):
            enriched = {}
            if 'fields' in ritem['_source']['data']:
                JiraEnrich.enrich_fields(ritem['_source']['data']['fields'], enriched)
            self.assertDictEqual(enriched, eitem)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    unittest.main()
