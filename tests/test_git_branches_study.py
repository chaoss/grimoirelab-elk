# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 Bitergia
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
#     Tanmay Baranwal <baranwaltanmay16@gmail.com>
#

import datetime
import json
import unittest
from unittest import mock

from grimoire_elk.enriched.git import GitEnrich
from grimoirelab_toolkit.datetime import datetime_utcnow, str_to_datetime


class TestUpdateBranchesField(unittest.TestCase):
    """Tests for the query sent by the git branches study to update `branches`"""

    def _update_query(self):
        """Return the update query sent by `update_branches_field`"""

        enrich = GitEnrich()
        enrich.requests = mock.MagicMock()
        enrich_backend = mock.MagicMock()
        enrich_backend.elastic.index_url = 'http://localhost:9200/git_enriched'
        git_repo = mock.MagicMock()
        git_repo.uri = 'https://example.com/repo.git'

        enrich.update_branches_field(git_repo, enrich_backend)

        return json.loads(enrich.requests.post.call_args.kwargs['data'])

    def test_branches_are_replaced(self):
        """Test whether `branches` is still replaced with `branches_aux`"""

        query = self._update_query()
        source = query['script']['source']

        self.assertIn("ctx._source.branches = ctx._source.branches_aux;", source)
        self.assertIn("ctx._source.remove('branches_aux');", source)
        self.assertEqual(query['query']['bool']['filter'][0]['term']['origin'], 'https://example.com/repo.git')

    def test_enriched_on_is_updated(self):
        """Test whether `metadata__enriched_on` is set to the current date (#1079)"""

        before = datetime_utcnow()
        query = self._update_query()
        after = datetime_utcnow()

        self.assertIn("ctx._source.metadata__enriched_on = params.enriched_on;", query['script']['source'])

        enriched_on = str_to_datetime(query['script']['params']['enriched_on'])
        self.assertEqual(enriched_on.tzinfo.utcoffset(enriched_on), datetime.timedelta(0))
        self.assertLessEqual(before, enriched_on)
        self.assertLessEqual(enriched_on, after)


if __name__ == "__main__":
    unittest.main(warnings='ignore')
