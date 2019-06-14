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
#   Valerio Cosentino <valcos@bitergia.com>
#   Nishchith Shetty <inishchith@gmail.com>
#

import logging

from .enrich import Enrich, metadata
from grimoirelab_toolkit.datetime import str_to_datetime

MAX_SIZE_BULK_ENRICHED_ITEMS = 200

logger = logging.getLogger(__name__)


class CocomEnrich(Enrich):

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        return identities

    def has_identities(self):
        """ Return whether the enriched items contains identities """

        return False

    def get_field_unique_id(self):
        return "id"

    def extract_modules(self, file_path):
        """ Extracts module path from the given file path """
        path_chunks = file_path.split('/')

        modules = []
        for idx in range(len(path_chunks)):
            sub_path = '/'.join(path_chunks[:idx])

            if sub_path:
                modules.append(sub_path)

        return modules

    @metadata
    def get_rich_item(self, file_analysis):

        eitem = {}

        eitem['ccn'] = file_analysis.get("ccn", None)
        eitem['num_funs'] = file_analysis.get("num_funs", None)
        eitem['tokens'] = file_analysis.get("tokens", None)
        eitem['loc'] = file_analysis.get("loc", None)
        eitem['ext'] = file_analysis.get("ext", None)
        eitem['in_commit'] = file_analysis.get("in_commit", None)
        eitem['blanks'] = file_analysis.get("blanks", None)
        eitem['comments'] = file_analysis.get("comments", None)
        eitem['file_path'] = file_analysis.get("file_path", None)
        eitem['modules'] = self.extract_modules(eitem['file_path'])
        eitem = self.__add_derived_metrics(file_analysis, eitem)

        return eitem

    def get_rich_items(self, item):
        # The real data
        entry = item['data']

        enriched_items = []

        for file_analysis in entry["analysis"]:
            eitem = self.get_rich_item(file_analysis)

            for f in self.RAW_FIELDS_COPY:
                if f in item:
                    eitem[f] = item[f]
                else:
                    eitem[f] = None

            # common attributes
            eitem['commit_sha'] = entry['commit']
            eitem['author'] = entry['Author']
            eitem['committer'] = entry['Commit']
            eitem['commit'] = entry['commit']
            eitem['message'] = entry['message']
            eitem['author_date'] = self.__fix_field_date(entry['AuthorDate'])
            eitem['commit_date'] = self.__fix_field_date(entry['CommitDate'])

            if self.prjs_map:
                eitem.update(self.get_item_project(eitem))

            # uuid
            eitem['id'] = "{}_{}".format(eitem['commit_sha'], eitem['file_path'])

            eitem.update(self.get_grimoire_fields(entry["AuthorDate"], "file"))

            self.add_repository_labels(eitem)
            self.add_metadata_filter_raw(eitem)

            enriched_items.append(eitem)

        return enriched_items

    def __add_derived_metrics(self, file_analysis, eitem):
        """ Add derived metrics fields """
        if eitem['loc']:
            total_lines = eitem['loc'] + eitem['comments'] + eitem['blanks']
            eitem["comments_ratio"] = eitem['comments'] / total_lines
            eitem["blanks_ratio"] = eitem['blanks'] / total_lines
        else:
            eitem["comments_ratio"] = eitem['comments']
            eitem["blanks_ratio"] = eitem['blanks']

        return eitem

    def enrich_items(self, ocean_backend, events=False):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        for item in ocean_backend.fetch():
            rich_items = self.get_rich_items(item)

            items_to_enrich.extend(rich_items)
            if len(items_to_enrich) < MAX_SIZE_BULK_ENRICHED_ITEMS:
                continue

            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())
            items_to_enrich = []

        if len(items_to_enrich) > 0:
            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())

        if num_items != ins_items:
            missing = num_items - ins_items
            logger.error("%s/%s missing items for Cocom", str(missing), str(num_items))
        else:
            logger.info("%s items inserted for Cocom", str(num_items))

        return num_items

    def __fix_field_date(self, date_value):
        """Fix possible errors in the field date"""

        field_date = str_to_datetime(date_value)

        try:
            _ = int(field_date.strftime("%z")[0:3])
        except ValueError:
            field_date = field_date.replace(tzinfo=None)

        return field_date.isoformat()
