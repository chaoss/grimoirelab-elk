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
from dateutil.relativedelta import relativedelta

from elasticsearch import Elasticsearch as ES, RequestsHttpConnection

from .enrich import (Enrich,
                     metadata)
from .graal_study_evolution import (get_to_date,
                                    get_unique_repository,
                                    get_files_at_time)

from grimoirelab_toolkit.datetime import (str_to_datetime,
                                          datetime_utcnow)
from grimoire_elk.elastic import ElasticSearch

MAX_SIZE_BULK_ENRICHED_ITEMS = 200

logger = logging.getLogger(__name__)


class CocomEnrich(Enrich):
    metrics = ["ccn", "num_funs", "tokens", "loc", "comments", "blanks"]

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []
        self.studies.append(self.enrich_repo_analysis)

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
        for metric in self.metrics:
            if file_analysis.get(metric, None) is not None:
                eitem[metric] = file_analysis[metric]
            else:
                eitem[metric] = None

        eitem["file_path"] = file_analysis.get("file_path", None)
        eitem["ext"] = file_analysis.get("ext", None)
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

        # TODO: Fix Logic: None rather than 1
        if None not in [eitem["loc"], eitem["comments"], eitem["num_funs"]]:
            eitem["loc_per_comment_lines"] = eitem["loc"] / max(eitem["comments"], 1)
            eitem["loc_per_blank_lines"] = eitem["loc"] / max(eitem["blanks"], 1)
            eitem["loc_per_function"] = eitem["loc"] / max(eitem["num_funs"], 1)
        else:
            eitem["loc_per_comment_lines"] = eitem["loc_per_blank_lines"] = eitem["loc_per_function"] = None

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

    def enrich_repo_analysis(self, ocean_backend, enrich_backend, no_incremental=False,
                             out_index="cocom_enrich_graal_repo", interval_months=3,
                             date_field="grimoire_creation_date"):

        logger.info("Doing enrich_repository_analysis study for index {}"
                    .format(self.elastic.anonymize_url(self.elastic.index_url)))

        es_in = ES([enrich_backend.elastic_url], retry_on_timeout=True, timeout=100,
                   verify_certs=self.elastic.requests.verify, connection_class=RequestsHttpConnection)
        in_index = enrich_backend.elastic.index

        unique_repos = es_in.search(
            index=in_index,
            body=get_unique_repository())

        repositories = [repo['key'] for repo in unique_repos['aggregations']['unique_repos'].get('buckets', [])]
        num_items = 0
        ins_items = 0

        for repository_url in repositories:
            es_out = ElasticSearch(enrich_backend.elastic.url, out_index)
            evolution_items = []

            to_month = get_to_date(es_in, in_index, out_index, repository_url)
            to_month = to_month.replace(day=1, hour=0, minute=0, second=0)
            current_month = datetime_utcnow().replace(day=1, hour=0, minute=0, second=0)

            while to_month < current_month:
                files_at_time = es_in.search(
                    index=in_index,
                    body=get_files_at_time(repository_url, to_month.isoformat())
                )['aggregations']['file_stats'].get("buckets", [])

                if not len(files_at_time):
                    to_month = to_month + relativedelta(months=+interval_months)
                    continue

                repository_name = repository_url.split("/")[-1]
                evolution_item = {
                    "id": "{}_{}_{}".format(to_month.isoformat(), repository_name, interval_months),
                    "origin": repository_url,
                    "interval_months": interval_months,
                    "study_creation_date": to_month.isoformat(),
                    "total_files": len(files_at_time)
                }

                for file_ in files_at_time:
                    file_details = file_["1"]["hits"]["hits"][0]["_source"]

                    for metric in self.metrics:
                        total_metric = "total_" + metric
                        evolution_item[total_metric] = evolution_item.get(total_metric, 0)
                        evolution_item[total_metric] += file_details[metric] if file_details[metric] is not None else 0

                # TODO: Fix Logic: None rather than 1
                evolution_item["total_loc_per_comment_lines"] = evolution_item["total_loc"] / \
                    max(evolution_item["total_comments"], 1)
                evolution_item["total_loc_per_blank_lines"] = evolution_item["total_loc"] / max(evolution_item["total_blanks"], 1)
                evolution_item["total_loc_per_function"] = evolution_item["total_loc"] / max(evolution_item["total_num_funs"], 1)

                evolution_items.append(evolution_item)

                if len(evolution_items) >= self.elastic.max_items_bulk:
                    num_items += len(evolution_items)
                    ins_items += es_out.bulk_upload(evolution_items, self.get_field_unique_id())
                    evolution_items = []

                to_month = to_month + relativedelta(months=+interval_months)

            if len(evolution_items) > 0:
                num_items += len(evolution_items)
                ins_items += es_out.bulk_upload(evolution_items, self.get_field_unique_id())

            if num_items != ins_items:
                missing = num_items - ins_items
                logger.error("%s/%s missing items for Graal Repository Analysis Study", str(missing), str(num_items))
            else:
                logger.info("%s items inserted for Graal Repository Analysis Study", str(num_items))

    def __fix_field_date(self, date_value):
        """Fix possible errors in the field date"""

        field_date = str_to_datetime(date_value)

        try:
            _ = int(field_date.strftime("%z")[0:3])
        except ValueError:
            field_date = field_date.replace(tzinfo=None)

        return field_date.isoformat()
