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
from .utils import fix_field_date

from grimoirelab_toolkit.datetime import datetime_utcnow
from grimoire_elk.elastic import ElasticSearch

MAX_SIZE_BULK_ENRICHED_ITEMS = 200

logger = logging.getLogger(__name__)


class ColicEnrich(Enrich):

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []
        self.studies.append(self.enrich_colic_analysis)

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        return identities

    def has_identities(self):
        """ Return whether the enriched items contains identities """

        return False

    def get_field_unique_id(self):
        return "id"

    def get_licensed_files(self, repository_url, to_date):
        """ Retrieve all the licensed files until the to_date, corresponding
        to the given repository.
        """

        query_licensed_files = """
        {
            "size": 0,
            "aggs": {
                "1": {
                    "cardinality": {
                        "field": "file_path"
                    }
                }
            },
            "query": {
                "bool": {
                    "must": [{
                        "match_phrase": {
                            "has_license": {
                                "query": 1
                            }
                        }
                    },
                    {
                        "match_phrase": {
                            "origin": {
                                "query": "%s"
                            }
                        }
                    },
                    {
                        "range": {
                            "metadata__updated_on": {
                                "lte": "%s"
                            }
                        }
                    }]
                }
            }
        }
        """ % (repository_url, to_date)

        return query_licensed_files

    def get_copyrighted_files(self, repository_url, to_date):
        """ Retrieve all the copyrighted files until the to_date, corresponding
        to the given repository.
        """

        query_copyrighted_files = """
        {
            "size": 0,
            "aggs": {
                "1": {
                    "cardinality": {
                        "field": "file_path"
                    }
                }
            },
            "query": {
                "bool": {
                    "must": [{
                        "match_phrase": {
                            "has_copyright": {
                                "query": 1
                            }
                        }
                    },
                    {
                        "match_phrase": {
                            "origin": {
                                "query": "%s"
                            }
                        }
                    },
                    {
                        "range": {
                            "metadata__updated_on": {
                                "lte": "%s"
                            }
                        }
                    }]
                }
            }
        }
        """ % (repository_url, to_date)

        return query_copyrighted_files

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
        # TODO: requires adjustments regarding category of backend used

        eitem = {}
        eitem["file_path"] = file_analysis["file_path"]
        eitem["modules"] = self.extract_modules(eitem["file_path"])
        eitem["copyrights"] = []
        eitem["licenses"] = []
        eitem["license_name"] = []
        eitem["has_license"] = 0
        eitem["has_copyright"] = 0

        if file_analysis.get("licenses", False):
            eitem["has_license"] = 1
            for _license in file_analysis["licenses"]:
                eitem["licenses"].extend(_license["matched_rule"]["licenses"])
                eitem["license_name"].append(_license["name"])

        if file_analysis.get("copyrights", False):
            eitem["has_copyright"] = 1
            for _copyright in file_analysis["copyrights"]:
                eitem["copyrights"].append(_copyright["value"])

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
            eitem['author_date'] = fix_field_date(entry['AuthorDate'])
            eitem['commit_date'] = fix_field_date(entry['CommitDate'])

            if self.prjs_map:
                eitem.update(self.get_item_project(eitem))

            # uuid
            eitem['id'] = "{}_{}".format(eitem['commit_sha'], eitem['file_path'])

            eitem.update(self.get_grimoire_fields(entry["AuthorDate"], "file"))

            self.add_repository_labels(eitem)
            self.add_metadata_filter_raw(eitem)

            enriched_items.append(eitem)

        return enriched_items

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
            logger.error("%s/%s missing items for CoLic", str(missing), str(num_items))
        else:
            logger.info("%s items inserted for CoLic", str(num_items))

        return num_items

    def enrich_colic_analysis(self, ocean_backend, enrich_backend, no_incremental=False,
                              out_index="colic_enrich_graal_repo", interval_months=[3],
                              date_field="grimoire_creation_date"):

        logger.info("Doing enrich_colic_analysis study for index {}"
                    .format(self.elastic.anonymize_url(self.elastic.index_url)))

        es_in = ES([enrich_backend.elastic_url], retry_on_timeout=True, timeout=100,
                   verify_certs=self.elastic.requests.verify, connection_class=RequestsHttpConnection)
        in_index = enrich_backend.elastic.index
        interval_months = list(map(int, interval_months))

        unique_repos = es_in.search(
            index=in_index,
            body=get_unique_repository())

        repositories = [repo['key'] for repo in unique_repos['aggregations']['unique_repos'].get('buckets', [])]
        current_month = datetime_utcnow().replace(day=1, hour=0, minute=0, second=0)
        num_items = 0
        ins_items = 0

        for repository_url in repositories:
            es_out = ElasticSearch(enrich_backend.elastic.url, out_index)
            evolution_items = []

            for interval in interval_months:

                to_month = get_to_date(es_in, in_index, out_index, repository_url, interval)
                to_month = to_month.replace(month=int(interval), day=1, hour=0, minute=0, second=0)

                while to_month < current_month:
                    copyrighted_files_at_time = es_in.search(
                        index=in_index,
                        body=self.get_copyrighted_files(repository_url, to_month.isoformat()))

                    licensed_files_at_time = es_in.search(
                        index=in_index,
                        body=self.get_licensed_files(repository_url, to_month.isoformat()))

                    files_at_time = es_in.search(
                        index=in_index,
                        body=get_files_at_time(repository_url, to_month.isoformat()))

                    licensed_files = int(licensed_files_at_time["aggregations"]["1"]["value"])
                    copyrighted_files = int(copyrighted_files_at_time["aggregations"]["1"]["value"])
                    # TODO: Fix - need more efficient query
                    total_files = len(files_at_time['aggregations']['file_stats'].get("buckets", []))

                    if not total_files:
                        to_month = to_month + relativedelta(months=+interval)
                        continue

                    repository_name = repository_url.split("/")[-1]
                    evolution_item = {
                        "id": "{}_{}_{}".format(to_month.isoformat(), repository_name, interval),
                        "origin": repository_url,
                        "interval_months": interval,
                        "study_creation_date": to_month.isoformat(),
                        "licensed_files": licensed_files,
                        "copyrighted_files": copyrighted_files,
                        "total_files": total_files
                    }

                    evolution_items.append(evolution_item)

                    if len(evolution_items) >= self.elastic.max_items_bulk:
                        num_items += len(evolution_items)
                        ins_items += es_out.bulk_upload(evolution_items, self.get_field_unique_id())
                        evolution_items = []

                    to_month = to_month + relativedelta(months=+interval)

                if len(evolution_items) > 0:
                    num_items += len(evolution_items)
                    ins_items += es_out.bulk_upload(evolution_items, self.get_field_unique_id())

                if num_items != ins_items:
                    missing = num_items - ins_items
                    logger.error("%s/%s missing items for Graal CoLic Analysis Study", str(missing), str(num_items))
                else:
                    logger.info("%s items inserted for Graal CoLic Analysis Study", str(num_items))
