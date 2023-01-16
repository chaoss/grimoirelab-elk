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
#   Nishchith Shetty <inishchith@gmail.com>
#

import logging
from dateutil.relativedelta import relativedelta

from elasticsearch import Elasticsearch as ES, RequestsHttpConnection
from .enrich import (Enrich,
                     metadata)
from .graal_study_evolution import (get_to_date,
                                    get_unique_repository)
from .utils import fix_field_date, anonymize_url
from ..elastic_mapping import Mapping as BaseMapping

from grimoirelab_toolkit.datetime import datetime_utcnow
from grimoire_elk.elastic import ElasticSearch

MAX_SIZE_BULK_ENRICHED_ITEMS = 200

logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        Ensure data.message is string, since it can be very large

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = '''
         {
            "dynamic":true,
            "properties": {
                "id" : {
                    "type" : "keyword"
                },
                "interval_months" : {
                    "type" : "long"
                },
                "origin" : {
                    "type" : "keyword"
                },
                "study_creation_date" : {
                    "type" : "date"
                },
                "total_files": {
                    "type": "long"
                },
                "licensed_files": {
                    "type": "long"
                },
                "copyrighted_files": {
                    "type": "long"
                }
            }
        }
        '''

        return {"items": mapping}


class ColicEnrich(Enrich):

    def __init__(self, db_sortinghat=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', db_path=None,
                 db_port=None, db_ssl=False):
        super().__init__(db_sortinghat=db_sortinghat, json_projects_map=json_projects_map,
                         db_user=db_user, db_password=db_password, db_host=db_host,
                         db_port=db_port, db_path=db_path, db_ssl=db_ssl)

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

    def __get_total_files(self, repository_url, to_date):
        """ Retrieve total number for files until to_date, corresponding
        to the given repository
        """

        query_total_files = """
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
                    "filter": [{
                        "term": {
                            "origin": "%s"
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

        return query_total_files

    def __get_licensed_files(self, repository_url, to_date):
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
                    "filter": [{
                        "term": {
                            "has_license": 1
                        }
                    },
                    {
                        "term": {
                            "origin": "%s"
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

    def __get_copyrighted_files(self, repository_url, to_date):
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
                    "filter": [{
                        "term": {
                            "has_copyright": 1
                        }
                    },
                    {
                        "term": {
                            "origin": "%s"
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
    def __get_rich_scancode(self, file_analysis):
        # Scancode and Scancode-CLI Implementation

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

    @metadata
    def __get_rich_nomossa(self, file_analysis):
        # NOMOS analyzer implementation

        eitem = {}
        eitem["file_path"] = file_analysis["file_path"]
        eitem["modules"] = self.extract_modules(eitem["file_path"])
        eitem["licenses"] = []
        eitem["license_name"] = []
        eitem["has_license"] = 0

        if file_analysis["licenses"] != "No_license_found":
            eitem["has_license"] = 1
            for _license in file_analysis["licenses"]:
                eitem["licenses"].append(_license)
                eitem["license_name"].append(_license)

        # NOMOS doesn't provide copyright information.
        eitem["copyrights"] = []
        eitem["has_copyright"] = 0

        return eitem

    def get_rich_items(self, item):
        """
            :category: code_license_scancode_cli(default)
        """

        if item["category"] == "code_license_nomos":
            get_rich_item = self.__get_rich_nomossa
        else:
            get_rich_item = self.__get_rich_scancode

        entry = item['data']
        enriched_items = []

        for file_analysis in entry["analysis"]:
            eitem = get_rich_item(file_analysis)

            self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)

            # common attributes
            eitem['author'] = entry['Author']
            eitem['author_date'] = fix_field_date(entry['AuthorDate'])
            eitem["category"] = item["category"]
            eitem['commit'] = entry['commit']
            eitem['committer'] = entry['Commit']
            eitem['commit_date'] = fix_field_date(entry['CommitDate'])
            eitem['commit_sha'] = entry['commit']
            eitem['message'] = entry.get('message', None)

            # Other enrichment
            eitem["repo_url"] = item["origin"]
            if eitem["repo_url"].startswith('http'):
                eitem["repo_url"] = anonymize_url(eitem["repo_url"])

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
            logger.error("[colic] {}/{} missing items".format(missing, num_items))
        else:
            logger.info("[colic] {} items inserted".format(num_items))

        return num_items

    def enrich_colic_analysis(self, ocean_backend, enrich_backend, no_incremental=False,
                              out_index="colic_enrich_graal_repo", interval_months=[3],
                              date_field="grimoire_creation_date"):

        logger.info("[colic] study enrich-colic-analysis start")

        es_in = ES([enrich_backend.elastic_url], retry_on_timeout=True, timeout=100,
                   verify_certs=self.elastic.requests.verify, connection_class=RequestsHttpConnection)
        in_index = enrich_backend.elastic.index
        interval_months = list(map(int, interval_months))

        unique_repos = es_in.search(
            index=in_index,
            body=get_unique_repository())

        repositories = [repo['key'] for repo in unique_repos['aggregations']['unique_repos'].get('buckets', [])]

        logger.info("[colic] study enrich-colic-analysis {} repositories to process".format(
                    len(repositories)))
        es_out = ElasticSearch(enrich_backend.elastic.url, out_index, mappings=Mapping)
        es_out.add_alias("colic_study")

        current_month = datetime_utcnow().replace(day=1, hour=0, minute=0, second=0)
        num_items = 0
        ins_items = 0

        for repository_url in repositories:
            repository_url_anonymized = repository_url
            if repository_url_anonymized.startswith('http'):
                repository_url_anonymized = anonymize_url(repository_url_anonymized)

            logger.info("[colic] study enrich-colic-analysis start analysis for {}".format(
                        repository_url_anonymized))
            evolution_items = []

            for interval in interval_months:

                to_month = get_to_date(es_in, in_index, out_index, repository_url, interval)
                to_month = to_month.replace(month=int(interval), day=1, hour=0, minute=0, second=0)

                while to_month < current_month:
                    copyrighted_files_at_time = es_in.search(
                        index=in_index,
                        body=self.__get_copyrighted_files(repository_url, to_month.isoformat()))

                    licensed_files_at_time = es_in.search(
                        index=in_index,
                        body=self.__get_licensed_files(repository_url, to_month.isoformat()))

                    files_at_time = es_in.search(
                        index=in_index,
                        body=self.__get_total_files(repository_url, to_month.isoformat()))

                    licensed_files = int(licensed_files_at_time["aggregations"]["1"]["value"])
                    copyrighted_files = int(copyrighted_files_at_time["aggregations"]["1"]["value"])
                    total_files = int(files_at_time["aggregations"]["1"]["value"])

                    if not total_files:
                        to_month = to_month + relativedelta(months=+interval)
                        continue

                    evolution_item = {
                        "id": "{}_{}_{}".format(to_month.isoformat(), hash(repository_url_anonymized), interval),
                        "repo_url": repository_url_anonymized,
                        "origin": repository_url,
                        "interval_months": interval,
                        "study_creation_date": to_month.isoformat(),
                        "licensed_files": licensed_files,
                        "copyrighted_files": copyrighted_files,
                        "total_files": total_files
                    }

                    evolution_item.update(self.get_grimoire_fields(evolution_item["study_creation_date"], "stats"))
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
                    logger.error(
                        "[colic] study enrich-colic-analysis {}/{} missing items for Graal CoLic Analysis "
                        "Study".format(missing, num_items)
                    )
                else:
                    logger.info(
                        "[colic] study enrich-colic-analysis {} items inserted for Graal CoLic Analysis "
                        "Study".format(num_items)
                    )

            logger.info(
                "[colic] study enrich-colic-analysis end analysis for {} with month interval".format(
                    repository_url_anonymized)
            )

        logger.info("[colic] study enrich-colic-analysis end")
