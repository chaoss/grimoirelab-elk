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
# You should have received a copy of the GNU General Public License
#
# Authors:
#   Valerio Cosentino <valcos@bitergia.com>
#   Nishchith Shetty <inishchith@gmail.com>
#

from grimoirelab_toolkit.datetime import str_to_datetime, unixtime_to_datetime


def get_unique_repository():
    """ Retrieve all the repository names from the index. """

    query_unique_repository = """
    {
        "size": 0,
        "aggs": {
            "unique_repos": {
                "terms": {
                    "field": "origin",
                    "size": 5000
                }
            }
        }
    }
    """

    return query_unique_repository


def get_last_study_date(repository_url, interval):
    """ Retrieve the last study_creation_date of the item corresponding
    to given repository from the study index.
    """

    query_last_study_date = """
    {
        "size": 0,
        "aggs": {
            "1": {
                "max": {
                    "field": "study_creation_date"
                }
            }
        },
        "query": {
            "bool": {
                "filter": [{
                    "term": {
                        "origin.keyword": "%s"
                    }
                },{
                    "term":{
                        "interval_months": "%s"
                    }
                }]
            }
        }
    }
    """ % (repository_url, interval)

    return query_last_study_date


def get_first_enriched_date(repository_url):
    """ Retrieve the first/oldest metadata__updated_on of the item
    corresponding to given repository.
    """

    query_first_enriched_date = """
    {
        "size": 0,
        "aggs": {
            "1": {
                "top_hits": {
                    "docvalue_fields": [
                        "metadata__updated_on"
                    ],
                    "_source": "metadata__updated_on",
                    "size": 1,
                    "sort": [{
                        "commit_date": {
                            "order": "asc"
                        }
                    }]
                }
            }
        },
        "query": {
            "bool": {
                "filter": [{
                    "term": {
                        "origin": "%s"
                    }
                }]
            }
        }
    }
    """ % (repository_url)

    return query_first_enriched_date


def get_files_at_time(repository_url, to_date):
    """ Retrieve all the latest changes wrt files until the to_date,
    corresponding to the given repository.
    """

    query_files_at_time = """
    {
        "size": 0,
        "aggs": {
            "file_stats": {
                "terms": {
                    "field": "file_path",
                    "size": 2147483647,
                    "order": {
                        "_key": "desc"
                    }
                },
                "aggs": {
                    "1": {
                        "top_hits": {
                            "size": 1,
                            "sort": [{
                                "metadata__updated_on": {
                                    "order": "desc"
                                }
                            }]
                        }
                    }
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

    return query_files_at_time


def get_to_date(es_in, in_index, out_index, repository_url, interval):
    """ Get the appropriate to_date value for incremental insertion. """
    study_data_available = False

    if es_in.indices.exists(index=out_index):
        last_study_date = es_in.search(
            index=out_index,
            body=get_last_study_date(repository_url, interval))["aggregations"]["1"]

        if "value_as_string" in last_study_date and last_study_date["value_as_string"]:
            study_data_available = True
            to_date = str_to_datetime(last_study_date["value_as_string"])
        elif "value" in last_study_date and last_study_date["value"]:
            study_data_available = True
            try:
                to_date = unixtime_to_datetime(last_study_date["value"])
            except Exception:
                to_date = unixtime_to_datetime(last_study_date["value"] / 1000)

    if not study_data_available:
        first_item_date = es_in.search(
            index=in_index,
            body=get_first_enriched_date(repository_url))["aggregations"]["1"]["hits"]["hits"][0]["_source"]

        to_date = str_to_datetime(first_item_date["metadata__updated_on"])

    return to_date
