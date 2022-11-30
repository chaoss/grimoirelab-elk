# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2000 Bitergia
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
#
# Authors:
#   Florent Kaisser <florent.pro@kaisser.name>
#
# In this file, we have the ES requests used by backlog evolution github study
#


def get_unique_repository_with_project_name():
    """ Retrieve all the repository names from the index. """

    query_unique_repository = """
    {
        "size": 0,
        "aggs": {
            "unique_repos": {
              "composite" : {
                "size": 5000,
                "sources" : [
                  {"origin" : {"terms": {
                      "field": "origin"
                  }}},
                  {"project" : {"terms": {
                      "field": "project"
                  }}},
                  {"organization" : {"terms": {
                      "field": "author_org_name"
                  }}}
              ]
            }
          }
        }
    }
    """

    return query_unique_repository


def get_issues_not_closed_by_label(repository_url, to_date, label):
    issues_not_closed = """
    {
      "size": 10000,
      "query": {
          "bool": {
              "must_not": {
                  "exists": {
                      "field": "closed_at"
                  }
              },
      "filter": [{
                  "term": {
                      "origin": "%s"
                  }},{
                  "term": {
                      "labels": "%s"
                  }},{
                  "range" : {
                     "created_at": {"lt" : "%s"}
                  }
              }
              ]
          }
      }
    }
    """ % (repository_url, label, to_date)

    return issues_not_closed


def get_issues_open_at_by_label(repository_url, to_date, label):
    issues_open_at = """
    {
      "size": 10000,
      "query": {
          "bool": {
              "filter": [{
                  "term": {
                      "origin": "%s"
                  }},{
                  "term": {
                      "labels": "%s"
                  }},{
                  "range" : {
                     "closed_at": {"gt" : "%s"}
                  }},{
                  "range" : {
                     "created_at": {"lt" : "%s"}
                  }
              }
              ]
          }
      }
    }
    """ % (repository_url, label, to_date, to_date)

    return issues_open_at


def get_issues_not_closed_other_label(repository_url, to_date, exclude_labels):
    issues_not_closed = """
    {
      "size": 10000,
      "query": {
          "bool": {
              "must_not":[{
                "terms": {
                  "labels": %s
                }
              },{
                  "exists": {
                      "field": "closed_at"
                  }
              }],
              "filter": [{
                  "term": {
                      "origin":"%s"
                  }},{
                  "range" : {
                     "created_at": {"lt" : "%s"}
                  }
              }
              ]
          }
      }
    }
    """ % (str(exclude_labels).replace("'", "\""), repository_url, to_date)

    return issues_not_closed


def get_issues_open_at_other_label(repository_url, to_date, exclude_labels):
    issues_open_at = """
    {
      "size": 10000,
      "query": {
          "bool": {
              "must_not":{
                "terms": {
                  "labels": %s
                }
              },
              "filter": [{
                  "term": {
                      "origin": "%s"
                  }},{
                  "range" : {
                     "closed_at": {"gt" : "%s"}
                  }},{
                  "range" : {
                     "created_at": {"lt" : "%s"}
                  }
              }
              ]
          }
      }
    }
    """ % (str(exclude_labels).replace("'", "\""),
           repository_url, to_date, to_date)

    return issues_open_at


def get_issues_dates(elastic, interval, repository_url):
    interval_string = "interval" if elastic.is_legacy() else "fixed_interval"
    query_issues_dates = """
{
    "size": 0,
    "aggs" : {
        "created_per_interval" : {
            "date_histogram" : {
                "field" : "metadata__updated_on",
                "%s" : "%dd"
            },
            "aggs": {
                "created": {
                  "value_count": {
                        "field": "created_at"
                    }
                },
                "closed": {
                  "value_count": {
                        "field": "closed_at"
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
                }]
            }
        }
    }
    """ % (interval_string, interval, repository_url)

    return query_issues_dates
