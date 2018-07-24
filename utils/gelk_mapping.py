#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Tool to show the mapping used in a GrimoireELK data source
#
# Copyright (C) 2018 Bitergia
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
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import argparse
import json
import sys

from grimoire_elk.utils import get_connectors


ES_SUPPORTED = ["5", "6"]  # Elasticsearch major versions supported


def get_params():
    parser = argparse.ArgumentParser(usage="usage:gelk_mapping [options]",
                                     description="Show the mapping used by GrimoireELK given a data source")
    parser.add_argument('-d', '--data-source', required=True, dest='data_source',
                        help="perceval data source (git, github ...)")
    parser.add_argument('-v', '--version', dest='version', required=True,
                        help="Major Elasticsearch version for the mapping (2, 5, 6")
    parser.add_argument('--index-raw', required=True, help="Name of the raw index to be imported")
    parser.add_argument('--index-enriched', required=True, help="Name of the enriched index to be imported")
    args = parser.parse_args()

    return args


def find_general_mappings(es_major_version):
    """
    Find the general mappings applied to all data sources
    :param es_major_version: string with the major version for Elasticsearch
    :return: a dict with the mappings (raw and enriched)
    """

    if es_major_version not in ES_SUPPORTED:
        print("Elasticsearch version not supported %s (supported %s)" % (es_major_version, ES_SUPPORTED))
        sys.exit(1)

    # By default all strings are not analyzed in ES < 6
    if es_major_version == '5':
        # Before version 6, strings were strings
        not_analyze_strings = """
        {
          "dynamic_templates": [
            { "notanalyzed": {
                  "match": "*",
                  "match_mapping_type": "string",
                  "mapping": {
                      "type":        "string",
                      "index":       "not_analyzed"
                  }
               }
            }
          ]
        } """
    else:
        # After version 6, strings are keywords (not analyzed)
        not_analyze_strings = """
        {
          "dynamic_templates": [
            { "notanalyzed": {
                  "match": "*",
                  "match_mapping_type": "string",
                  "mapping": {
                      "type":        "keyword"
                  }
               }
            }
          ]
        } """

    return json.loads(not_analyze_strings)


def find_ds_mapping(data_source, es_major_version):
    """
    Find the mapping given a perceval data source

    :param data_source: name of the perceval data source
    :param es_major_version: string with the major version for Elasticsearch
    :return: a dict with the mappings (raw and enriched)
    """
    mappings = {"raw": None,
                "enriched": None}

    # Backend connectors
    connectors = get_connectors()

    try:
        raw_klass = connectors[data_source][1]
        enrich_klass = connectors[data_source][2]
    except KeyError:
        print("Data source not found", data_source)
        sys.exit(1)

    # Mapping for raw index
    backend = raw_klass(None)
    if backend:
        mapping = json.loads(backend.mapping.get_elastic_mappings(es_major_version)['items'])
        mappings['raw'] = [mapping, find_general_mappings(es_major_version)]

    # Mapping for enriched index
    backend = enrich_klass(None)
    if backend:
        mapping = json.loads(backend.mapping.get_elastic_mappings(es_major_version)['items'])
        mappings['enriched'] = [mapping, find_general_mappings(es_major_version)]

    return mappings


if __name__ == '__main__':

    ARGS = get_params()

    mappings = find_ds_mapping(ARGS.data_source, ARGS.version)

    print(json.dumps(mappings, indent=True))

    # Let's generate two files, <backend>-mapping-raw.json <backend>-mapping-enriched.json
    raw_dict = mappings['raw'][0]
    raw_dict.update(mappings['raw'][1])
    raw_full = {ARGS.index_raw: {"mappings": {"items": raw_dict}}}
    with open(ARGS.data_source + "-mapping-raw.json", "w") as fmap:
        json.dump(raw_full, fmap, indent=True)

    enriched_dict = mappings['enriched'][0]
    enriched_dict.update(mappings['enriched'][1])
    enriched_full = {ARGS.index_enriched: {"mappings": {"items": enriched_dict}}}
    with open(ARGS.data_source + "-mapping-enriched.json", "w") as fmap:
        json.dump(enriched_full, fmap, indent=True)
