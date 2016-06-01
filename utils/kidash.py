#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Import and Export Kibana dashboards
#
# Copyright (C) 2015 Bitergia
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
import logging
import os
import requests
import sys

from grimoire.ocean.elastic import ElasticOcean

from grimoire.elk.elastic import ElasticSearch
from grimoire.utils import config_logging
from e2k import get_dashboard_json, get_vis_json, get_search_json, get_index_pattern_json
from e2k import get_search_from_vis, get_index_pattern_from_vis


def get_params_parser_create_dash():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(usage="usage: kidash [options]",
                                     description="Import or Export a Kibana Dashboard")

    ElasticOcean.add_params(parser)

    parser.add_argument("--dashboard", help="Kibana dashboard id to export")
    parser.add_argument("--export", dest="export_file", help="file with the dashboard exported")
    parser.add_argument("--import", dest="import_file", help="file with the dashboard to be imported")
    parser.add_argument("--kibana", dest="kibana_index", default=".kibana", help="Kibana index name (.kibana default)")
    parser.add_argument("--list", action='store_true', help="list available dashboards")
    parser.add_argument('-g', '--debug', dest='debug', action='store_true')

    return parser

def get_params():
    parser = get_params_parser_create_dash()
    args = parser.parse_args()

    if not (args.export_file or args.import_file or args.list):
        parser.error("--export or --import or --list needed")
    else:
        if args.export_file and not args.dashboard:
            parser.error("--export needs --dashboard")
    return args

def list_dashboards(elastic_url, es_index=None):
    if not es_index:
        es_index = ".kibana"

    elastic = ElasticSearch(elastic_url, es_index)

    dash_json_url = elastic.index_url+"/dashboard/_search?size=10000"

    print (dash_json_url)

    r = requests.get(dash_json_url, verify=False)

    res_json = r.json()

    if "hits" not in res_json:
        logging.error("Can't find dashboards")
        raise RuntimeError("Can't find dashboards")

    for dash in res_json["hits"]["hits"]:
        print (dash["_id"])


def import_dashboard(elastic_url, import_file, es_index=None):
    logging.debug("Reading from %s the JSON for the dashboard to be imported" % (args.import_file))

    with open(import_file, 'r') as f:
        try:
            kibana = json.loads(f.read())
        except ValueError:
            logging.error("Wrong file format")
            sys.exit(1)

        if 'dashboard' not in kibana:
            logging.error("Wrong file format. Can't find 'dashboard' field.")
            sys.exit(1)

        if not es_index:
            es_index = ".kibana"
        elastic = ElasticSearch(elastic_url, es_index)

        url = elastic.index_url+"/dashboard/"+kibana['dashboard']['id']
        requests.post(url, data = json.dumps(kibana['dashboard']['value']), verify=False)

        if 'searches' in kibana:
            for search in kibana['searches']:
                url = elastic.index_url+"/search/"+search['id']
                requests.post(url, data = json.dumps(search['value']), verify=False)

        if 'index_patterns' in kibana:
            for index in kibana['index_patterns']:
                url = elastic.index_url+"/index-pattern/"+index['id']
                requests.post(url, data = json.dumps(index['value']), verify=False)

        if 'visualizations' in kibana:
            for vis in kibana['visualizations']:
                url = elastic.index_url+"/visualization"+"/"+vis['id']
                requests.post(url, data = json.dumps(vis['value']), verify=False)

        logging.debug("Done")


def export_dashboard(elastic_url, dash_id, export_file, es_index=None):

    # Kibana dashboard fields
    kibana = {"dashboard": None,
              "visualizations": [],
              "index_patterns": [],
              "searches": []}

    # Used to avoid having duplicates
    search_ids_done = []
    index_ids_done = []

    logging.debug("Exporting dashboard %s to %s" % (args.dashboard, args.export_file))
    if not es_index:
        es_index = ".kibana"

    elastic = ElasticSearch(elastic_url, es_index)

    kibana["dashboard"] = {"id":dash_id, "value":get_dashboard_json(elastic, dash_id)}

    if "panelsJSON" not in kibana["dashboard"]["value"]:
        # The dashboard is empty. No visualizations included.
        return kibana

    # Export all visualizations and the index patterns and searches in them
    for panel in json.loads(kibana["dashboard"]["value"]["panelsJSON"]):
        if panel['type'] in ['visualization']:
            vis_id = panel['id']
            vis_json = get_vis_json(elastic, vis_id)
            kibana["visualizations"].append({"id": vis_id, "value": vis_json})
            search_id = get_search_from_vis(elastic, vis_id)
            if search_id and search_id not in search_ids_done:
                search_ids_done.append(search_id)
                kibana["searches"].append({"id":search_id,
                                           "value":get_search_json(elastic, search_id)})
            index_pattern_id = get_index_pattern_from_vis(elastic, vis_id)
            if index_pattern_id and index_pattern_id not in index_ids_done:
                index_ids_done.append(index_pattern_id)
                kibana["index_patterns"].append({"id":index_pattern_id,
                                                 "value":get_index_pattern_json(elastic, index_pattern_id)})
    logging.debug("Done")

    with open(export_file, 'w') as f:
        f.write(json.dumps(kibana))

if __name__ == '__main__':

    args = get_params()

    config_logging(args.debug)

    if args.import_file:
        import_dashboard(args.elastic_url, args.import_file, args.kibana_index)
    elif args.export_file:
        if os.path.isfile(args.export_file):
            logging.info("%s exists. Remove it before running." % (args.export_file))
            sys.exit(0)
        export_dashboard(args.elastic_url, args.dashboard, args.export_file, args.kibana_index)
    elif args.list:
        list_dashboards(args.elastic_url, args.kibana_index)
