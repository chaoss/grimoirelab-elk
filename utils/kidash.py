#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Import and Export Kibana dashborads
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
import requests

from grimoire.ocean.elastic import ElasticOcean
 
from grimoire.utils import get_elastic
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

def list_dashboards(elastic_url):
    es_index = "/.kibana"
    elastic = get_elastic(elastic_url, es_index)
    dash_json_url = elastic.index_url+"/dashboard/_search?size=10000"

    print (dash_json_url)

    r = requests.get(dash_json_url)

    res_json = r.json()

    for dash in res_json["hits"]["hits"]:
        print (dash["_id"])


def import_dashboard(elastic_url, import_file):
    logging.debug("Reading from %s the JSON for the dashboard to be imported" % (args.import_file))

    with open(import_file, 'r') as f:
        kibana = json.loads(f.read())

        es_index = "/.kibana"
        elastic = get_elastic(elastic_url, es_index)

        url = elastic.index_url+"/dashboard/"+kibana['dashboard']['id']
        requests.post(url, data = json.dumps(kibana['dashboard']['value']))

        url = elastic.index_url+"/search/"+kibana['search']['id']
        requests.post(url, data = json.dumps(kibana['search']['value']))

        url = elastic.index_url+"/index-pattern/"+kibana['index_pattern']['id']
        requests.post(url, data = json.dumps(kibana['index_pattern']['value']))

        for vis in kibana['visualizations']:
            url = elastic.index_url+"/visualization"+"/"+vis['id']
            requests.post(url, data = json.dumps(vis['value']))

def export_dashboard(elastic_url, dash_id, export_file):

    # Kibana dashboard fields
    kibana = {"dashboard": None,
              "visualizations": [],
              "index_pattern": None,
              "search": None}

    logging.debug("Exporting dashboard %s to %s" % (args.dashboard, args.export_file))
    es_index = "/.kibana"
    elastic = get_elastic(elastic_url, es_index)

    kibana["dashboard"] = {"id":dash_id, "value":get_dashboard_json(elastic, dash_id)}

    if "panelsJSON" not in kibana["dashboard"]["value"]:
        return kibana

    for panel in json.loads(kibana["dashboard"]["value"]["panelsJSON"]):
        if panel['type'] in ['visualization']:
            vis_id = panel['id']
            vis_json = get_vis_json(elastic, vis_id)
            kibana["visualizations"].append({"id": vis_id, "value": vis_json})
            if not kibana["search"]:
                # Only one search is shared in the dashboard
                search_id = get_search_from_vis(elastic, vis_id)
                if search_id:
                    kibana["search"] = {"id":search_id, "value":get_search_json(elastic, search_id)}
            if not kibana["index_pattern"]:
                # Only one index pattern is shared in the dashboard
                index_pattern_id = get_index_pattern_from_vis(elastic, vis_id)
                kibana["index_pattern"] = {"id":index_pattern_id, "value":get_index_pattern_json(elastic, index_pattern_id)}

    with open(export_file, 'w') as f:
        f.write(json.dumps(kibana))

if __name__ == '__main__':

    args = get_params()

    config_logging(args.debug)
    # TODO: Use param to build a real URL if possible
    kibana_host = "http://localhost:5601"

    if args.import_file:
        import_dashboard(args.elastic_url, args.import_file)
    elif args.export_file:
        export_dashboard(args.elastic_url, args.dashboard, args.export_file)
    elif args.list:
        list_dashboards(args.elastic_url)