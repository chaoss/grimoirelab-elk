#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Enriched to Kibana Publisher
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
from os import sys
import requests

from grimoire.ocean.elastic import ElasticOcean


from grimoire.elk.elastic import ElasticSearch
from grimoire.utils import config_logging

E2K_USAGE_MSG = ""
E2K_USAGE_MSG = ""
E2K_MSG = ""

def get_params_parser_create_dash():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(usage=E2K_USAGE_MSG,
                                     description=E2K_USAGE_MSG,
                                     epilog=E2K_MSG,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     add_help=True)

    ElasticOcean.add_params(parser)

    parser.add_argument("-d", "--dashboard", help="dashboard to be used as template")
    parser.add_argument("-i", "--index", help="enriched index to be used as data source")
    parser.add_argument("--kibana", dest="kibana_index", default=".kibana", help="Kibana index name (.kibana default)")
    parser.add_argument('-g', '--debug', dest='debug', action='store_true')

    return parser

def get_params():
    parser = get_params_parser_create_dash()
    args = parser.parse_args()

    return args

def get_dashboard_json(elastic, dashboard):
    dash_json_url = elastic.index_url+"/dashboard/"+dashboard

    r = requests.get(dash_json_url, verify=False)

    dash_json = r.json()
    if "_source" not in dash_json:
        logging.error("Can not find dashboard: %s" % (dashboard))
        print (dash_json_url)
        sys.exit(1)

    return dash_json['_source']

def get_vis_json(elastic, vis):
    vis_json_url = elastic.index_url+"/visualization/"+vis

    r = requests.get(vis_json_url, verify=False)

    vis_json = r.json()
    if "_source" not in vis_json:
        logging.error("Can not find vis: %s (%s)" % (vis, vis_json_url))
        return

    return vis_json['_source']

def get_search_json(elastic, search_id):
    search_json_url = elastic.index_url+"/search/"+search_id

    r = requests.get(search_json_url, verify=False)

    search_json = r.json()
    if "_source" not in search_json:
        logging.error("Can not find search: %s (%s)" % (search_json_url))
        return
    return search_json['_source']

def get_index_pattern_json(elastic, index_pattern):
    index_pattern_json_url = elastic.index_url+"/index-pattern/"+index_pattern

    r = requests.get(index_pattern_json_url, verify=False)

    index_pattern_json = r.json()
    if "_source" not in index_pattern_json:
        logging.error("Can not find index_pattern_json: %s" % (index_pattern_json_url))
        return
    return index_pattern_json['_source']

def get_search_from_vis(elastic, vis):
    search_id = None
    vis_json = get_vis_json(elastic, vis)
    if not vis_json:
        search_id
    # The index pattern could be in search or in state
    # First search for it in saved search
    if "savedSearchId" in vis_json:
        search_id = vis_json["savedSearchId"]
    return search_id


def create_search(elastic_url, dashboard, index_pattern, es_index=None):
    """ Create the base search for vis if used

        :param elastic_url: URL for ElasticSearch (ES) server
        :param dashboard: kibana dashboard to be used as template
        :param enrich_index: ES index with enriched items used in the new dashboard

    """

    search_id = None
    if not es_index:
        es_index = ".kibana"
    elastic = ElasticSearch(elastic_url, es_index)

    dash_data = get_dashboard_json(elastic, dashboard)

    # First vis
    if "panelsJSON" not in dash_data:
        logging.error("Can not find vis in dashboard: %s" % (dashboard))
        raise

    # Get the search from the first vis in the panel
    for panel in json.loads(dash_data["panelsJSON"]):
        panel_id = panel["id"]
        logging.debug("Checking search in %s vis" % (panel_id))

        search_id = get_search_from_vis(elastic, panel_id)
        if search_id:
            break

    # And now time to create the search found
    if not search_id:
        logging.info("Can't find search  %s" % (dashboard))
        return

    logging.debug("Found template search %s" % (search_id))

    search_json = get_search_json(elastic, search_id)
    search_source = search_json['kibanaSavedObjectMeta']['searchSourceJSON']
    new_search_source = json.loads(search_source)
    new_search_source['index'] = index_pattern
    new_search_source = json.dumps(new_search_source)
    search_json['kibanaSavedObjectMeta']['searchSourceJSON'] = new_search_source

    search_json['title'] += " " + index_pattern
    new_search_id = search_id+"__"+index_pattern

    url = elastic.index_url+"/search/"+new_search_id
    requests.post(url, data = json.dumps(search_json), verify=False)

    logging.debug("New search created: %s" % (url))

    return new_search_id

def get_index_pattern_from_meta(meta_data):
    index = None
    mdata = meta_data["searchSourceJSON"]
    mdata = json.loads(mdata)
    if "index" in mdata:
        index = mdata["index"]
    if "filter" in mdata:
        if len(mdata["filter"]) > 0:
            index = mdata["filter"][0]["meta"]["index"]
    return index

def get_index_pattern_from_vis(elastic, vis):
    index_pattern = None
    vis_json = get_vis_json(elastic, vis)
    if not vis_json:
        return
    # The index pattern could be in search or in state
    # First search for it in saved search
    if "savedSearchId" in vis_json:
        search_json_url = elastic.index_url+"/search/"+vis_json["savedSearchId"]
        search_json = requests.get(search_json_url, verify=False).json()["_source"]
        index_pattern = get_index_pattern_from_meta(search_json["kibanaSavedObjectMeta"])
    elif "kibanaSavedObjectMeta" in vis_json:
        index_pattern = get_index_pattern_from_meta(vis_json["kibanaSavedObjectMeta"])
    return index_pattern


def create_index_pattern(elastic_url, dashboard, enrich_index, es_index=None):
    """ Create a index pattern using as template the index pattern
        in dashboard template vis

        :param elastic_url: URL for ElasticSearch (ES) server
        :param dashboard: kibana dashboard to be used as template
        :param enrich_index: ES index with enriched items used in the new dashboard

    """

    index_pattern = None
    if not es_index:
        es_index = ".kibana"
    elastic = ElasticSearch(elastic_url, es_index)

    dash_data = get_dashboard_json(elastic, dashboard)

    # First vis
    if "panelsJSON" not in dash_data:
        logging.error("Can not find vis in dashboard: %s" % (dashboard))
        raise

    # Get the index pattern from the first vis in the panel
    # that as index pattern data
    for panel in json.loads(dash_data["panelsJSON"]):
        panel_id = panel["id"]
        logging.debug("Checking index pattern in %s vis" % (panel_id))

        index_pattern = get_index_pattern_from_vis(elastic, panel_id)
        if index_pattern:
            break

    # And now time to create the index pattern found
    if not index_pattern:
        logging.error("Can't find index pattern for %s" % (dashboard))
        raise

    logging.debug("Found %s template index pattern" % (index_pattern))


    new_index_pattern_json = get_index_pattern_json(elastic, index_pattern)

    new_index_pattern_json['title'] = enrich_index
    url = elastic.index_url+"/index-pattern/"+enrich_index
    requests.post(url, data = json.dumps(new_index_pattern_json), verify=False)

    logging.debug("New index pattern created: %s" % (url))

    return enrich_index

def create_dashboard(elastic_url, dashboard, enrich_index, kibana_host, es_index=None):
    """ Create a new dashboard using dashboard as template
        and reading the data from enriched_index """

    def new_panels(elastic, panels, search_id):
        """ Create the new panels and their vis for the dashboard from the
            panels in the template dashboard """

        dash_vis_ids = []
        new_panels = []
        for panel in panels:
            if panel['type'] in ['visualization', 'search']:
                if panel['type'] == 'visualization':
                    dash_vis_ids.append(panel['id'])
                panel['id'] += "__"+enrich_index
                if panel['type'] == 'search':
                    panel['id'] = search_id
            new_panels.append(panel)

        create_vis(elastic, dash_vis_ids, search_id)


        return new_panels

    def create_vis(elastic, dash_vis_ids, search_id):
        """ Create new visualizations for the dashboard """

        # Create visualizations for the new dashboard
        item_template_url = elastic.index_url+"/visualization"
        # Hack: Get all vis if they are <10000. Use scroll API to get all.
        # Better: use mget to get all vis in dash_vis_ids
        item_template_url_search = item_template_url+"/_search?size=10000"
        r = requests.get(item_template_url_search, verify=False)
        all_visualizations =r.json()['hits']['hits']

        visualizations = []
        for vis in all_visualizations:
            if vis['_id'] in dash_vis_ids:
                visualizations.append(vis)

        logging.info("Total template vis found: %i" % (len(visualizations)))

        for vis in visualizations:
            vis_data = vis['_source']
            vis_name = vis['_id'].split("_")[-1]
            vis_id = vis_name+"__"+enrich_index
            vis_data['title'] = vis_id
            vis_meta = json.loads(vis_data['kibanaSavedObjectMeta']['searchSourceJSON'])
            vis_meta['index'] = enrich_index
            vis_data['kibanaSavedObjectMeta']['searchSourceJSON'] = json.dumps(vis_meta)
            if "savedSearchId" in vis_data:
                vis_data["savedSearchId"] = search_id

            url = item_template_url+"/"+vis_id

            r = requests.post(url, data = json.dumps(vis_data), verify=False)
            logging.debug("Created new vis %s" % (url))

    if not es_index:
        es_index = ".kibana"

    # First create always the index pattern as data source
    index_pattern = create_index_pattern(elastic_url, dashboard, enrich_index, es_index)
    # If search is used create a new search with the new index_pàttern
    search_id = create_search(elastic_url, dashboard, index_pattern, es_index)

    elastic = ElasticSearch(elastic_url, es_index)

    # Create the new dashboard from the template
    dash_data = get_dashboard_json(elastic, dashboard)
    dash_data['title'] = enrich_index
    # Load template panels to create the new ones with their new vis
    panels = json.loads(dash_data['panelsJSON'])
    dash_data['panelsJSON'] = json.dumps(new_panels(elastic, panels, search_id))
    dash_path = "/dashboard/"+dashboard+"__"+enrich_index
    url = elastic.index_url+dash_path
    requests.post(url, data = json.dumps(dash_data), verify=False)

    dash_url = kibana_host+"/app/kibana#"+dash_path
    return dash_url

if __name__ == '__main__':

    args = get_params()

    config_logging(args.debug)
    # TODO: Use param to build a real URL if possible
    kibana_host = "http://localhost:5601"

    try:
        url = create_dashboard(args.elastic_url, args.dashboard, args.index, kibana_host, args.kibana_index)
    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        sys.exit(0)


    logging.info("Kibana dashboard generated %s" % (url))
