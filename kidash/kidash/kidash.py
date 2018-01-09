#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Panels lib
#
# Copyright (C) 2016 Bitergia
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

import copy
import json
import logging

import os
import os.path
import pkgutil
import sys

from grimoire_elk.elk.elastic import ElasticSearch
from grimoire_elk.elk.utils import grimoire_con

logger = logging.getLogger(__name__)

requests_ses = grimoire_con()

ES_VER = None
HEADERS_JSON = {"Content-Type": "application/json"}


def find_elasticsearch_version(elastic):
    global ES_VER
    if not ES_VER:
        res = requests_ses.get(elastic.url)
        main_ver = res.json()['version']['number'].split(".")[0]
        ES_VER = int(main_ver)
    return ES_VER


def find_item_json(elastic, type_, item_id):
    """ Find and item (dashboard, vis, search, index pattern) using its id """
    elastic_ver = find_elasticsearch_version(elastic)

    if elastic_ver < 6:
        item_json_url = elastic.index_url + "/" + type_ + "/" + item_id
    else:
        if type_ not in item_id:
            # Inside a dashboard ids don't include type_:
            item_id = type_ + ":" + item_id
        # The type_is included in the item_id
        item_json_url = elastic.index_url + "/doc/" + item_id

    res = requests_ses.get(item_json_url, verify=False)
    if res.status_code == 200 and res.status_code == 404:
        res.raise_for_status()

    item_json = res.json()

    if "_source" not in item_json:
        logger.debug("Can not find type %s item %s", type_, item_id)
        item_json = {}
    else:
        if elastic_ver < 6:
            item_json = item_json["_source"]
        else:
            item_json = item_json["_source"][type_]

    return item_json


def clean_dashboard_for_data_sources(dash_json, data_sources):
    """ Remove all items that are not from the data sources """

    logger.debug("Cleaning dashboard for %s", data_sources)

    dash_json_clean = copy.deepcopy(dash_json)

    dash_json_clean['uiStateJSON'] = ""
    dash_json_clean['panelsJSON'] = ""

    # Time to add the panels (widgets) related to the data_sources
    panelsJSON = json.loads(dash_json['panelsJSON'])
    clean_panelsJSON = []
    for panel in panelsJSON:
        for ds in data_sources:
            if panel['id'].split("_")[0] == ds:
                clean_panelsJSON.append(panel)
                break
    dash_json_clean['panelsJSON'] = json.dumps(clean_panelsJSON)

    return dash_json_clean


def import_item_json(elastic, type_, item_id, item_json, data_sources=None):
    """ Import an item in Elasticsearch  """
    elastic_ver = find_elasticsearch_version(elastic)

    if data_sources:
        if type_ == 'dashboard':
            item_json = clean_dashboard_for_data_sources(item_json, data_sources)
        if type_ == 'search':
            if not is_search_from_data_sources(item_json, data_sources):
                logger.debug("Search %s not for %s. Not included.", item_id, data_sources)
                return
        elif type_ == 'index_pattern':
            if not is_index_pattern_from_data_sources(item_json, data_sources):
                logger.debug("Index pattern %s not for %s. Not included.", item_id, data_sources)
                return
        elif type_ == 'visualization':
            if not is_vis_from_data_sources(item_json, data_sources):
                logger.debug("Vis %s not for %s. Not included.", item_id, data_sources)
                return

    if elastic_ver < 6:
        item_json_url = elastic.index_url + "/" + type_ + "/" + item_id
    else:
        if type_ not in item_id:
            # Inside a json dashboard ids don't include type_
            item_id = type_ + ":" + item_id
        item_json_url = elastic.index_url + "/doc/" + item_id
        item_json = {"type": type_, type_: item_json}

    headers = HEADERS_JSON
    res = requests_ses.post(item_json_url, data=json.dumps(item_json),
                            verify=False, headers=headers)
    res.raise_for_status()

    return item_json


def exists_dashboard(elastic_url, dash_id, es_index=None):
    """ Check if a dashboard exists """
    exists = False

    if not es_index:
        es_index = ".kibana"
    elastic = ElasticSearch(elastic_url, es_index)
    dash_data = get_dashboard_json(elastic, dash_id)
    if 'panelsJSON' in dash_data:
        exists = True

    return exists


def get_dashboard_json(elastic, dashboard_id):
    dash_json = find_item_json(elastic, "dashboard", dashboard_id)

    return dash_json


def get_vis_json(elastic, vis_id):
    vis_json = find_item_json(elastic, "visualization", vis_id)

    return vis_json


def get_search_json(elastic, search_id):
    search_json = find_item_json(elastic, "search", search_id)

    return search_json


def get_index_pattern_json(elastic, index_pattern_id):
    index_pattern_json = find_item_json(elastic, "index-pattern", index_pattern_id)

    return index_pattern_json


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
        logger.error("Can not find vis in dashboard: %s", dashboard)
        raise

    # Get the search from the first vis in the panel
    for panel in json.loads(dash_data["panelsJSON"]):
        panel_id = panel["id"]
        logger.debug("Checking search in %s vis", panel_id)

        search_id = get_search_from_vis(elastic, panel_id)
        if search_id:
            break

    # And now time to create the search found
    if not search_id:
        logger.info("Can't find search  %s", dashboard)
        return

    logger.debug("Found template search %s", search_id)

    search_json = get_search_json(elastic, search_id)
    search_source = search_json['kibanaSavedObjectMeta']['searchSourceJSON']
    new_search_source = json.loads(search_source)
    new_search_source['index'] = index_pattern
    new_search_source = json.dumps(new_search_source)
    search_json['kibanaSavedObjectMeta']['searchSourceJSON'] = new_search_source

    search_json['title'] += " " + index_pattern
    new_search_id = search_id + "__" + index_pattern

    url = elastic.index_url + "/search/" + new_search_id
    headers = {"Content-Type": "application/json"}
    res = requests_ses.post(url, data=json.dumps(search_json),
                            verify=False, headers=headers)
    res.raise_for_status()

    logger.debug("New search created: %s", url)

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


def get_index_pattern_from_search(elastic, search):
    index_pattern = None
    search_json = get_search_json(elastic, search)
    if not search_json:
        return
    if "kibanaSavedObjectMeta" in search_json:
        index_pattern = get_index_pattern_from_meta(search_json["kibanaSavedObjectMeta"])
    return index_pattern


def get_index_pattern_from_vis(elastic, vis):
    index_pattern = None
    vis_json = get_vis_json(elastic, vis)
    if not vis_json:
        return
    # The index pattern could be in search or in state
    # First search for it in saved search
    if "savedSearchId" in vis_json:
        search_json = find_item_json(elastic, "search", vis_json["savedSearchId"])
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
        logger.error("Can not find vis in dashboard: %s", dashboard)
        raise

    # Get the index pattern from the first vis in the panel
    # that as index pattern data
    for panel in json.loads(dash_data["panelsJSON"]):
        panel_id = panel["id"]
        logger.debug("Checking index pattern in %s vis", panel_id)

        index_pattern = get_index_pattern_from_vis(elastic, panel_id)
        if index_pattern:
            break

    # And now time to create the index pattern found
    if not index_pattern:
        logger.error("Can't find index pattern for %s", dashboard)
        raise

    logger.debug("Found %s template index pattern", index_pattern)

    new_index_pattern_json = get_index_pattern_json(elastic, index_pattern)

    new_index_pattern_json['title'] = enrich_index
    url = elastic.index_url + "/index-pattern/" + enrich_index
    headers = {"Content-Type": "application/json"}
    res = requests_ses.post(url, data=json.dumps(new_index_pattern_json),
                            verify=False, headers=headers)
    res.raise_for_status()
    logger.debug("New index pattern created: %s", url)

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
                panel['id'] += "__" + enrich_index
                if panel['type'] == 'search':
                    panel['id'] = search_id
            new_panels.append(panel)

        create_vis(elastic, dash_vis_ids, search_id)

        return new_panels

    def create_vis(elastic, dash_vis_ids, search_id):
        """ Create new visualizations for the dashboard """

        # Create visualizations for the new dashboard
        item_template_url = elastic.index_url + "/visualization"
        # Hack: Get all vis if they are <10000. Use scroll API to get all.
        # Better: use mget to get all vis in dash_vis_ids
        item_template_url_search = item_template_url + "/_search?size=10000"
        res = requests_ses.get(item_template_url_search, verify=False)
        res.raise_for_status()
        all_visualizations = res.json()['hits']['hits']

        visualizations = []
        for vis in all_visualizations:
            if vis['_id'] in dash_vis_ids:
                visualizations.append(vis)

        logger.info("Total template vis found: %i", len(visualizations))

        for vis in visualizations:
            vis_data = vis['_source']
            vis_name = vis['_id'].split("_")[-1]
            vis_id = vis_name + "__" + enrich_index
            vis_data['title'] = vis_id
            vis_meta = json.loads(vis_data['kibanaSavedObjectMeta']['searchSourceJSON'])
            vis_meta['index'] = enrich_index
            vis_data['kibanaSavedObjectMeta']['searchSourceJSON'] = json.dumps(vis_meta)
            if "savedSearchId" in vis_data:
                vis_data["savedSearchId"] = search_id

            url = item_template_url + "/" + vis_id

            headers = {"Content-Type": "application/json"}
            res = requests_ses.post(url, data=json.dumps(vis_data),
                                    verify=False, headers=headers)
            res.raise_for_status()
            logger.debug("Created new vis %s", url)

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
    dash_path = "/dashboard/" + dashboard + "__" + enrich_index
    url = elastic.index_url + dash_path
    res = requests_ses.post(url, data=json.dumps(dash_data), verify=False, headers=HEADERS_JSON)
    res.raise_for_status()
    dash_url = kibana_host + "/app/kibana#" + dash_path
    return dash_url


def search_dashboards(elastic_url, es_index=None):

    dashboards = []

    if not es_index:
        es_index = ".kibana"

    elastic = ElasticSearch(elastic_url, es_index)
    elastic_ver = find_elasticsearch_version(elastic)

    if elastic_ver < 6:
        dash_json_url = elastic.index_url + "/dashboard/_search?size=10000"
        res = requests_ses.get(dash_json_url, verify=False)
    else:
        items_json_url = elastic.index_url + "/_search?size=10000"
        query = '''
        {
            "query" : {
                "term" : { "type" : "dashboard"  }
             }
        }'''
        res = requests_ses.post(items_json_url, data=query, verify=False,
                                headers=HEADERS_JSON)
    res.raise_for_status()

    res_json = res.json()

    if "hits" not in res_json:
        logger.error("Can't find dashboards")
        raise RuntimeError("Can't find dashboards")

    for dash in res_json["hits"]["hits"]:
        if elastic_ver < 6:
            dash_json = dash["_source"]
        else:
            dash_json = dash["_source"]["dashboard"]

        dashboards.append({"_id": dash["_id"], "title": dash_json["title"]})

    return dashboards


def list_dashboards(elastic_url, es_index=None):

    dashboards = search_dashboards(elastic_url, es_index)
    for dash in dashboards:
        print("_id:%s title:%s" % (dash["_id"], dash["title"]))


def read_panel_file(panel_file):
    """Read a panel file (in JSON format) and return its contents.

    :param panel_file: name of JSON file with the dashboard to read
    :returns: dictionary with dashboard read,
                None if not found or wrong format
    """

    if os.path.isfile(panel_file):
        logger.debug("Reading panel from directory: %s", panel_file)
        with open(panel_file, 'r') as f:
            kibana_str = f.read()
    else:
        try:
            import panels
            panels_mod = panels
            logger.debug("Reading panel from module panels")
            # Next is just a hack for files with "expected" prefix
            if panel_file.startswith('panels/json/'):
                module_file = panel_file[len('panels/json/'):]
            else:
                module_file = panel_file
            kibana_bytes = pkgutil.get_data('panels', 'json' + '/' + module_file)
            kibana_str = kibana_bytes.decode(encoding='utf8')
        except (ImportError, FileNotFoundError, AttributeError):
            logger.error("Panel not found (not in directory, no panels module): %s",
                         panel_file)
            return None

    try:
        kibana_dict = json.loads(kibana_str)
    except ValueError:
        logger.error("Wrong file format (not JSON): %s", module_file)
        return None
    return kibana_dict


def get_dashboard_name(panel_file):
    """ Return the dashboard name included in a JSON panel file """

    dash_name = None

    kibana = read_panel_file(panel_file)
    if kibana and 'dashboard' in kibana:
        dash_name = kibana['dashboard']['id']
    elif kibana:
        logger.error("Wrong panel format (can't find 'dashboard' field): %s",
                     panel_file)
    return dash_name


def is_search_from_data_sources(search, data_sources):
    found = False
    index_pattern = get_index_pattern_from_meta(search['kibanaSavedObjectMeta'])

    for data_source in data_sources:
        # ex: github_issues
        if data_source == index_pattern.split("_")[0]:
            found = True
            break

    return found


def is_vis_from_data_sources(vis, data_sources):
    found = False
    vis_title = vis['value']['title']

    for data_source in data_sources:
        # ex: github_issues_evolutionary
        if data_source == vis_title.split("_")[0]:
            found = True
            break

    return found


def is_index_pattern_from_data_sources(index, data_sources):
    found = False
    es_index = index['value']['title']

    for data_source in data_sources:
        # ex: github_issues
        if data_source == es_index.split("_")[0]:
            found = True
            break

    return found


def import_dashboard(elastic_url, import_file, es_index=None, data_sources=None):
    """ Import a dashboard from a file
    """

    logger.debug("Reading panels JSON file: %s", import_file)
    dashboard = read_panel_file(import_file)

    if (dashboard is None) or ('dashboard' not in dashboard):
        logger.error("Wrong file format (can't find 'dashboard' field): %s",
                     import_file)
        sys.exit(1)

    feed_dashboard(dashboard, elastic_url, es_index, data_sources)

    logger.info("Dashboard %s imported", get_dashboard_name(import_file))


def feed_dashboard(dashboard, elastic_url, es_index=None, data_sources=None):
    """ Import a dashboard. If data_sources are defined, just include items
        for this data source.
    """

    if not es_index:
        es_index = ".kibana"

    elastic = ElasticSearch(elastic_url, es_index)

    import_item_json(elastic, "dashboard", dashboard['dashboard']['id'],
                     dashboard['dashboard']['value'], data_sources)

    if 'searches' in dashboard:
        for search in dashboard['searches']:
            import_item_json(elastic, "search", search['id'], search['value'], data_sources)

    if 'index_patterns' in dashboard:
        for index in dashboard['index_patterns']:
            if not data_sources or is_index_pattern_from_data_sources(index, data_sources):
                import_item_json(elastic, "index-pattern", index['id'], index['value'])
            else:
                logger.debug("Index pattern %s not for %s. Not included.", index['id'], data_sources)

    if 'visualizations' in dashboard:
        for vis in dashboard['visualizations']:
            if not data_sources or is_vis_from_data_sources(vis, data_sources):
                import_item_json(elastic, "visualization", vis['id'], vis['value'])
            else:
                logger.debug("Vis %s not for %s. Not included.", vis['id'], data_sources)


def fetch_dashboard(elastic_url, dash_id, es_index=None):

    # Kibana dashboard fields
    kibana = {"dashboard": None,
              "visualizations": [],
              "index_patterns": [],
              "searches": []}

    # Used to avoid having duplicates
    search_ids_done = []
    index_ids_done = []

    logger.debug("Fetching dashboard %s", dash_id)
    if not es_index:
        es_index = ".kibana"

    elastic = ElasticSearch(elastic_url, es_index)

    kibana["dashboard"] = {"id": dash_id, "value": get_dashboard_json(elastic, dash_id)}

    if "panelsJSON" not in kibana["dashboard"]["value"]:
        # The dashboard is empty. No visualizations included.
        return kibana

    # Export all visualizations and the index patterns and searches in them
    for panel in json.loads(kibana["dashboard"]["value"]["panelsJSON"]):
        logger.debug("Analyzing panel %s (%s)", panel['id'], panel['type'])
        if panel['type'] in ['visualization']:
            vis_id = panel['id']
            vis_json = get_vis_json(elastic, vis_id)
            kibana["visualizations"].append({"id": vis_id, "value": vis_json})
            search_id = get_search_from_vis(elastic, vis_id)
            if search_id and search_id not in search_ids_done:
                search_ids_done.append(search_id)
                kibana["searches"].append({"id": search_id,
                                           "value": get_search_json(elastic, search_id)})
            index_pattern_id = get_index_pattern_from_vis(elastic, vis_id)
            if index_pattern_id and index_pattern_id not in index_ids_done:
                index_ids_done.append(index_pattern_id)
                kibana["index_patterns"].append({"id": index_pattern_id,
                                                 "value": get_index_pattern_json(elastic, index_pattern_id)})
        elif panel['type'] in ['search']:
            # A search could be directly visualized inside a panel
            search_id = panel['id']
            kibana["searches"].append({"id": search_id,
                                       "value": get_search_json(elastic, search_id)})
            index_pattern_id = get_index_pattern_from_search(elastic, search_id)
            if index_pattern_id and index_pattern_id not in index_ids_done:
                index_ids_done.append(index_pattern_id)
                kibana["index_patterns"].append({"id": index_pattern_id,
                                                 "value": get_index_pattern_json(elastic, index_pattern_id)})

    return kibana


def export_dashboard(elastic_url, dash_id, export_file, es_index=None):

    logger.debug("Exporting dashboard %s to %s", dash_id, export_file)

    kibana = fetch_dashboard(elastic_url, dash_id, es_index)

    with open(export_file, 'w') as f:
        f.write(json.dumps(kibana, indent=4, sort_keys=True))

    logger.debug("Done")
