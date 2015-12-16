#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Perceval to Kibana Publisher
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

from datetime import datetime
import json
import logging
from os import sys
import requests

from grimoire.ocean.conf import ConfOcean
 

from grimoire.utils import get_connector_from_name, get_connectors, get_elastic
from grimoire.utils import get_params, config_logging


def feed_backend(url, params, connectors, clean):
    ''' Feed Ocean with backend data '''

    backend = None
    backend_name = params['backend']
    repo = {}    # repository data to be stored in conf
    repo['params'] = params
    es_index = None

    connector = get_connector_from_name(backend_name, connectors)
    if not connector:
        logging.error("Cant find %s backend" % (backend_name))
        sys.exit(1)

    try:
        backend = connector[0](**params)
        ocean_backend = connector[1](backend, **params)

        logging.info("Feeding Ocean from %s (%s)" % (backend.get_name(),
                                                     backend.get_id()))

        es_index = backend.get_name() + "_" + backend.get_id()
        elastic_ocean = get_elastic(url, es_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)

        ConfOcean.set_elastic(elastic_ocean)

        ocean_backend.feed()
    except Exception as ex:
        if backend:
            logging.error("Error feeding ocean from %s (%s): %s" %
                          (backend.get_name(), backend.get_id()), ex)
        else:
            logging.error("Error feeding ocean %s" % ex)

        repo['success'] = False
        repo['error'] = ex
    else:
        repo['success'] = True

    repo['repo_update'] = datetime.now().isoformat()

    if es_index:
        ConfOcean.add_repo(es_index, repo)
    else:
        logging.debug("Repository not added to Ocean because errors.")
        logging.debug(params)

    logging.info("Done %s " % (backend_name))


def create_items(params, connectors, kind):
    ''' Create an item if it does not exists yet '''

    kinds = ['index-pattern','dashboard']

    if kind not in kinds:
        logging.error("Can not get template for %s" % (kind))
        raise

    backend_name = params['backend']
    url = params['elastic_url']

    connector = get_connector_from_name(backend_name, connectors)
    if not connector:
        logging.error("Cant find %s backend" % (backend_name))
        sys.exit(1)


    backend = connector[0](**params)
    ds = backend.get_name()
    item_id = ds+"_"+backend.get_id()

    # Get the index pattern template
    es_index = "/.kibana"
    elastic = get_elastic(url, es_index)


    item_template_url = elastic.index_url+"/"+kind
    item_template_url_search = item_template_url+"/_search"


    query = '''
        {"query": {"prefix": {"_id": {"value": "%s_" }}}}
    ''' % (ds)

    r = requests.post(item_template_url_search, data = query)

    item_templates =r.json()['hits']['hits']

    if len(item_templates) == 0:
        logging.error("Can not find template/s for data source: %s" % (ds))
        print (item_template_url_search, query)
        raise

    if kind in ['index-pattern','dashboard']:
        # All items can be used as template. Use the first one.
        item = item_templates[0]["_source"]

        if kind == 'index-pattern':
            item['title'] = item_id
            url = item_template_url+"/"+item_id
            r = requests.post(url, data = json.dumps(item))

        elif kind == 'dashboard':

            dash_vis_ids = []

            item['title'] = item_id
            # Update visualizations
            panels = json.loads(item['panelsJSON'])
            new_panels = []
            for panel in panels:
                if panel['type'] == 'visualization':
                    dash_vis_ids.append(panel['id'])
                    vid = panel['id'].split("_")[-1]
                    panel['id'] = item_id+"_"+vid
                new_panels.append(panel)
            item['panelsJSON'] = json.dumps(new_panels)

            url = item_template_url+"/"+item_id
            r = requests.post(url, data = json.dumps(item))

            # Time to add visualizations
            kind = 'visualization'
            item_template_url = elastic.index_url+"/"+kind
            item_template_url_search = item_template_url+"/_search"

            r = requests.get(item_template_url_search)

            all_visualizations =r.json()['hits']['hits']

            visualizations = []
            for vis in all_visualizations:
                if vis['_id'] in dash_vis_ids:
                    visualizations.append(vis)

            logging.info("Total template vis found: %i" % (len(visualizations)))

            # Time to add all visualizations for new dashboard
            for vis in visualizations:
                vis_data = vis['_source']
                vis_name = vis['_id'].split("_")[-1]
                vis_id = item_id + "_" + vis_name
                vis_data['title'] = vis_id
                vis_meta = json.loads(vis_data['kibanaSavedObjectMeta']['searchSourceJSON'])
                vis_meta['index'] = item_id
                vis_data['kibanaSavedObjectMeta']['searchSourceJSON'] = json.dumps(vis_meta)

                url = item_template_url+"/"+vis_id

                r = requests.post(url, data = json.dumps(vis_data))


def create_index_pattern(params, connectors):
    logging.debug("Generating index pattern")

    create_items(params, connectors, 'index-pattern')


def create_dashboard(params, connectors):
    ''' Create the dashboard and its visualizations '''
    logging.debug("Generating dashboard")

    create_items(params, connectors, 'dashboard')


if __name__ == '__main__':

    app_init = datetime.now()

    args = get_params()

    config_logging(args.debug)

    clean = args.no_incremental
    if args.cache:
        clean = True

    try:
        feed_backend(args.elastic_url, vars(args), connectors, clean)
        # Time to create Kibana dashbnoard
        logging.info("Generating Kibana dashboard")
        create_index_pattern(vars(args), connectors)
        create_dashboard(vars(args), connectors)

    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        sys.exit(0)


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
