#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Sample API REST for creating Kibana dashboards
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

from redis import Redis
from rq import Queue

from flask import Flask, request, Response
from grimoire.utils import get_params_parser, config_logging, get_connectors
from grimoire.arthur import feed_backend, enrich_backend

from e2k import create_dashboard, get_params_parser_create_dash

app = Flask(__name__)

params_fields = ['p2o_params','e2k_params']  # Params needed to create a dashboard

def check_params(params):
    """Check if we have all params to create the dashboard"""
    check = True
    for f in params_fields:
        if f not in params:
            logging.info("Param not received %s " % (f))
            check = False
            break
    return check

def get_backend_id(backend_name, backend_params):

    if backend_name not in get_connectors():
        raise RuntimeError("Unknown backend %s" % backend_name)
    connector = get_connectors()[backend_name]
    klass = connector[3]  # BackendCmd for the connector

    backend_cmd = klass(*backend_params)

    backend = backend_cmd.backend

    return backend.unique_id

def build_dashboard(params):
    parser = get_params_parser()
    parser_create_dash = get_params_parser_create_dash()
    args = parser.parse_args(params['p2o_params'].split())

    config_logging(args.debug)

    url = args.elastic_url
    clean = False
    async_ = True

    q = Queue('create', connection=Redis(args.redis), async=async_)
    task_feed = q.enqueue(feed_backend, url, clean, args.fetch_cache,
                          args.backend, args.backend_args)
    q = Queue('enrich', connection=Redis(args.redis), async=async_)
    if async_:
        # Task enrich after feed
        result = q.enqueue(enrich_backend, url, clean,
                           args.backend, args.backend_args,
                           depends_on=task_feed)
    else:
        result = q.enqueue(enrich_backend, url, clean,
                           args.backend, args.backend_args)

    result = q.enqueue(enrich_backend, url, clean,
                       args.backend, args.backend_args,
                       depends_on=task_feed)
    # The creation of the dashboard is quick. Do it sync and return the URL.
    enrich_index = args.backend+"_"
    enrich_index += get_backend_id(args.backend, args.backend_args)+"_enrich"
    args = parser_create_dash.parse_args(params['e2k_params'].split())
    kibana_host = "http://localhost:5601"
    dash_url = create_dashboard(args.elastic_url, args.dashboard, enrich_index, kibana_host)

    return dash_url

@app.route("/api/dashboard",methods = ['POST'])
def dashboard():
    """Create a new dashboard using params from POST"""
    if request.headers['Content-Type'] == 'application/json':
        params = request.json
        print(request.json)
        if not check_params(params):
            error = "Error in params. Please include %s" % (", ".join(params_fields))
            msg = {"params": json.dumps(params), \
                   "error": error}
            resp = Response(json.dumps(msg), status=400, mimetype='application/json')
            return resp
        else:
            url = build_dashboard(params)
            msg = json.dumps({"url": url})
            resp = Response(msg, status=200, mimetype='application/json')
        return resp


if __name__ == "__main__":
    app.debug = True
    # app.run()
    # To access the REST server from the network
    app.run(host= '0.0.0.0')