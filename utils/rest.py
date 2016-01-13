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

from flask import Flask, request, Response
import json

app = Flask(__name__)

def check_params(params):
    """Check if we have all params to create the dashboard"""
    return False

def create_dashboard(params):
    dash_url = "http://localhost:5601"
    return dash_url

@app.route("/api/dashboard",methods = ['POST'])
def dashboard():
    """Create a new dashboard using params from POST"""
    if request.headers['Content-Type'] == 'application/json':
        params = request.json
        print(request.json)
        if not check_params(params):
            resp = Response(json.dumps(params), status=502, mimetype='application/json')
            return resp
        url = create_dashboard(params)
        return url


if __name__ == "__main__":
    app.debug = True
    # app.run()
    # To access the REST server from the network
    app.run(host= '0.0.0.0')