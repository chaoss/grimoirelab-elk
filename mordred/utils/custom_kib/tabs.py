#!/usr/bin/env python3

# -*- coding: utf-8 -*-
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
#     David Pose Fernández <dpose@bitergia.com>
#


# imports
import io
import json
import http


# order tabs using the configuration file as reference
def order_tabs(tab_list):
    tabs_value = ""
    ordered_tab_list = sorted(tab_list.items(), key=lambda x: x[1])
    for cont, item in enumerate(ordered_tab_list):
        if (cont == len(ordered_tab_list)-1):
            tabs_value = tabs_value+item[0]+"\n"
        else:
            tabs_value = tabs_value+item[0]+","+"\n"

    return tabs_value

# format of tabs for the POST operation
def tabs_to_post(tab_list):
    tabs_value = "{\n"
    tabs_value = tabs_value + tab_list
    tabs_value = tabs_value + "}"

    return tabs_value

# get the list of tabs for a specific data sources
def list_tabs(config_file, sources):
    f = open(config_file, "r")
    config = json.load(f)
    f.close()

    for data_sources in config:
        tab_list = {}
        for source in config[data_sources]:
            if ((source["name"] in sources) or source["name"] == "any"):
                for panel in source["panels"]:
                    tab_list["\""+panel["name"]+"\":\""+panel["id"]+"\""] = panel["order"]

    return tab_list

# get the list of data sources for a specific tabs
def list_data_sources(config_file, tabs):
    f = open(config_file, "r")
    config = json.load(f)
    f.close()

    for data_sources in config:
        data_sources_list = []
        for source in config[data_sources]:
            for panel in source["panels"]:
                if (panel["name"] in tabs) and (source["name"] not in data_sources_list):
                    data_sources_list.append(source["name"])

    return data_sources_list

# get the list of tabs from an instance of kibana
def get_tabs(es_url, add="False"):
    response = io.BytesIO()
    response = http.get(es_url, response)
    project_url = es_url.split("@")[1].split("/.kibana")[0]
    value = json.loads(response.getvalue().decode('UTF-8'))
    response.close()

    if value['found'] == True:
        msg_value = json.dumps(value['_source'], indent=1)
    else:
        msg_value = ""
    if add == "False":
        print("List of tabs ("+project_url+"):\n"+msg_value)

    return msg_value

# add default tabs to an instance of kibana
def add_default_tabs(config_file, es_url):
    print("Not implemented yet!")

# add tabs to an instance of kibana
def add_tabs(config_file, es_url, sources=None):
    if sources == None:
        add_default_tabs(config_file, es_url)
    else:
        tabs = get_tabs(es_url, "True")
        data_sources = list_data_sources(config_file, json.loads(tabs))
        for source in sources.split(", "):
            data_sources.append(source)
        tabs_list = list_tabs(config_file, data_sources)
        ordered_tabs_list = order_tabs(tabs_list)
        sources = tabs_to_post(ordered_tabs_list)
    http.post(es_url, sources, "\nTabs have been added successfully. Current tabs at (" + es_url.split("@")[1].split("/.kibana")[0] + "):\n")

# remove tabs from an instance of kibana
def remove_tabs(config_file, es_url, sources=None):
    tabs = get_tabs(es_url, "True")
    data_sources = list_data_sources(config_file, tabs)
    ds = list(data_sources)
    if sources == None:
        for source in ds:
            data_sources.remove(source)
    else:
        for source in sources.split(", "):
            data_sources.remove(source)
    tabs_list = list_tabs(config_file, data_sources)
    ordered_tabs_list = order_tabs(tabs_list)
    sources = tabs_to_post(ordered_tabs_list)
    http.post(es_url, sources, "\nTabs have been removed successfully. Current tabs at (" + es_url.split("@")[1].split("/.kibana")[0] + "):\n")
