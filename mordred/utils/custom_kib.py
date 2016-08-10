#!/usr/bin/env python

import sys
import json
import argparse
import pycurl
import StringIO

usage = sys.argv[0]+" [--help] command"
parser = argparse.ArgumentParser(usage=usage)
parser.add_argument('command', action='store_true', help='Command you want to execute.')
subparsers = parser.add_subparsers(dest="command")
usage = sys.argv[0]+" list"
list_parser = subparsers.add_parser("list", help="List of sources supported.", usage=usage)
usage = sys.argv[0]+" tabs es_url sources"
tabs_parser = subparsers.add_parser("tabs", help="Get the list of tabs.", usage=usage)
tabs_parser.add_argument('es_url', help='URL of the ES you want to use. (e.g.: https://user:pass@project.biterg.io/data)')
tabs_parser.add_argument('sources', help='List of sources you need. (e.g.: "git, gerrit")')
usage = sys.argv[0]+" panels sources"
panels_parser = subparsers.add_parser("panels", help="Get the list of json files to import.", usage=usage)
panels_parser.add_argument('sources', help='List of sources you need. (e.g.: "git, gerrit")')
args = parser.parse_args()

config_file = open("/path/config.json", "r")
config = json.load(config_file)
config_file.close()


tabs_value = ""

def list_sources():
    for data_sources in config:
        for source in config[data_sources]:
            print source["name"]

def list_tabs():
    sources_list = args.sources.split(", ")

    for data_sources in config:
        tab_list = {}
        for source in config[data_sources]:
            if ((source["name"] in sources_list) or source["name"] == "any"):
                for panel in source["panels"]:
                    tab_list["\""+panel["name"]+"\":\""+panel["id"]+"\""] = panel["order"]
    ordered_tab_list = sorted(tab_list.items(), key=lambda x: x[1])
    tabs_value = "{\n"
    for cont, item in enumerate(ordered_tab_list):
        if (cont == len(ordered_tab_list)-1):
            tabs_value = tabs_value+item[0]+"\n"
        else:
            tabs_value = tabs_value+item[0]+","+"\n"
    tabs_value = tabs_value+"}"
    return tabs_value

def list_panels():
    sources_list = args.sources.split(", ")

    for data_sources in config:
        for source in config[data_sources]:
            if ((source["name"] in sources_list) or source["name"] == "any"):
                for panel in source["panels"]:
                    print source["name"]+": "+panel["files"]

def curl(es_url, value):
    c = pycurl.Curl()
    c.setopt(pycurl.URL, es_url)
    c.setopt(c.SSL_VERIFYPEER, 0)
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.POSTFIELDS, value)
    c.perform()
    print "\nTabs below have been imported successfully:\n"+value

if (args.command):
    if (args.command == "list"):
        list_sources()
    elif (args.command == "tabs"):
        tabs_value = list_tabs()
        curl(args.es_url+"/.kibana/metadashboard/main",tabs_value)
    elif (args.command == "panels"):
        list_panels()
    else:
        exit(-1)
else:
    exit(-1)
