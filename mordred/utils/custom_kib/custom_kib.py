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
from git import Repo
import os
import shutil

import alias
import index
import list_sources
import panels
import parser
import tabs


## global vars
git_panels_url = "https://github.com/grimoirelab/panels.git"
tmp_dir = "/tmp/"
panels_dir = tmp_dir + "panels"


##
args = parser.usage()

if not args.file:
    if os.path.exists(panels_dir):
        shutil.rmtree(panels_dir)

    Repo.clone_from(git_panels_url, panels_dir)
    input_file = panels_dir + "/dashboards/config.json"
else:
    input_file = args.file

if (args.command == "sourceslist"):
    list_sources.list_data_sources(input_file)
elif (args.command == "tabs"):
    if (args.action == "get"):
        tabs.get_tabs(args.es_url + "/.kibana/metadashboard/main")
    elif (args.action == "add"):
        tabs.add_tabs(input_file, args.es_url + "/.kibana/metadashboard/main", args.sources)
    elif (args.action == "rm"):
        tabs.remove_tabs(input_file, args.es_url + "/.kibana/metadashboard/main", args.sources)
    else:
        print("This action is not suported")
        parser.usage()
        exit(-1)
elif (args.command == "panels"):
    panels.get_panels(input_file, args.sources, args.has_projects)
elif (args.command == "aliases"):
    if (args.action == "get"):
        alias.get_alias(args.es_url + "/_cat/aliases?v")
    elif (args.action == "add"):
        alias.add_alias(args.es_url + "/_aliases", args.alias, args.new_index)
    elif (args.action == "replace"):
        alias.replace_alias(args.es_url + "/_aliases", args.alias, args.old_index, args.new_index)
    elif (args.action == "remove"):
        alias.remove_alias(args.es_url + "/_aliases", args.alias, args.old_index)
    else:
        print("This action is not suported")
        parser.usage()
        exit(-1)
elif (args.command == "indexes"):
    if (args.action == "get"):
        index.get_index(args.es_url + "/_cat/indices?v")
    elif (args.action == "remove"):
        index.remove_index(args.es_url, args.index)
    else:
        print("This action is not suported")
        parser.usage()
        exit(-1)
else:
    parser.usage()
    exit(-1)
