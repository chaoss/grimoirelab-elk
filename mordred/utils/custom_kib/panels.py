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
import fnmatch
import json
import os


# get the list of panels from the config file
def get_panels_2(config_file, sources, has_projects):
    f = open(config_file, "r")
    config = json.load(f)
    f.close()

    for data_sources in config:
        for source in config[data_sources]:
            if ((source["name"] in sources) or source["name"] == "any"):
                for panel in source["panels"]:
                    print(source["name"] + ": " + panel["files"])

def get_panels(config_file, sources, has_projects):
    f = open(config_file, "r")
    config = json.load(f)
    f.close()

    panels_dir = config_file.split("/config.json")[0]

    for data_sources in config:
        for source in config[data_sources]:
            if ((source["name"] in sources) or source["name"] == "any"):
                for panel in source["panels"]:
                    for f in os.listdir(panels_dir):
                            if fnmatch.fnmatch(f, panel["files"]):
                                print(f)
