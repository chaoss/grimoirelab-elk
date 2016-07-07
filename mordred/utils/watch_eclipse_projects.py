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
#     Luis Cañas-Díaz <lcanas@bitergia.com>
#

import json
import sys
import requests
import argparse
import logging


# Logging format
LOG_FORMAT = "[%(asctime)s] - %(message)s"

URL="https://projects.eclipse.org/json/projects/all"

USAGE_MSG = \
"""%(prog)s <file> | --help """

def main():
    logging.basicConfig(level=logging.INFO,
                        format=LOG_FORMAT)

    args = parse_args()
    content = fetch_json()
    new_set = load_new_set(content)
    old_set = load_old_set(args.file)

    removed = old_set - new_set
    new = new_set - old_set

    logging.info("%s projects found in %s" % (len(new_set),URL))
    logging.info(" %s removed projects: %s" % (len(removed), str(removed)))
    logging.info(" %s new projects: %s" % (len(new), str(new)))

    save(content,args.file)

def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(usage=USAGE_MSG,
                                     add_help=True)

    parser.add_argument('file',
                        help='Previous version of projects\' JSON file')

    return parser.parse_args()

def fetch_json():
    """Fetchs the content of the URL"""
    try:
        r = requests.get(URL)
        r.status_code
        return r.text
    except ConnectionError:
        print("ConnectionError Exception")
        sys.exit(1)

def load_new_set(content):
    """Returns a set with the projects for the URL"""
    json_data = json.loads(content) #watch out with the extra s
    k = json_data['projects'].keys()
    return set(k)

def load_old_set(filepath):
    """Returns a set with the projects for the cached version"""
    with open(filepath) as json_file:
        json_data = json.load(json_file)
        p = json_data['projects'].keys()
        return set(p)

def save(content, filepath):
    with open(filepath, "w") as f:
        f.write(content)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        s = "\n\nReceived Ctrl-C or other break signal. Exiting.\n"
        sys.stderr.write(s)
        sys.exit(0)
