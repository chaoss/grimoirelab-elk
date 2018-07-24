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
#     Alberto Martín <alberto.martin@bitergia.com>
#     Jesus M. Gonzalez-Barahona <jgb@gsyc.es>
#     Alvaro del Castillo <acs@bitergia.com>
#

import argparse
import json

import requests
import bs4
import re

GOOGLE_SEARCH_URL = 'https://www.google.com/search'

description = """
Uses Google API to get list of hits of some keywords.
"""


def parse_args():
    """
    Parse command line arguments

    """

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('keywords', nargs='+',
                        help="Keywords to search as Google hits")
    args = parser.parse_args()
    return args


def get_hits(keyword):
    """
    Get the Google Hits for a keyword
    """
    params = {'q': keyword}
    # Make the request
    req = requests.get(GOOGLE_SEARCH_URL, params=params)
    # Create the soup and get the desired div
    bs_result = bs4.BeautifulSoup(req.text, 'html.parser')
    hit_string = bs_result.find("div", id="resultStats").text
    # Remove commas or dots
    hit_string = hit_string.replace(',', u'')
    hit_string = hit_string.replace('.', u'')
    # Strip the hits
    hits = re.search('\d+', hit_string).group(0)
    keyword_json = {
        "hits": hits,
        "type": "googleSearchHits",
        "keywords": [keyword]
    }
    return keyword_json


args = parse_args()

json_hits = []

for keyword in args.keywords:
    json_hits.append(get_hits(keyword))

print(json.dumps(json_hits, indent=4, sort_keys=True))
