#!/usr/bin/python3
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

from bs4 import BeautifulSoup
import urllib3
import argparse
import sys

USAGE_MSG = \
"""%(prog)s <url> | --help """

DESC_MSG = \
"""Script to get the list of Mercurial repositories from a hgweb"""

def parse_args():
    parser = argparse.ArgumentParser(usage=USAGE_MSG,
                                     description=DESC_MSG,
                                     add_help=False)
    parser.add_argument('--help', action='help',
                       help=argparse.SUPPRESS)
    parser.add_argument('url', help=argparse.SUPPRESS)

    if len(sys.argv) != 2:
        parser.print_help()
        sys.exit(1)

    return parser.parse_args()

def get_sections(soup):
    sections = []
    links = soup.findAll("a", href=True)
    for a in links:
        link = a['href']
        if link.startswith('/'): sections.append(link)
    return sections

def get_repos(soup):
    repos = []
    links = soup.findAll("a", href=True)
    for a in links:
        link = a['href']
        if link.startswith('/') \
        and not link.endswith('.zip') and not link.endswith('.gz') \
        and not link.endswith('.bz2') and not link.endswith('rss-log') \
        and not link.endswith('atom-log'):
            repos.append(link)
    return repos

def get_soup_tables(http, url):
    r = http.request('GET', url)
    content = r.data
    soup = BeautifulSoup(content)
    tables = soup.findAll("table")
    return tables

def main():
    args = parse_args()
    url=args.url
    http = urllib3.PoolManager()
    tables = get_soup_tables(http, url)
    sections = get_sections(tables[1])

    for s in sections:
        sect_url = url + s
        s_tables = get_soup_tables(http, sect_url)
        repos = get_repos(s_tables[0])

        for r in repos:
            print(url,r,sep='')

if __name__ == "__main__":
    main()
