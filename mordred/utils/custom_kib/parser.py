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
import argparse
import sys


# usage and argparse
def usage():
    usage = sys.argv[0]+" [-f <file>] <command> | --help"
    parser = argparse.ArgumentParser(usage=usage, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-f', '--file', help='File with the appropriate information for panels.\n\
(https://github.com/grimoirelab/panels/blob/master/dashboards/config.json)\n\n')

    parser.add_argument('command', action='store_true', help='Command you want to execute.')
    subparsers = parser.add_subparsers(dest="command")

    # sourceslist
    usage = sys.argv[0]+" [-f <file>] <sourceslist>"
    list_parser = subparsers.add_parser("sourceslist", help="List of supported sources.", usage=usage)

    # tabs
    usage = sys.argv[0]+" [-f <file>] <tabs> <action> <es_url> [-s <data_sources>]"
    tabs_parser = subparsers.add_parser("tabs", help="Get, add or remove tabs.", usage=usage, formatter_class=argparse.RawTextHelpFormatter)
    tabs_parser.add_argument('action', help='Action you want to run (get|add|rm).')
    tabs_parser.add_argument('es_url', help='URL of the ES you want to use.\n\
(e.g.: https://user:pass@project.biterg.io/data)')
    tabs_parser.add_argument('-s', '--sources', help='List of sources you need. (e.g.: "git, gerrit")')

    # panels
    usage = sys.argv[0]+" [-f <file>] <panels> <sources> <has_projects>"
    panels_parser = subparsers.add_parser("panels", help="Get the list of json files to import.", usage=usage)
    panels_parser.add_argument('sources', help='List of sources you need. (e.g.: "git, gerrit")')
    panels_parser.add_argument('has_projects', help='This value must be set to "True" or "False"')

    # aliases
    usage = sys.argv[0]+" [-f <file>] <aliases> <action> <es_url> [--alias <alias>] [--old-index <old_index>] [--new-index <new_index>]"
    alias_parser = subparsers.add_parser("aliases", help="Get, add, remove or replace aliases.", usage=usage, formatter_class=argparse.RawTextHelpFormatter)
    alias_parser.add_argument('action', help='Action you want to run (get|add|replace|remove).')
    alias_parser.add_argument('es_url', help='URL of the ES you want to use.\n\
(e.g.: https://user:pass@project.biterg.io/data)')
    alias_parser.add_argument('--alias', help='Name of the alias. (e.g.: "git, gerrit")')
    alias_parser.add_argument('--old-index', help='Name of the index currently pointed by the alias.')
    alias_parser.add_argument('--new-index', help='Name of the index which you want the alias to point to.')

    # indexes
    usage = sys.argv[0]+" [-f <file>] <indexes> <action> <es_url> [--index <index>]"
    index_parser = subparsers.add_parser("indexes", help="Get or remove indexes.", usage=usage)
    index_parser.add_argument('action', help='Action you want to run (get|remove).')
    index_parser.add_argument('es_url', help='URL of the ES you want to use. (e.g.: https://user:pass@project.biterg.io/data)')
    index_parser.add_argument('--index', help='Name of the index to remove. (e.g.: "git_project_20160101")')
    args = parser.parse_args()

    if not args.file:
        if len(sys.argv) < 2:
            parser.print_help()
            exit(-1)
    else:
        if len(sys.argv) < 4:
            parser.print_help()
            exit(-1)

    return args
