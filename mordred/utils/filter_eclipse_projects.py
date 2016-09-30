#!/usr/bin/env python
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

import argparse
import json
import sys

def parse_args():
    parser = argparse.ArgumentParser()

    # Options
    parser.add_argument('-f', '--input_file',
                        dest='input_file',
                        help='Input file', default='')

    parser.add_argument('-o', '--output', dest='output_file',
                        help='Output file', default='')

    args = parser.parse_args()

    return args


def filter_json(content):
    output = {"projects":{}}
    tree = json.loads(content)
    count_repos = 0
    for p in tree["projects"]:
        output["projects"][p]={}

        res_repos = []
        for rep in tree["projects"][p]["source_repo"]:
            url = rep["url"]
            url = url.replace("git.eclipse.org/c/","git.eclipse.org/gitroot/")
            res_repos.append({"url":url})
        count_repos += len(tree["projects"][p]["source_repo"])
        output["projects"][p]["source_repo"] = res_repos

        output["projects"][p]["title"] = tree["projects"][p]["title"]
        output["projects"][p]["parent_project"] = tree["projects"][p]["parent_project"]
        output["projects"][p]["bugzilla"] = tree["projects"][p]["bugzilla"]
        output["projects"][p]["mailing_lists"] = tree["projects"][p]["mailing_lists"]

    print("Converting project/repos JSON to Grimoire format from Eclipse")
    print("- %s projects" % len(tree["projects"]))
    print("- %s repositories" % str(count_repos))

    return output


if __name__ == '__main__':
    args = parse_args()
    with open(args.input_file) as fd:
        data = filter_json(fd.read())
        #dump data
    if data:
        with open(args.output_file,'w') as fd:
            json.dump(data, fd)
