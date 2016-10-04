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


USAGE_MSG = \
"""%(prog)s -f <input_file> -y <hierarchy_file> -p <projects_file>
"""

def parse_args():
    parser = argparse.ArgumentParser(usage=USAGE_MSG)

    # Options
    parser.add_argument('-f', '--input_file', dest='input_file',
                        help='Input file', required=True)

    parser.add_argument('-y', '--hierarchy', dest='hierarchy_file',
                        help='Output file for hierarchy', required=True)

    parser.add_argument('-p', '--projects', dest='projects_file',
                        help='Output file for projects', required=True)

    args = parser.parse_args()

    return args


def get_project_data(content):
    ##
    ## Returns dictionary whose key is the name/id of the project. The value
    ## contains metadata and the repositories associated
    ##
    output = {}
    tree = json.loads(content)
    for p in tree["projects"]:
        output[p]={}

        meta = {}
        git_repos = []
        bugzilla_trackers = []
        pipermail_archives = []

        for rep in tree["projects"][p]["source_repo"]:
            url = rep["url"]
            url = url.replace("git.eclipse.org/c/","git.eclipse.org/gitroot/")
            git_repos.append(url)

        if tree["projects"][p]["bugzilla"]:
            bugzilla_trackers.append(tree["projects"][p]["bugzilla"][0]["query_url"])

        if tree["projects"][p]["mailing_lists"]:
            for ml in tree["projects"][p]["mailing_lists"]:
                if ml["url"].find("mailto:") >= 0: continue
                pipermail_archives.append(ml["url"])
        if tree["projects"][p]["dev_list"]:
            pipermail_archives.append(tree["projects"][p]["dev_list"]["url"])

        meta["title"] = tree["projects"][p]["title"]
        #meta["description"] = tree["projects"][p]["description"]
        meta["origin"] = "Eclipse Foundation JSON file"

        output[p]["meta"] = meta
        if len(git_repos) > 0: output[p]["git"] = git_repos
        if len(bugzilla_trackers) > 0: output[p]["bugzilla"] = bugzilla_trackers
        if len(pipermail_archives) > 0: output[p]["pipermail"] = pipermail_archives

    #print("Converting project/repos JSON to Grimoire format from Eclipse")
    #print("- %s projects" % len(tree["projects"]))

    return output

def _get_mother_son(tree):
    ##
    ## Receives as input a json tree.
    ## Return a dict with "mother project":[list of daughters]
    ##
    output = {}
    for p in tree["projects"]:
        pp = tree["projects"][p]["parent_project"]
        #print("RAW: son %s - mother %s" % (p, str(pp)))
        if pp:
            mother_id = pp[0]["id"]
        else:
            mother_id = "ROOT"

        if not mother_id in output:
            output[mother_id] = []

        output[mother_id].append(p)
        # easy peasy dict feeded
    return output

def get_projects_tree(content):
    ##
    ## Returns a dict with a list of string and {} elements with the
    ## projects hierarchy
    ##
    tree = json.loads(content)
    mother_son = _get_mother_son(tree)

    def walk(key):
        o = []
        if key in mother_son:
            for son in mother_son[key]:
                if son in mother_son:
                    o.append({son:walk(son)})
                else:
                    #son has no children
                    o.append(walk(son))
        else:
            return key
        return o

    output = {}
    # build the tree from the granmmas
    # {"ROOT":["rt","iot","birt"]}
    # {"rt":["a","b","c"]}
    output["ROOT"] = walk("ROOT")
    return output["ROOT"]


if __name__ == '__main__':
    args = parse_args()
    # check_args(args)

    with open(args.input_file) as fd:
        content = fd.read()

        with open(args.hierarchy_file,"w") as hfd:
            data = get_projects_tree(content)
            json.dump(data, hfd, sort_keys=True, indent=4)

        with open(args.projects_file,"w") as pfd:
            data = get_project_data(content)
            json.dump(data, pfd, sort_keys=True, indent=4)
