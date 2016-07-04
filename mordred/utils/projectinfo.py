#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# This script manage the information about Eclipse projects
#
# Copyright (C) 2014 Bitergia
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
#   Quan Zhou <quan@bitergia.com>
#


import argparse
import json
import os
import sys


REPOSITORIES_AVALIABLE = ['source_repo', 'github', 'mailing_lists', 'telegram', 'kuma',
                          'kitsune', 'mozilla_reps', 'mediawiki', 'irc', 'confluence',
                          'jira',' maniphest', 'gerrit']

def read_arguments():
    desc = """
    Repositories avaliable

    source_repo: Adding a source repo for the project
    github: Adding a github repo for the project
    mailing_lists: Adding a mailing lists repo for the project
    telegram: Adding a telegram for the project
    kuma: Adding a kuma for the project
    kitsune: Adding a kitsune for the project
    mozilla_reps: Adding a mozilla reps for the project
    irc: Adding a irc for the project
    confluence: Adding a confluence for the project
    jira: Adding a jira for the project
    maniphest: Adding a maniphest for the project
    gerrit: Adding a gerrit for the project
    """

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=desc)

    # Required
    parser.add_argument("-f", "--file",
                      action="store",
                      dest="file",
                      help="the JSON file to update")

    # Actions
    parser.add_argument("-a", "--add",
                      action="store",
                      dest="add",
                      help="add a new project")
    parser.add_argument("-r", "--rm",
                      action="store",
                      dest="rm",
                      help="remove a project")
    parser.add_argument("-l", "--list",
                      action="store",
                      dest="list",
                      help="list the repository")

    # Options
    parser.add_argument("-p", "--parent",
                      action="store",
                      dest="parent",
                      help="update the parent_project information")
    parser.add_argument("--repo",
                      action="store",
                      dest="repo",
                      help="see list of repositories available")
    parser.add_argument("-u", "--url",
                      action="store",
                      dest="url",
                      help="the URL of the repo")

    args = parser.parse_args()

    if args.repo:
        if args.repo not in REPOSITORIES_AVALIABLE:
            raise KeyError(args.repo,' is not exist')

    return args

def read_file(filename):
    """Open the file if it exist and return in JSON format"""

    try:
        f = open(filename, 'r')
        read = json.loads(f.read())
        f.close()
    except:
        read = {'projects': {}}

    return read

def write_file(filename, data):
    with open(filename, 'w+') as f:
        json.dump(data, f)

def add(conf, raw):
    """Add a new project or a project repository"""

    new_project = {
                    "bugzilla": [],
                    "description": [
                        {
                            "format": "",
                            "safe_summary": "",
                            "safe_value": "",
                            "summary": "",
                            "value": ""
                        }
                    ],
                    "dev_list": {
                        "email": "foo@bar",
                        "name": "foobar",
                        "url": "https://foo.bar"
                    },
                    "downloads": [],
                    "forums": [],
                    "gerrit_repo": [
                        {}
                    ],
                    "mailing_lists": [],
                    "parent_project": [],
                    "releases": [],
                    "title": conf['project'],
                    "wiki_url": []
                }

    if 'repo' in conf:
        #$ ./projectinfo.py -f coreos.json --add Appc-spec --repo github --url https://github.com/appc/spec.git
        #Adding a source code repository for the project
        new_repo = {'url': conf['url']}

        try:
            if new_repo not in raw['projects'][conf['project']][conf['repo']]:
                raw['projects'][conf['project']][conf['repo']].append(new_repo)
        except KeyError:
            try:
                raw['projects'][conf['project']][conf['repo']] = []
                raw['projects'][conf['project']][conf['repo']].append(new_repo)
            except KeyError:
                print("No exist the project",conf['project'],"do first --add",conf['project'])
    else:
        #$ ./projectinfo.py -f coreos.json --add Appc-spec
        #Adding a project (it will create the file if did not exist):
        if conf['project'] not in raw['projects']:
            raw['projects'][conf['project']] = new_project
        else:
            print('The project', conf['project'], 'already exist')

    write_file(conf['file'], raw)

def remove(conf, raw):
    """Remove a project or a project repository"""

    if 'repo' in conf:
        # $ ./projectinfo.py -f coreos.json --rm Appc-spec --repo source_repo --url https://github.com/appc/spec.git
        #Removing the source repo https://github.com/appc/spec.git for the project Appc-spec
        remove_repo = {'url': conf['url']}
        try:
            raw['projects'][conf['project']][conf['repo']].remove(remove_repo)
        except ValueError:
            print("No exist the URL", conf['url'],"in", conf['repo'], "OR the repository", conf['repo'], "is not exist")
    else:
        #$ ./projectinfo.py -f coreos.json --rm Appc-spec
        #Removing all the information about the project:
        try:
            del raw['projects'][conf['project']]
        except KeyError:
            print("No exist the project",conf['project'])

    write_file(conf['file'], raw)

def lists(conf, raw):
    """List a repository or list the URL of a repository"""
    if 'repo' in conf:
        #$ ./projectinfo.py -f coreos.json --list Appc-spec --repo source_repo
        #Listing the source repos for a project
        repo = conf['repo']
        try:
            for project in raw['projects'][conf['project']][repo]:
                print(project)
        except KeyError:
            print("The repository", repo, "is not exist in", conf['project'])

    elif 'parent' in conf:
        #$ ./projectinfo.py --list Appc-spec --parent Specs
        #We also want a method to update the parent_project information. 1 to n relationship.
        try:
            raw['projects'][conf['project']]['parent_project'] = conf['parent']
            print(conf['project']," is now subproject of ",conf['parent'])

            write_file(conf['file'], raw)
        except KeyError:
            print(conf['project'],"or",conf['parent'],"is not exist")
    else:
        #$ ./projectinfo.py --list Appc-spec
        #List the information about the project:
        default = ["bugzilla", "description", "dev_list", "downloads",
                  "forums", "gerrit_repo", "mailing_lists", "parent_project",
                  "releases", "title", "wiki_url"]
        try:
            for repo in raw['projects'][conf['project']].keys():
                if repo not in default:
                    if len(raw['projects'][conf['project']][repo]) > 0:
                        print(repo)
        except KeyError:
            print("No exist the project",conf['project'])

def run(conf):

    f = read_file(conf['file'])
    if conf['option'] == "add":
        add(conf, f)
    elif conf['option'] == "rm":
        remove(conf, f)
    elif conf['option'] == "list":
        lists(conf, f)

if __name__ == '__main__':
    args = read_arguments()
    conf = {}

    if args.add:
        conf['option'] = "add"
        conf['project'] = args.add
    elif args.list:
        conf['option'] = "list"
        conf['project'] = args.list
    elif args.rm:
        conf['option'] = "rm"
        conf['project'] = args.rm

    if args.file:
        conf['file'] = args.file
    if args.parent:
        conf['parent'] = args.parent
    if args.repo:
        conf['repo'] = args.repo
    if args.url:
        conf['url'] = args.url

    run(conf)
