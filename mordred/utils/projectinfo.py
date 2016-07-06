#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# This script manage the information about Mordred projects
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
#   Quan Zhou <quan@bitergia.com>
#


import argparse
import json
import os
import sys


REPOSITORIES_AVALIABLE = ['source_repo', 'github', 'mailing_lists', 'telegram', 'kuma',
                          'kitsune', 'mozilla_reps', 'mediawiki', 'irc', 'confluence',
                          'jira',' maniphest', 'gerrit', 'meetup']


def add_subparser(subparsers):
    """Add subparser"""

    add_usage = "projectinfo.py file add [--help] project_name [--repo <repo_type> <url>]"
    add_parser = subparsers.add_parser("add",
                                       help="add project",
                                       usage=add_usage)
    add_parser.add_argument("project_name",
                            action="store",
                            help="project name")
    add_parser.add_argument("--repo",
                            action="store",
                            dest="repo",
                            nargs='*',
                            help="see list of repositories available, url of the repository")

def rm_subparser(subparsers):
    """Remove subparser"""

    rm_usage = "projectinfo.py file rm [--help] project_name [--repo <repo_type> <url>]"
    rm_parser = subparsers.add_parser("rm",
                                      help="remove project",
                                      usage=rm_usage)
    rm_parser.add_argument("project_name",
                           action="store",
                           help="project name")
    rm_parser.add_argument("--repo",
                           action="store",
                           dest="repo",
                           nargs='*',
                           help="see list of repositories available")

def list_subparser(subparsers):
    """List subparser"""

    list_usage = "projectinfo.py file list [--help] project_name"
    list_parser = subparsers.add_parser("list",
                                        help="add project",
                                        usage=list_usage)
    list_parser.add_argument("project_name",
                             action="store",
                             help="project name")
    list_parser.add_argument("-p",
                             action="store",
                             dest="parent",
                             help="update the parent project information")

def read_arguments():
    usage = "projectinfo.py [--help] file command project_name [--repo <repo_type> <url>] [--parent <project_name>]"
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
    meetup: Adding a meetup for the project
    """

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=desc,
                                     usage=usage)

    parser.add_argument("file",
                        action="store",
                        help="the JSON file to update")

    subparsers = parser.add_subparsers(help='command',
                                       dest="command")
    add_subparser(subparsers)
    rm_subparser(subparsers)
    list_subparser(subparsers)

    args = parser.parse_args()

    if args.command != "list" and args.repo:
        if args.repo[0] not in REPOSITORIES_AVALIABLE:
            sys.exit(str(args.repo[0]) + ' is not exist')

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
                    "source_repo": [],
                    "title": conf['project'],
                    "wiki_url": []
                }

    if 'repo' in conf:
        #$ ./projectinfo.py <JSON_file> add <projec_name> --repo <repo_name> <url>
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
        #$ ./projectinfo.py <JSON_file> add <project_name>
        #Adding a project (it will create the file if did not exist):
        if conf['project'] not in raw['projects']:
            raw['projects'][conf['project']] = new_project
        else:
            print('The project', conf['project'], 'already exist')

    write_file(conf['file'], raw)

def remove(conf, raw):
    """Remove a project or a project repository"""

    if 'repo' in conf:
        # $ ./projectinfo.py <JSON_file> rm <project_name> --repo <repo_name> <url>
        #Removing the repo url for the project
        remove_repo = {'url': conf['url']}
        try:
            raw['projects'][conf['project']][conf['repo']].remove(remove_repo)
        except ValueError:
            print("No exist the URL", conf['url'],"in", conf['repo'], "OR the repository", conf['repo'], "is not exist")
    else:
        #$ ./projectinfo.py <JSON_file> rm <project_name>
        #Removing all the information about the project:
        try:
            del raw['projects'][conf['project']]
        except KeyError:
            print("No exist the project",conf['project'])

    write_file(conf['file'], raw)

def lists(conf, raw):
    """List a repository or list the URL of a repository"""

    if 'parent' in conf:
        #$ ./projectinfo.py <JSON_file> list <projec_name> --parent <project>
        #We also want a method to update the parent_project information. 1 to n relationship.
        try:
            raw['projects'][conf['project']]['parent_project'] = conf['parent']
            print(conf['project']," is now subproject of ",conf['parent'])

            write_file(conf['file'], raw)
        except KeyError:
            print(conf['project'],"or",conf['parent'],"is not exist")
    else:
        #$ ./projectinfo.py <JSON_file> list <project_name>
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

    if args.command:
        conf['option'] = args.command
    if args.project_name:
        conf['project'] = args.project_name
    if args.file:
        conf['file'] = args.file

    if args.command == "list":
        if args.parent:
            conf['parent'] = args.parent
    else:
        if args.repo:
            if len(args.repo) == 2:
                conf['repo'] = args.repo[0]
                conf['url'] = args.repo[1]
            else:
                sys.exit("Must have two arguments: --repo <repo_name> <url>")

    run(conf)
