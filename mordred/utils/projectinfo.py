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


REPOSITORIES_AVALIABLE = ['source_repo', 'github', 'gmane', 'telegram', 'kuma',
                          'kitsune', 'mozilla_reps', 'mediawiki', 'mbox', 'confluence',
                          'jira',' maniphest', 'gerrit', 'meetup', 'supybot',
                          'pipermail']


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
    confluence: Adding a confluence for the project
    jira: Adding a jira for the project
    maniphest: Adding a maniphest for the project
    gerrit: Adding a gerrit for the project
    meetup: Adding a meetup for the project
    supybot: Adding a irc for the project, you need to includ the path
    gmane: Archive mailing lists hosted in gmane
    pipermail: Archive mailing list hosted with pipermail
    mbox: Adding a mbox for the project
    """

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=desc,
                                     usage=usage)

    parser.add_argument("file",
                        action="store",
                        help="the JSON file to update")

    subparsers = parser.add_subparsers(help='command',
                                       dest="command")

    # add
    add_usage = "projectinfo.py file add [--help] project_name [--repo <repo_type> <url>] [--parent <project>]"
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
                            help="see the list of available repositories and the repository URL")
    add_parser.add_argument("-p", "--parent",
                            action="store",
                            dest="parent",
                            help="add the parent project information")

    # remove
    rm_usage = "projectinfo.py file rm [--help] project_name [--repo <repo_type> <url>] [--parent <project>]"
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
                           help="see the list of available repositories")
    rm_parser.add_argument("-p", "--parent",
                           action="store",
                           dest="parent",
                           help="remove the parent project information")

    # list
    list_usage = "usage: projectinfo.py list <project_name> [--repo <repo_type>]"
    list_desc = "List data for a given project name"
    list_parser = subparsers.add_parser("list",
                                        help="list project",
                                        usage=list_usage,
                                        description=list_desc)
    list_parser.add_argument("project_name",
                             action="store",
                             nargs='?',
                             help="project name")
    list_parser.add_argument("--repo",
                             action="store",
                             dest="repo",
                             nargs='*',
                             help="see the list of available repositories")

    args = parser.parse_args()

    if args.repo:
        if args.repo[0] not in REPOSITORIES_AVALIABLE:
            sys.exit(str(args.repo[0]) + ' does not exist')

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
        #$ ./projectinfo.py <JSON_file> add <project_name> --repo <repo_name> <url>
        #Adding a source code repository for the project
        if conf['repo'] == "supybot":
            new_repo = {'url': conf['url'], 'path': conf['path']}
        else:
            new_repo = {'url': conf['url']}

        try:
            if new_repo not in raw['projects'][conf['project']][conf['repo']]:
                raw['projects'][conf['project']][conf['repo']].append(new_repo)
        except KeyError:
            try:
                raw['projects'][conf['project']][conf['repo']] = []
                raw['projects'][conf['project']][conf['repo']].append(new_repo)
            except KeyError:
                sys.exit("Project "+conf['project']+" does not exist, do first --add "+conf['project'])

    elif 'parent' in conf:
        #$ ./projectinfo.py <JSON_file> add <project_name> --parent <project>
        #We also want a method to update the parent_project information. 1 to n relationship.
        try:
            if conf['parent'] not in raw['projects'][conf['project']]['parent_project']:
                raw['projects'][conf['project']]['parent_project'].append(conf['parent'])
                print(conf['project']," is now subproject of ",conf['parent'])

                write_file(conf['file'], raw)
            else:
                sys.exit(conf['project']+" is already a subproject of "+conf['parent'])
        except KeyError:
            sys.exit(conf['project']+" or "+conf['parent']+" does not exist")

    else:
        #$ ./projectinfo.py <JSON_file> add <project_name>
        #Adding a project (it will create the file if did not exist):
        if conf['project'] not in raw['projects']:
            raw['projects'][conf['project']] = new_project
        else:
            sys.exit('The project "'+conf['project']+'" already exist')

    write_file(conf['file'], raw)

def remove(conf, raw):
    """Remove a project or a project repository"""

    if 'repo' in conf:
        # $ ./projectinfo.py <JSON_file> rm <project_name> --repo <repo_name> <url>
        #Removing the repo url for the project
        if conf['repo'] == "supybot":
            remove_repo = {'url': conf['url'], 'path': conf['path']}
        else:
            remove_repo = {'url': conf['url']}

        try:
            raw['projects'][conf['project']][conf['repo']].remove(remove_repo)
        except ValueError:
            sys.exit("URL "+conf['url']+" does not exist in "+conf['repo']+" OR the repository "+conf['repo']+" does not exist")

    elif 'parent' in conf:
        #$ ./projectinfo.py <JSON_file> rm <project_name> --parent <project>
        #remove the parent_project information.
        try:
            if conf['parent'] in raw['projects'][conf['project']]['parent_project']:
                raw['projects'][conf['project']]['parent_project'].remove(conf['parent'])
                print(conf['project']," is no longer a subproject of ",conf['parent'])

                write_file(conf['file'], raw)
            else:
                sys.exit(conf['project']+" is not a subproject of "+conf['parent'])
        except KeyError:
            sys.exit(conf['project']+" or "+conf['parent']+" does not exist")

    else:
        #$ ./projectinfo.py <JSON_file> rm <project_name>
        #Removing all the information about the project:
        try:
            del raw['projects'][conf['project']]
        except KeyError:
            sys.exit("Project "+conf['project'])

    write_file(conf['file'], raw)

def lists(conf, raw):
    """List a repository or list the URL of a repository"""

    if 'repo' in conf:
        #$ ./projectinfo.py <JSON_file> list <project_name> --repo <repo_name>
        #Listing the URL's for a repository
        repo = conf['repo']
        try:
            for project in raw['projects'][conf['project']][repo]:
                if repo == "supybot":
                    print(project['url'],project['path'])
                else:
                    print(project['url'])
        except KeyError:
            sys.exit("The repository "+repo+" does not exist in "+conf['project'])

    elif 'project' in conf:
        #$ ./projectinfo.py <JSON_file> list <project_name>
        #Mustn't list this list:
        default = ["bugzilla", "description", "dev_list", "downloads",
                  "forums", "gerrit_repo", "mailing_lists", "parent_project",
                  "releases", "title", "wiki_url"]
        try:
            for repo in raw['projects'][conf['project']].keys():
                if repo not in default:
                    if len(raw['projects'][conf['project']][repo]) > 0:
                        print(repo)
        except KeyError:
            sys.exit("Project "+conf['project']+" does not exist")

    else:
        for proj in raw['projects']:
            print(proj)

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

    if args.command != "list":
        if args.parent:
            conf['parent'] = args.parent

    if args.repo:
        conf['repo'] = args.repo[0]
        if args.repo[0] != "supybot" and len(args.repo) == 2:
            conf['url'] = args.repo[1]
        elif args.repo[0] == "supybot" and len(args.repo) == 3:
                conf['url'] = args.repo[1]
                conf['path'] = args.repo[2]
        elif args.command != "list":
            sys.exit("Must have two or three arguments if repo_name is 'supybot': --repo <repo_name> <url> <path>")
        elif len(args.repo) > 1 and args.command == "list":
            sys.exit("Must have one arguments if you use list: list <project_name> --repo <repo_name>")

    run(conf)
