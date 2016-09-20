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
import os
import requests
import sys


GITHUB_API_URL = "https://api.github.com/"


def read_arguments():
    #github_update_projectinfo.py file myProjectName -b blacklist.txt https://github.com/grimoirelab
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("file",
                        action="store",
                        help="the JSON file to update")
    parser.add_argument("myProjectName",
                        action="store",
                        help="the name you want to save")
    parser.add_argument("-b",
                        action="store",
                        dest="blacklist",
                        help="blacklist")
    parser.add_argument("url",
                        action="store",
                        help="github url")

    args = parser.parse_args()

    return args

def read_file(filename):

    try:
        lines = [line.rstrip('\n') for line in open(filename)]
    except:
        sys.exit("Blacklist file doesn't exist")

    return lines

def get_repo_url(user, blacklist):
    repos = []
    github = []

    url = GITHUB_API_URL+"users/"+user+"/repos"
    page = 0
    last_page = 0

    r = requests.get(url+"?page="+str(page))
    r.raise_for_status()
    for repo in r.json():
        if repo['html_url'] not in blacklist and not repo['fork']:
            repos.append(repo['html_url'])
            github.append(repo['clone_url'])

    if 'last' in r.links:
        last_url = r.links['last']['url']
        last_page = last_url.split('page=')[1]
        last_page = int(last_page)

    while(page < last_page):
        page += 1
        r = requests.get(url+"?page="+str(page))
        r.raise_for_status()
        for repo in r.json():
            if repo['html_url'] not in blacklist and not repo['fork']:
                repos.append(repo['html_url'])
                github.append(repo['clone_url'])

    return repos, github

def project_info(json, proj_name, urls, name):
    for url in urls:
        status = os.system('python3 projectinfo.py '+json+' add '+proj_name+' --repo '+name+' '+url+' 2>/dev/null')
        if status != 0:
            os.system('python3 projectinfo.py '+json+' add '+proj_name)
            os.system('python3 projectinfo.py '+json+' add '+proj_name+' --repo '+name+' '+url)

if __name__ == '__main__':
    args = read_arguments()

    user = args.url.split('https://github.com/')[1]
    user = user.split('/')[0]
    json = args.file
    proj_name = args.myProjectName

    blacklist = []
    if args.blacklist:
        blacklist = read_file(args.blacklist)

    repos, github = get_repo_url(user, blacklist)

    project_info(json, proj_name, repos, "source_repo")
    project_info(json, proj_name, github, "github")
