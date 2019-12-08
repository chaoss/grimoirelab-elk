#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import argparse
import json
import logging
import sys
from datetime import datetime
from os import path
from time import sleep

import MySQLdb
import requests
from dateutil import parser

from grimoire_elk.elastic import ElasticSearch
from grimoire_elk.errors import ElasticError
from grimoire_elk.utils import config_logging

GITHUB_URL = "https://github.com/"
GITHUB_API_URL = "https://api.github.com"
NREPOS = 0  # Default number of repos to be analyzed: all
CAULDRON_DASH_URL = "https://cauldron.io/dashboards"
GIT_CLONE_DIR = "/tmp"
OCEAN_INDEX = "ocean"
PERCEVAL_BACKEND = "git"
PROJECTS_DS = "scm"


def get_params_parser():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser()

    parser.add_argument("-e", "--elastic_url", default="http://127.0.0.1:9200",
                        help="Host with elastic search (default: http://127.0.0.1:9200)")
    parser.add_argument('-g', '--debug', dest='debug', action='store_true')
    parser.add_argument('-t', '--token', dest='token', help="GitHub token")
    parser.add_argument('-o', '--org', dest='org', nargs='*', help='GitHub Organization/s to be analyzed')
    parser.add_argument('-l', '--list', dest='list', action='store_true', help='Just list the repositories')
    parser.add_argument('-n', '--nrepos', dest='nrepos', type=int, default=NREPOS,
                        help='Number of GitHub repositories from the Organization to be analyzed (default:0, no limit)')
    parser.add_argument('--db-projects-map', help="Database to include the projects Mapping DB")

    return parser


def get_params():
    parser = get_params_parser()
    args = parser.parse_args()

    if not args.org or not args.token:
        parser.error("token and org params must be provided.")
        sys.exit(1)

    return args


def get_payload():
    # 100 max in repos
    payload = {'per_page': 100,
               'fork': False,
               'sort': 'updated',  # does not work in repos listing
               'direction': 'desc'}
    return payload


def get_headers(token):
    headers = {'Authorization': 'token ' + token}
    return headers


def get_owner_repos_url(owner, token):
    """ The owner could be a org or a user.
        It waits if need to have rate limit.
        Also it fixes a djando issue changing - with _
    """
    url_org = GITHUB_API_URL + "/orgs/" + owner + "/repos"
    url_user = GITHUB_API_URL + "/users/" + owner + "/repos"

    url_owner = url_org  # Use org by default

    try:
        r = requests.get(url_org,
                         params=get_payload(),
                         headers=get_headers(token))
        r.raise_for_status()

    except requests.exceptions.HTTPError as e:
        if r.status_code == 403:
            rate_limit_reset_ts = datetime.fromtimestamp(int(r.headers['X-RateLimit-Reset']))
            seconds_to_reset = (rate_limit_reset_ts - datetime.utcnow()).seconds + 1
            logging.info("GitHub rate limit exhausted. Waiting %i secs for rate limit reset." % (seconds_to_reset))
            sleep(seconds_to_reset)
        else:
            # owner is not an org, try with a user
            url_owner = url_user
    return url_owner


def get_repositores(owner_url, token, nrepos):
    """ owner could be an org or and user """
    all_repos = []

    url = owner_url

    while True:
        logging.debug("Getting repos from: %s" % (url))
        try:
            r = requests.get(url,
                             params=get_payload(),
                             headers=get_headers(token))

            r.raise_for_status()
            all_repos += r.json()

            logging.debug("Rate limit: %s" % (r.headers['X-RateLimit-Remaining']))

            if 'next' not in r.links:
                break

            url = r.links['next']['url']  # Loving requests :)
        except requests.exceptions.ConnectionError:
            logging.error("Can not connect to GitHub")
            break

    # Remove forks
    nrepos_recent = [repo for repo in all_repos if not repo['fork']]
    # Sort by updated_at and limit to nrepos
    nrepos_sorted = sorted(nrepos_recent, key=lambda repo: parser.parse(repo['updated_at']), reverse=True)
    if nrepos > 0:
        nrepos_sorted = nrepos_sorted[0:nrepos]
    # First the small repositories to feedback the user quickly
    nrepos_sorted = sorted(nrepos_sorted, key=lambda repo: repo['size'])
    for repo in nrepos_sorted:
        logging.debug("%s %i %s" % (repo['updated_at'], repo['size'], repo['name']))
    return nrepos_sorted


def create_projects_schema(cursor):
    project_table = """
        CREATE TABLE projects (
            project_id int(11) NOT NULL AUTO_INCREMENT,
            id varchar(255) NOT NULL,
            title varchar(255) NOT NULL,
            PRIMARY KEY (project_id)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8
    """
    project_repositories_table = """
        CREATE TABLE project_repositories (
            project_id int(11) NOT NULL,
            data_source varchar(32) NOT NULL,
            repository_name varchar(255) NOT NULL,
            UNIQUE (project_id, data_source, repository_name)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8
    """
    project_children_table = """
        CREATE TABLE project_children (
            project_id int(11) NOT NULL,
            subproject_id int(11) NOT NULL,
            UNIQUE (project_id, subproject_id)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8
    """

    # The data in tables is created automatically.
    # No worries about dropping tables.
    cursor.execute("DROP TABLE IF EXISTS projects")
    cursor.execute("DROP TABLE IF EXISTS project_repositories")
    cursor.execute("DROP TABLE IF EXISTS project_children")

    cursor.execute(project_table)
    cursor.execute(project_repositories_table)
    cursor.execute(project_children_table)


def insert_projects_mapping(db_projects_map, project, repositories):
    try:
        db = MySQLdb.connect(user="root", passwd="", host="mariadb",
                             db=db_projects_map)
    except Exception:
        # Try to create the database and the tables
        db = MySQLdb.connect(user="root", passwd="", host="mariadb")
        cursor = db.cursor()
        cursor.execute("CREATE DATABASE %s CHARACTER SET utf8" % (db_projects_map))
        db = MySQLdb.connect(user="root", passwd="", host="mariadb",
                             db=db_projects_map)
        cursor = db.cursor()
        create_projects_schema(cursor)

    cursor = db.cursor()

    # Insert the project in projects
    query = "INSERT INTO projects (id, title) VALUES (%s, %s)"
    q = "INSERT INTO projects (title, id) values (%s, %s)"
    cursor.execute(q, (project, project))
    project_id = db.insert_id()

    # Insert its repositories in project_repositories
    for repo in repositories:
        repo_url = repo['clone_url']
        q = "INSERT INTO project_repositories (project_id, data_source, repository_name) VALUES (%s, %s, %s)"
        cursor.execute(q, (project_id, PROJECTS_DS, repo_url))

    db.close()


if __name__ == '__main__':
    """GitHub to Kibana"""

    task_init = datetime.now()

    arthur_repos = {"repositories": []}

    args = get_params()

    config_logging(args.debug)

    total_repos = 0

    # enrich ocean
    index_enrich = OCEAN_INDEX + "_" + PERCEVAL_BACKEND + "_enrich"
    es_enrich = None
    try:
        es_enrich = ElasticSearch(args.elastic_url, index_enrich)
    except ElasticError:
        logging.error("Can't connect to Elastic Search. Is it running?")

    # The owner could be an org or an user.
    for org in args.org:
        owner_url = get_owner_repos_url(org, args.token)
        try:
            repos = get_repositores(owner_url, args.token, args.nrepos)
        except requests.exceptions.HTTPError:
            logging.error("Can't get repos for %s" % (owner_url))
            continue
        if args.db_projects_map:
            insert_projects_mapping(args.db_projects_map, org, repos)

        for repo in repos:
            repo_url = repo['clone_url']
            origin = repo_url
            clone_dir = path.join(GIT_CLONE_DIR, repo_url.replace("/", "_"))
            filter_ = {"name": "origin", "value": origin}
            last_update = None
            if es_enrich:
                last_update = es_enrich.get_last_date("metadata__updated_on", filter_)
            if last_update:
                last_update = last_update.isoformat()
            repo_args = {
                "gitpath": clone_dir,
                "uri": repo_url,
                "cache": False
            }
            if last_update:
                repo_args["from_date"] = last_update
            arthur_repos["repositories"].append({
                "args": repo_args,
                "backend": PERCEVAL_BACKEND,
                "origin": repo_url,
                "elastic_index": PERCEVAL_BACKEND
            })

        total_repos += len(repos)

    logging.debug("Total repos listed: %i" % (total_repos))

    print(json.dumps(arthur_repos, indent=4, sort_keys=True))
