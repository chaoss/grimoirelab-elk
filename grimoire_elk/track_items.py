# -*- coding: utf-8 -*-
#
# Copyright (C) 2017 Bitergia
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
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import json
import logging
import os
import tempfile

import requests

from grimoire_elk.elk import load_identities
from grimoire_elk.enriched.gerrit import GerritEnrich
from grimoire_elk.enriched.git import GitEnrich
from grimoire_elk.enriched.utils import grimoire_con

logger = logging.getLogger(__name__)

requests_ses = grimoire_con()


def fetch_track_items(upstream_file_url, data_source):
    """ The file format is:

    # Upstream contributions, bitergia will crawl this and extract the relevant information
    # system is one of Gerrit, Bugzilla, Launchpad (insert more)
    ---
    -
      url: https://review.openstack.org/169836
      system: Gerrit
    """

    track_uris = []
    req = requests_ses.get(upstream_file_url)
    try:
        req.raise_for_status()
    except requests.exceptions.HTTPError as ex:
        logger.warning("Can't get gerrit reviews from %s", upstream_file_url)
        logger.warning(ex)
        return track_uris

    logger.debug("Found reviews to be tracked in %s", upstream_file_url)

    lines = iter(req.text.split("\n"))
    for line in lines:
        if 'url: ' in line:
            dso = next(lines).split('system: ')[1].strip('\n')
            if dso == data_source:
                track_uris.append(line.split('url: ')[1].strip('\n'))
    return track_uris


def get_gerrit_numbers(gerrit_uris):
    # uuid to search the gerrit review in ElasticSearch
    numbers = []
    for gerrit_uri in gerrit_uris:
        gerrit_number = _get_gerrit_number(gerrit_uri)
        numbers.append(gerrit_number)

    return numbers


def enrich_gerrit_items(es, index_gerrit_raw, gerrit_numbers, project, db_config):
    reviews = _get_gerrit_reviews(es, index_gerrit_raw, gerrit_numbers)
    projects_file_path = _create_projects_file(project, "gerrit", reviews)
    logger.info("Total gerrit track items to be enriched: %i", len(reviews))

    enriched_items = []
    enricher = GerritEnrich(db_sortinghat=db_config['database'],
                            db_user=db_config['user'],
                            db_password=db_config['password'],
                            db_host=db_config['host'],
                            json_projects_map=projects_file_path)

    # First load identities
    load_identities(reviews, enricher)

    # Then enrich
    for review in reviews:
        enriched_items.append(enricher.get_rich_item(review))

    os.unlink(projects_file_path)

    return enriched_items


def get_commits_from_gerrit(es, index_gerrit_raw, gerrit_numbers):
    # Get the gerrit reviews from ES and extract the commits sha
    commits_sha = []

    reviews = _get_gerrit_reviews(es, index_gerrit_raw, gerrit_numbers)
    logger.info("Total gerrit track items found upstream: %i", len(reviews))

    for review in reviews:
        for patch in review['data']['patchSets']:
            if patch['revision'] not in commits_sha:
                commits_sha.append(patch['revision'])

    return commits_sha


def enrich_git_items(es, index_git_raw, commits_sha_list, project, db_config):
    commits = _get_git_commits(es, index_git_raw, commits_sha_list)
    projects_file_path = _create_projects_file(project, "git", commits)
    logger.info("Total git track items to be enriched: %i", len(commits))

    enriched_items = []
    enricher = GitEnrich(db_sortinghat=db_config['database'],
                         db_user=db_config['user'],
                         db_password=db_config['password'],
                         db_host=db_config['host'],
                         json_projects_map=projects_file_path)

    # First load identities
    load_identities(commits, enricher)

    # Then enrich
    for commit in commits:
        enriched_items.append(enricher.get_rich_item(commit))

    os.unlink(projects_file_path)

    return enriched_items


# PRIVATE METHODS

def _get_gerrit_number(gerrit_uri):
    # Get the uuid for this item_uri. Possible formats
    # https://review.openstack.org/424868/
    # https://review.openstack.org/#/c/430428
    # https://review.openstack.org/314915

    if gerrit_uri[-1] == "/":
        gerrit_uri = gerrit_uri[:-1]

    number = gerrit_uri.rsplit("/", 1)[1]

    return number


def _get_gerrit_origin(gerrit_uri):
    # Get the uuid for this item_uri. Possible formats
    # https://review.openstack.org/424868/
    # https://review.openstack.org/#/c/430428
    # https://review.openstack.org/314915
    # https://review.openstack.org/314915 redirects to https://review.openstack.org/#/c/314915

    if gerrit_uri[-1] == "/":
        gerrit_uri = gerrit_uri[:-1]

    gerrit_uri = gerrit_uri.replace('#/c/', '')
    origin = gerrit_uri.rsplit("/", 1)[0]

    return origin


def _get_gerrit_reviews(es, index_gerrit_raw, gerrit_numbers):
    # Get gerrit raw items
    query = {
        "query": {
            "terms": {"data.number": gerrit_numbers}
        }
    }

    req = requests_ses.post(es + "/" + index_gerrit_raw + "/_search?size=10000",
                            data=json.dumps(query))
    req.raise_for_status()
    reviews_es = req.json()["hits"]["hits"]
    reviews = []
    for review in reviews_es:
        reviews.append(review['_source'])
    return reviews


def _create_projects_file(project_name, data_source, items):
    """ Create a projects file from the items origin data """

    repositories = []
    for item in items:
        if item['origin'] not in repositories:
            repositories.append(item['origin'])
    projects = {
        project_name: {
            data_source: repositories
        }
    }

    projects_file, projects_file_path = tempfile.mkstemp(prefix='track_items_')

    with open(projects_file_path, "w") as pfile:
        json.dump(projects, pfile, indent=True)

    return projects_file_path


def _get_git_commits(es, index_git_raw, commits_sha_list):
    # Get gerrit raw items
    query = {
        "query": {
            "terms": {"data.commit": commits_sha_list}
        }
    }

    req = requests_ses.post(es + "/" + index_git_raw + "/_search?size=10000",
                            data=json.dumps(query))
    req.raise_for_status()
    commits_es = req.json()["hits"]["hits"]
    commits = []
    commits_sha_list_found = []
    for commit in commits_es:
        commits.append(commit['_source'])
        commits_sha_list_found.append(commit['_source']['data']['commit'])
    # It is normal that patchSets commits are not upstream once review is merged
    # commits_not_found = set(commits_sha_list) - set(commits_sha_list_found)
    # logger.debug("Review commits not found upstream %i: %s",
    #             len(commits_not_found), commits_not_found)
    return commits
