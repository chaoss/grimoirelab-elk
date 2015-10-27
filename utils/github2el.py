#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Github Pull Requests loader for Elastic Search
#
# Copyright (C) 2015 Bitergia
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
# TODO: Just a playing script yet.

from datetime import datetime
import json
import logging
import requests


def getTimeToCloseDays(pull):
    review_time = None

    if pull['closed_at']is None or pull['created_at'] is None:
        return review_time

    # closed_at - created_at
    closed_at = \
        datetime.strptime(pull['closed_at'], "%Y-%m-%dT%H:%M:%SZ")
    created_at = \
        datetime.strptime(pull['created_at'], "%Y-%m-%dT%H:%M:%SZ")

    seconds_day = float(60*60*24)
    review_time = \
        (closed_at-created_at).total_seconds() / seconds_day
    review_time = float('%.2f' % review_time)

    return review_time



def pullrequets2es(pulls):
    elasticsearch_url = "http://localhost:9200"
    elasticsearch_index = "github"
    elasticsearch_type = "pullrequests"

    for pull in pulls:

        pull['time_to_close_days'] =  getTimeToCloseDays(pull)

        data_json = json.dumps(pull)
        url = elasticsearch_url + "/"+elasticsearch_index
        url += "/"+elasticsearch_type
        url += "/"+str(pull["id"])
        requests.put(url, data = data_json)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    github_per_page = 20  # 100 in other items. 20 for pull requests
    page = 1
    last_page = None
    github_api = "https://api.github.com"
    github_api_repos = github_api + "/repos"
    owner = "elastic"
    repo = "kibana"
    url_repo = github_api_repos + "/" + owner +"/" + repo
    url_pulls = url_repo + "/pulls"
    url = url_pulls +"?per_page=" + str(github_per_page)
    url += "&page="+str(page)
    url += "&state=all"  # open and close pull requests
    url += "&sort=updated"  # sort by last updated
    url += "&direction=asc"  # first older pull request
    auth_token = "GITHUB_AUTH_TOKEN_HERE"

    url_next = url
    prs_count = 0

    while url_next:
        logging.info("Get pulls requests from " + url_next)
        r = requests.get(url_next, verify=False,
                         headers={'Authorization':'token ' + auth_token})
        pulls = r.json()
        pullrequets2es(pulls)

        logging.info(r.headers['X-RateLimit-Remaining'])

        url_next = None
        if 'next' in r.links:
            url_next = r.links['next']['url']  # Loving requests :)

        if not last_page:
            last_page = r.links['last']['url'].split('&page=')[1].split('&')[0]

        logging.info("Page: %i/%s" % (page, last_page))

        pullrequets2es(pulls)

        for pull in pulls:
            prs_count += 1

        page += 1

    logging.info("Total Pull Requests " + str(prs_count))