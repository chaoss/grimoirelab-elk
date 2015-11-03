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
#     - Use the _bulk API from ES to improve indexing

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

def getGithubUser(login):

    if login is None:
        return None

    url = github_api + "/users/" + login

    r = requests.get(url, verify=False,
                     headers={'Authorization':'token ' + auth_token})
    user = r.json()

    users[login] = user

    # Get the public organizations also
    url += "/orgs"
    r = requests.get(url, verify=False,
                     headers={'Authorization':'token ' + auth_token})
    orgs = r.json()

    users[login]['orgs'] = orgs

    return user


def getUserEmail(login):
    email = None

    if login not in users:
        user = getGithubUser(login)
        if 'email' in user:
            email = user['email']

    return email


def getUserOrg(login):
    company = None

    if login not in users:
        user = getGithubUser(login)
        if 'company' in user:
            company = user['company']

    if company is None:
        company = ''
        # Return the list of orgs
        for org in users[login]['orgs']:
            company += org['login'] +";;"
        company = company[:-2]

    return company

def getUserName(login):
    name = None

    if login not in users:
        user = getGithubUser(login)
        if 'name' in user:
            name = user['name']

    return name


def getRichPull(pull):
    rich_pull = {}
    rich_pull['id'] = pull['id']
    rich_pull['time_to_close_days'] = getTimeToCloseDays(pull)

    rich_pull['user_login'] = pull['user']['login']
    rich_pull['user_name'] = getUserName(rich_pull['user_login'])
    rich_pull['user_email'] = getUserEmail(rich_pull['user_login'])
    rich_pull['user_org'] = getUserOrg(rich_pull['user_login'])
    if pull['assignee'] is not None:
        rich_pull['assignee_login'] = pull['assignee']['login']
        rich_pull['assignee_name'] = getUserName(rich_pull['assignee_login'])
        rich_pull['assignee_email'] = getUserEmail(rich_pull['assignee_login'])
        rich_pull['assignee_org'] = getUserOrg(rich_pull['assignee_login'])
    else:
        rich_pull['assignee_name'] = None
        rich_pull['assignee_login'] = None
        rich_pull['assignee_email'] = None
        rich_pull['assignee_org'] = None
    rich_pull['title'] = pull['title']
    rich_pull['state'] = pull['state']
    rich_pull['created_at'] = pull['created_at']
    rich_pull['updated_at'] = pull['updated_at']
    rich_pull['closed_at'] = pull['closed_at']
    rich_pull['url'] = pull['html_url']
    labels = ''
    if 'labels' in pull:
        for label in pull['labels']:
            labels += label['name']+";;"
    if labels != '':
        labels[:-2]
    rich_pull['labels'] = labels

    return rich_pull


def usersToES(pulls):

    elasticsearch_type = "users"  # github global users

    for login in users:

        # First upload the raw pullrequest data to ES
        data_json = json.dumps(users[login])
        url = elasticsearch_url + "/"+elasticsearch_index_github
        url += "/"+elasticsearch_type
        url += "/"+str(users[login]["id"])
        requests.put(url, data = data_json)

def usersFromES():

    users_es = {}
    res_size = 100  # best size?
    _from = 0

    elasticsearch_type = "users"

    url = elasticsearch_url + "/"+elasticsearch_index_github
    url += "/"+elasticsearch_type
    url += "/_search" + "?" + "size=%i" % res_size
    print url
    r = requests.get(url)
    users_raw = r.json()

    if 'hits' not in users_raw:
        logging.info("No github user data in ES")
        return

    while len(users_raw['hits']['hits']) > 0:
        for hit in users_raw['hits']['hits']:
            user = hit['_source']
            users_es[user['login']] = user
        _from += res_size
        r = requests.get(url+"&from=%i" % _from)
        users_raw = r.json()


    return users_es

def getLastUpdateFromES(_type):

    last_update = None

    url = elasticsearch_url + "/" + elasticsearch_index_raw
    url += "/"+ _type +  "/_search"

    data_json = """
    {
        "aggs": {
            "1": {
              "max": {
                "field": "updated_at"
              }
            }
        }
    }
    """

    res = requests.post(url, data = data_json)
    res_json = res.json()

    if 'aggregations' in res_json:
        last_update = res_json["aggregations"]["1"]["value_as_string"]

    return last_update


def pullrequets2ES(pulls, _type):

    elasticsearch_type = _type
    count = 0

    for pull in pulls:

        if not 'head' in pull.keys() and not 'pull_request' in pull.keys():
            # And issue that it is not a PR
            continue

        # First upload the raw pullrequest data to ES
        data_json = json.dumps(pull)
        url = elasticsearch_url + "/"+elasticsearch_index_raw
        url += "/"+elasticsearch_type
        url += "/"+str(pull["id"])
        requests.put(url, data = data_json)

        # The processed pull including user data and time_to_close
        rich_pull = getRichPull(pull)
        data_json = json.dumps(rich_pull)
        url = elasticsearch_url + "/"+elasticsearch_index
        url += "/"+elasticsearch_type
        url += "/"+str(rich_pull["id"])
        requests.put(url, data = data_json)

        count += 1

    return count

def getPullRequests(url):
    url_next = url
    prs_count = 0
    last_page = None
    page = 1

    url_next += "&page="+str(page)

    while url_next:
        logging.info("Get issues pulls requests from " + url_next)
        r = requests.get(url_next, verify=False,
                         headers={'Authorization':'token ' + auth_token})
        pulls = r.json()
        prs_count += pullrequets2ES(pulls, "pullrequests")

        logging.info(r.headers['X-RateLimit-Remaining'])

        url_next = None
        if 'next' in r.links:
            url_next = r.links['next']['url']  # Loving requests :)

        if not last_page:
            last_page = r.links['last']['url'].split('&page=')[1].split('&')[0]

        logging.info("Page: %i/%s" % (page, last_page))

        page += 1

    return prs_count

def getIssuesPullRequests(url):
    _type = "issues_pullrequests"
    prs_count = 0
    last_page = page = 1
    last_update = getLastUpdateFromES(_type)
    if last_update is not None:
        logging.info("Getting issues since: " + last_update)
        url += "&since="+last_update
    url_next = url

    while url_next:
        logging.info("Get issues pulls requests from " + url_next)
        r = requests.get(url_next, verify=False,
                         headers={'Authorization':'token ' + auth_token})
        pulls = r.json()

        prs_count += pullrequets2ES(pulls, _type)

        logging.info(r.headers['X-RateLimit-Remaining'])

        url_next = None
        if 'next' in r.links:
            url_next = r.links['next']['url']  # Loving requests :)

        if last_page == 1:
            if 'last' in r.links:
                last_page = r.links['last']['url'].split('&page=')[1].split('&')[0]

        logging.info("Page: %i/%s" % (page, last_page))

        page += 1

    return prs_count


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)

    github_owner = "elastic"
    # github_repo = "logstash"
    # github_repo = "kibana"
    github_repo = "filebeat"


    elasticsearch_url = "http://localhost:9200"
    elasticsearch_index_github = "github"
    elasticsearch_index = elasticsearch_index_github + \
        "_%s_%s" % (github_owner, github_repo)
    elasticsearch_index_raw = elasticsearch_index+"_raw"

    users = usersFromES()  #  cache from ES

    # We just need to add new pullrequests

    github_per_page = 20  # 100 in other items. 20 for pull requests
    github_api = "https://api.github.com"
    github_api_repos = github_api + "/repos"
    url_repo = github_api_repos + "/" + github_owner +"/" + github_repo

    url_pulls = url_repo + "/pulls"
    url_issues = url_repo + "/issues"

    url_params = "?per_page=" + str(github_per_page)
    url_params += "&state=all"  # open and close pull requests
    url_params += "&sort=updated"  # sort by last updated
    url_params += "&direction=asc"  # first older pull request

    auth_token = ""

    # prs_count = getPullRequests(url_pulls+url_params)
    issues_prs_count = getIssuesPullRequests(url_issues+url_params)


    # cache users in ES
    usersToES(users)

    # logging.info("Total Pull Requests " + str(prs_count))
    logging.info("Total Issues Pull Requests " + str(issues_prs_count))