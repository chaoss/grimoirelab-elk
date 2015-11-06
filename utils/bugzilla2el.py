#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Bugzilla tickets for Elastic Search
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

import argparse
import json
import logging
import requests
from dateutil import parser
import urlparse
# import xml.sax.handler
from xml.etree import ElementTree


def parse_args ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user",
                        help = "Bugzilla user")
    parser.add_argument("--password",
                        help = "Bugzilla user password")
    parser.add_argument("-d", "--delay", default="1",
                        help = "delay between requests in seconds (1s default)")
    parser.add_argument("-u", "--url", required = True,
                        help = "Bugzilla url")
    parser.add_argument("-e", "--elasticsearch_host",  default = "127.0.0.1",
                        help = "Host with elasticsearch" + \
                        "(default: 127.0.0.1)")
    parser.add_argument("--elasticsearch_port",  default = "9200",
                        help = "elasticsearch port " + \
                        "(default: 9200)")
    parser.add_argument("--delete",  action = 'store_true',
                        help = "delete repository data in ES")


    args = parser.parse_args()
    return args

def getBugzillaVersion():

    global bugzilla_version

    def get_domain(url):
        result = urlparse.urlparse(url)

        if url.find("show_bug.cgi") > 0:
            pos = result.path.find('show_bug.cgi')
        elif url.find("buglist.cgi") > 0:
            pos = result.path.find('buglist.cgi')

        newpath = result.path[0:pos]
        domain = urlparse.urljoin(result.scheme + '://' + result.netloc + '/',
                                  newpath)
        return domain

    if bugzilla_version:
        return bugzilla_version

    info_url = get_domain(args.url) + "show_bug.cgi?id=&ctype=xml"

    r = requests.get(info_url)

    tree = ElementTree.fromstring(r.content)

    bugzilla_version = tree.attrib['version']


def getTimeToLastUpdateDays(created_at_txt, updated_at_txt):
    """ Number of days between creation and last update """

    # updated_at - created_at
    updated_at = parser.parse(updated_at_txt)
    created_at = parser.parse(created_at_txt)

    seconds_day = float(60*60*24)
    update_time = \
        (updated_at-created_at).total_seconds() / seconds_day
    update_time = float('%.2f' % update_time)

    return update_time


def getLastUpdateFromES(_type):

    return None

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
        if "value_as_string" in res_json["aggregations"]["1"]:
            last_update = res_json["aggregations"]["1"]["value_as_string"]

    return last_update


def initES():

    _types = ['issues']

    # Remove and create indexes. Create mappings.
    url_raw = elasticsearch_url + "/"+elasticsearch_index_raw
    url = elasticsearch_url + "/"+elasticsearch_index

    requests.delete(url_raw)
    requests.delete(url)

    requests.post(url_raw)
    requests.post(url)


def issuesXML2ES(issues_xml):
    """ Store in ES the XML for each issue """

    # TODO: Use _bulk API

    elasticsearch_type = "issues_raw"

    for bug in issues_xml:

        _id = bug.findall('bug_id')[0].text
        # TODO.: detect XML enconding and use it
        xml = {"xml":ElementTree.tostring(bug, encoding="us-ascii")}
        data_json = json.dumps(xml)

        url = elasticsearch_url + "/"+elasticsearch_index
        url += "/"+elasticsearch_type
        url += "/"+str(_id)
        requests.put(url, data = data_json)



def issues2ES(issues):

    # TODO: use bulk API

    elasticsearch_type = "issues"

    for issue in issues:
        data_json = json.dumps(issue)
        url = elasticsearch_url + "/"+elasticsearch_index
        url += "/"+elasticsearch_type
        url += "/"+str(issue["id"])
        requests.put(url, data = data_json)

def getDomain(url):
    result = urlparse.urlparse(url)

    if url.find("show_bug.cgi") > 0:
        pos = result.path.find('show_bug.cgi')
    elif url.find("buglist.cgi") > 0:
        pos = result.path.find('buglist.cgi')

    newpath = result.path[0:pos]
    domain = urlparse.urljoin(result.scheme + '://' + result.netloc + '/',
                              newpath)
    return domain


def getIssues(url):

    def fix_review_dates(issue):
        """ Convert dates so ES detect them """

        for date_field in ['created_on','updated_on']:
            if date_field in issue.keys():
                date_ts = parser.parse(issue[date_field])
                issue[date_field] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')


    def get_issues_list_url(base_url, version):
        if '?' in base_url:
            url = base_url + '&'
        else:
            url = base_url + '?'

        if ((version == "3.2.3") or (version == "3.2.2")):
            url = url + "order=Last+Changed&ctype=csv"
        else:
            url = url + "order=changeddate&ctype=csv"
        return url


    def retrieve_issues_ids(url):
        logging.info("Getting issues list ...")

        url = get_issues_list_url(url, bugzilla_version)

        r = requests.get(url)

        csv = r.content.split('\n')[1:]
        ids = []
        for line in csv:
            # 0: bug_id, 7: changeddate
            values = line.split(',')
            issue_id = values[0]
            # change_ts = values[7].strip('"')

            ids.append(issue_id)

        return ids

    def get_issues_info_url(base_url, ids):
        url = base_url + "show_bug.cgi?"

        for issue_id in ids:
            url += "id=" + issue_id + "&"

        url += "ctype=xml"
        url += "&excludefield=attachmentdata"
        return url

    def addAttributes(issue, field, tag):
        """ Specific logic for using data in XML attributes """

        if field.tag == "reporter" or field.tag == "assigned_to":
            if 'name' in field.attrib:
                issue[tag + "_name"] = field.attrib['name'] 

    def getIssueProccesed(bug_xml_tree):
        """ Return a dict with selected fields """

        issue_processed = {}

        fields = ['reporter', 'assigned_to', 'status', 'resolution']
        fields += ['creation_ts', 'delta_ts', 'product', 'component']
        fields += ['bug_id','short_desc','priority']
        fields += ['version']

        fields_rename = {"delta_ts":"updated_on",
                         "creation_ts":"created_on",
                         "bug_id":"id",
                         "reporter":"submitted_by"}

        # Extra fields: enriched issue
        issue_processed['number_of_comments'] = 0
        issue_processed['time_to_last_update_days'] = None
        issue_processed['url'] = None

        for field in bug_xml_tree:
            if field.tag in fields:
                tag = field.tag
                if tag in fields_rename:
                    tag = fields_rename[tag]
                issue_processed[tag] = field.text

                addAttributes(issue_processed, field, tag)

            if field.tag == "long_desc":
                issue_processed['number_of_comments'] += 1

        issue_processed['time_to_last_update_days'] = \
            getTimeToLastUpdateDays(issue_processed['created_on'],
                                    issue_processed['updated_on'])
        issue_processed['url'] = getDomain(url) + "show_bug.cgi?id=" + \
            issue_processed['id']

        fix_review_dates(issue_processed)

        return issue_processed


    def retrieve_issues(ids, base_url):

        issues_processed = []  # Issues JSON ready to inserted in ES

        # We want to use pop() to get the oldest first so we must reverse the
        # order
        ids.reverse()
        while(ids):
            query_issues = []
            issues = []
            while (len(query_issues) < issues_per_query and ids):
                query_issues.append(ids.pop())

            # Retrieving main bug information
            url = get_issues_info_url(base_url, query_issues)
            logging.info("Getting %i issues data" % (issues_per_query))
            issues_raw = requests.get(url)
            logging.info("Processing issues data")

            tree = ElementTree.fromstring(issues_raw.content)

            issuesXML2ES(tree)


            for bug in tree:
                issues.append(getIssueProccesed(bug))

            issues2ES(issues)

            issues_processed += issues

        return issues_processed



    _type = "issues"

    logging.info("Getting issues from Bugzilla")

    # TODO: not all issues processed. Need to iterate.
    ids = retrieve_issues_ids(url)

    logging.info("Total issues to be gathered %i" % len(ids))

    base_url = getDomain(url)
    issues_processed = retrieve_issues(ids, base_url)

    logging.info("Total issues gathered %i" % len(issues_processed))

def getBugzillaIndex(url):
    """ Return bugzilla ES index name from url """

    _index = getDomain(url)[:-1].split('://')[1]

    if 'product' in url:
        _index += "-" + url.split('product=')[1]

    return _index.lower()  # ES index names must be lower case

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)

    args = parse_args()

    bugzilla_version = None
    getBugzillaVersion()

    users = {} 

    elasticsearch_url = "http://"
    elasticsearch_url += args.elasticsearch_host + ":" + args.elasticsearch_port
    elasticsearch_index_bugzilla = "bugzilla"
    elasticsearch_index = elasticsearch_index_bugzilla + \
        "_%s" % (getBugzillaIndex(args.url))
    elasticsearch_index_raw = elasticsearch_index+"_raw"

    initES()  # until we have incremental support, always from scratch
    # users = usersFromES()
    # geolocations = geoLocationsFromES()

    issues_per_query = 200  # number of tickets per query

    # prs_count = getPullRequests(url_pulls+url_params)
    issues_prs_count = getIssues(args.url)

    # usersToES(users)  # cache users in ES
    # geoLocationsToES(geolocations)

    # logging.info("Total Pull Requests " + str(prs_count))
    logging.info("Total Issues Pull Requests " + str(issues_prs_count))