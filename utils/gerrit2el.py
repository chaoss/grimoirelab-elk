#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Gerrit reviews loader for Elastic Search
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
#

import json
import logging
import os
import subprocess
import time
import urllib2


def fix_review_dates(item):
    # Convert dates so ES detect them
    for date_field in ['timestamp','createdOn','lastUpdated']:
        if date_field in item.keys():
            date_ts = item[date_field]
            item[date_field] = time.strftime('%Y-%m-%dT%H:%M:%S',
                                              time.localtime(date_ts))
    if 'patchSets' in item.keys():
        for patch in item['patchSets']:
            pdate_ts = patch['createdOn']
            patch['createdOn'] = time.strftime('%Y-%m-%dT%H:%M:%S',
                                               time.localtime(pdate_ts))
            if 'approvals' in patch:
                for approval in patch['approvals']:
                    adate_ts = approval['grantedOn']
                    approval['grantedOn'] = time.strftime('%Y-%m-%dT%H:%M:%S',
                                                          time.localtime(adate_ts))
    if 'comments' in item.keys():
        for comment in item['comments']:
            cdate_ts = comment['timestamp']
            comment['timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%S',
                                                 time.localtime(cdate_ts))

def get_projects():
    # First we need all projects
    cache_file = "gerrit-projects_cache.json"
    cache_file = os.path.join(cache_dir, cache_file)

    if not os.path.isfile(cache_file):
        gerrit_cmd_projects = gerrit_cmd + "ls-projects "
        raw_data = subprocess.check_output(gerrit_cmd_projects, shell = True)
        with open(cache_file, 'w') as f:
            f.write(raw_data)
    else:
        with open(cache_file) as f:
            raw_data = f.read()

    print raw_data

    projects = raw_data.split("\n")
    projects.pop() # Remove last empty line

    return projects

def fetch_events(review):
    """ Fetch in ES patches and comments (events) as documents """

    bulk_json = ""  # Bulk JSON to be feeded in ES

    # Review fields included in all events
    bulk_json_review  = '"review_id":"%s",' % review['id']
    bulk_json_review += '"review_createdOn":"%s",' % review['createdOn']
    bulk_json_review += '"review_email":"%s",' % review['owner']['email']
    bulk_json_review += '"review_status":"%s"' % review['status']
    # bulk_json_review += '"review_subject":"%s"' % review['subject']
    # bulk_json_review += '"review_topic":"%s"' % review['topic']

    for patch in review['patchSets']:
        # Patch fields included in all patch events
        bulk_json_patch  = '"patchSet_id":"%s",' % patch['number']
        bulk_json_patch += '"patchSet_createdOn":"%s",' % patch['createdOn']
        if 'author' in patch and 'email' in patch['author']:
            bulk_json_patch += '"patchSet_email":"%s"' % patch['author']['email']
        else:
            bulk_json_patch += '"patchSet_email":None'

        app_count = 0  # Approval counter for unique id
        if 'approvals' not in patch:
            bulk_json_ap  = '"approval_type":None,'
            bulk_json_ap += '"approval_value":None,'
            bulk_json_ap += '"approval_email":None'

            bulk_json_event = '{%s,%s,%s}' % (bulk_json_review,
                                              bulk_json_patch, bulk_json_ap)

            event_id = "%s_%s_%s" % (review['id'], patch['number'], app_count)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (event_id)  # Bulk operation
            bulk_json += bulk_json_event +"\n"  # Bulk document

        else:
            for app in patch['approvals']:
                bulk_json_ap  = '"approval_type":"%s",' % app['type']
                bulk_json_ap += '"approval_value":%i,' % int(app['value'])
                bulk_json_ap += '"approval_grantedOn":"%s",' % app['grantedOn']
                if 'email' in app['by']:
                    bulk_json_ap += '"approval_email":"%s",' % app['by']['email']
                else:
                    bulk_json_ap += '"approval_email":None,'
                if 'username' in app['by']:
                    bulk_json_ap += '"approval_username":"%s"' % app['by']['username']
                else:
                    bulk_json_ap += '"approval_username":None'
                bulk_json_event = '{%s,%s,%s}' % (bulk_json_review,
                                                  bulk_json_patch, bulk_json_ap)

                event_id = "%s_%s_%s" % (review['id'], patch['number'], app_count)
                bulk_json += '{"index" : {"_id" : "%s" } }\n' % (event_id)  # Bulk operation
                bulk_json += bulk_json_event +"\n"  # Bulk document
                app_count += 1

    url = elasticsearch_url+'/gerrit/reviews_events/_bulk'
    request = urllib2.Request(url, data=bulk_json)
    request.get_method = lambda: 'POST'

    opener.open(request)


def project_reviews_to_es(project):

    gerrit_cmd_prj = gerrit_cmd + " query project:"+project+" "
    gerrit_cmd_prj += "limit:" + max_items + " --all-approvals --comments --format=JSON"

    cache_file = "gerrit-"+project.replace("/","_")+"_cache.json"
    cache_file = os.path.join(cache_dir, cache_file)

    if not os.path.isfile(cache_file):
        # If data is not in cache, gather it
        raw_data = subprocess.check_output(gerrit_cmd_prj, shell = True)

        # Complete the raw_data to be a complete JSON document
        raw_data = "[" + raw_data.replace("\n", ",") + "]"
        raw_data = raw_data.replace(",]", "]")
        with open(cache_file, 'w') as f:
            f.write(raw_data)

    else:
        with open(cache_file) as f:
            raw_data = f.read()

    # Parse JSON document
    gerrit_json = json.loads(raw_data)

    for item in gerrit_json:
        if 'project' in item.keys():
            # Detected review JSON object

            fix_review_dates(item)

            data_json = json.dumps(item)
            url = elasticsearch_url + "/"+elasticsearch_index
            url += "/"+str(item["id"])
            request = urllib2.Request(url, data=data_json)
            request.get_method = lambda: 'PUT'
            opener.open(request)

            fetch_events(item)

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    cache_dir = "cache"
    if not os.path.exists(cache_dir):

        os.makedirs(cache_dir)
    # Get reviews JSON data from gerrit using SSH

    max_items = "500"

    elasticsearch_url = "http://sega.bitergia.net:9200"
    elasticsearch_url = "http://localhost:9200"
    elasticsearch_index = "gerrit/reviews"
    gerrit_cmd  = "ssh -p 29418 acs@gerrit.wikimedia.org "
    gerrit_cmd += "gerrit "

    # Feed the reviews items in EL
    opener = urllib2.build_opener(urllib2.HTTPHandler)

    # First we need all projects
    projects = get_projects()

    total = len(projects)
    done = 0

    for project in projects:
        logging.info("Processing project:" + project + " " +
                     str(done) + "/" + str(total))
        project_reviews_to_es(project)
        done += 1
