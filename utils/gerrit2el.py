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


if __name__ == '__main__':

    # Get reviews JSON data from gerrit using SSH

    max_items = "500"

    # elasticsearch_url = "http://sega.bitergia.net:9200"
    elasticsearch_url = "http://localhost:9200"
    elasticsearch_index = "gerrit/reviews"
    project = "VisualEditor/VisualEditor"
    gerrit_cmd  = "ssh -p 29418 acs@gerrit.wikimedia.org "
    gerrit_cmd += "gerrit query project:"+project+" "
    gerrit_cmd += "limit:"+max_items+" --all-approvals --comments --format=JSON"

    cache_file = "gerrit-"+project.replace("/","_")+"_cache.json"

    if not os.path.isfile(cache_file):
        # If data is not in cache, gather it
        raw_data = subprocess.check_output(gerrit_cmd, shell = True)

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

    # Feed the reviews items in EL
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    for item in gerrit_json:
        if 'project' in item.keys():
            # Detected review JSON object

            fix_review_dates(item)

            data_json = json.dumps(item)
            url = elasticsearch_url + "/"+elasticsearch_index
            url += "/"+str(item["id"])
            request = urllib2.Request(url, data=data_json)
            request.get_method = lambda: 'PUT'
            response = opener.open(request)