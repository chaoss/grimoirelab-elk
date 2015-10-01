#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import datetime
import json
import os
import subprocess
import urllib2

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
#        serial = obj.isoformat()
        serial = int((obj - datetime(1970,1,1)).total_seconds())
        return serial
    raise TypeError ("Type not serializable")

def json_dumps(data, compact = True):
    if compact:
        return json.dumps(data, sort_keys=False,
                          separators=(',',':'),
                          default=json_serial)
    else:
        return json.dumps(data, sort_keys=False, 
                          indent=4, separators=(',', ': '),
                          default=json_serial)


if __name__ == '__main__':

    # Get reviews JSON data from gerrit using SSH

    max_items = "500"

    elasticsearch_url = "http://sega.bitergia.net:9200"
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
            # In a review JSON object
            data_json = json_dumps(item, compact = True)
            url = elasticsearch_url + "/"+elasticsearch_index
            url += "/"+str(item["id"])
            request = urllib2.Request(url, data=data_json)
            request.get_method = lambda: 'PUT'
            response = opener.open(request)