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
# TODO: Just a playing script yet.

import json
import logging
import MySQLdb
import os
import subprocess
import time
import urllib2

# https://github.com/jgbarah/Grimoire-demo/blob/master/grimoire-ng-data.py#L338
class Database:
    """To work with a database (likely including several schemas).
    """

    def __init__ (self, user, passwd, host, port, scrdb, shdb, prjdb):
        self.user = user
        self.passwd = passwd
        self.host = host
        self.port = port
        self.scrdb = scrdb
        self.shdb = shdb
        self.prjdb = prjdb
        self.db, self.cursor = self._connect()

    def _connect(self):
        """Connect to the MySQL database.
        """

        try:
            db = MySQLdb.connect(user = self.user, passwd = self.passwd,
                                 host = self.host, port = self.port,
                                 db = self.shdb,
                                 use_unicode = True)
            return db, db.cursor()
        except:
            logging.error("Database connection error")
            raise

    def execute(self, query):
        """Execute an SQL query with the corresponding database.
        The query can be "templated" with {scm_db} and {sh_db}.
        """

        # sql = query.format(scm_db = self.scmdb,
        #                   sh_db = self.shdb,
        #                   prj_db = self.prjdb)

        results = int (self.cursor.execute(query))
        if results > 0:
            result1 = self.cursor.fetchall()
            return result1
        else:
            return []


def fix_review_dates(item):
    """ Convert dates so ES detect them """

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
    """ Get all projects in gerrit """

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

def get_organization(email):
    """ Get in the most efficient way the organization for an email """

    org = "Unknown"
    try:
        org = email2org[email]
    except:
        # logging.info("Can't find org for " + email)
        pass

    return org

def fetch_events(review):
    """ Fetch in ES patches and comments (events) as documents """

    elasticsearch_type = "reviews_events"
    url = elasticsearch_url + "/"+elasticsearch_index
    url_type = url + "/" + elasticsearch_type

    reviews_events_map = """
    {
        "properties": {
           "approval_email": {
              "type": "string",
              "index":"not_analyzed"
           },
           "approval_grantedOn": {
              "type": "date",
              "format": "dateOptionalTime"
           },
           "approval_organization": {
              "type": "string",
              "index":"not_analyzed"
           },
           "approval_type": {
              "type": "string",
              "index":"not_analyzed"
           },
           "approval_username": {
              "type": "string"
           },
           "approval_value": {
              "type": "long"
           },
           "patchSet_createdOn": {
              "type": "date",
              "format": "dateOptionalTime"
           },
           "patchSet_email": {
              "type": "string",
              "index":"not_analyzed"
           },
           "patchSet_id": {
              "type": "string"
           },
           "patchSet_organization": {
              "type": "string",
              "index":"not_analyzed"
           },
           "review_branch": {
              "type": "string",
              "index":"not_analyzed"
           },
           "review_createdOn": {
              "type": "date",
              "format": "dateOptionalTime"
           },
           "review_email": {
              "type": "string",
              "index":"not_analyzed"
           },
           "review_id": {
              "type": "string"
           },
           "review_organization": {
              "type": "string",
              "index":"not_analyzed"
           },
           "review_project": {
              "type": "string",
              "index":"not_analyzed"
           },
           "review_status": {
              "type": "string",
              "index":"not_analyzed"
           }
        }
    }
    """
    # Create mappings
    url_map = url_type+"/_mapping"
    request = urllib2.Request(url_map, data=reviews_events_map)
    request.get_method = lambda: 'PUT'
    opener.open(request)


    bulk_json = ""  # Bulk JSON to be feeded in ES

    # Review fields included in all events
    bulk_json_review  = '"review_id":"%s",' % review['id']
    bulk_json_review += '"review_createdOn":"%s",' % review['createdOn']
    if 'owner' in review and 'email' in review['owner']:
        email = review['owner']['email']
        bulk_json_review += '"review_email":"%s",' % email
        bulk_json_review += '"review_organization":"%s",' % get_organization(email)
    else:
        bulk_json_review += '"review_email":null,'
        bulk_json_review += '"review_organization":null,'
    bulk_json_review += '"review_status":"%s",' % review['status']
    bulk_json_review += '"review_project":"%s",' % review['project']
    bulk_json_review += '"review_branch":"%s"' % review['branch']
    # bulk_json_review += '"review_subject":"%s"' % review['subject']
    # bulk_json_review += '"review_topic":"%s"' % review['topic']

    for patch in review['patchSets']:
        # Patch fields included in all patch events
        bulk_json_patch  = '"patchSet_id":"%s",' % patch['number']
        bulk_json_patch += '"patchSet_createdOn":"%s",' % patch['createdOn']
        if 'author' in patch and 'email' in patch['author']:
            email = patch['author']['email']
            bulk_json_patch += '"patchSet_email":"%s",' % email
            bulk_json_patch += '"patchSet_organization":"%s"' % get_organization(email)
        else:
            bulk_json_patch += '"patchSet_email":null,'
            bulk_json_patch += '"patchSet_organization":null'

        app_count = 0  # Approval counter for unique id
        if 'approvals' not in patch:
            bulk_json_ap  = '"approval_type":null,'
            bulk_json_ap += '"approval_value":null,'
            bulk_json_ap += '"approval_email":null,'
            bulk_json_ap += '"approval_organization":null'

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
                    bulk_json_ap += '"approval_organization":"%s",' % get_organization(app['by']['email'])
                else:
                    bulk_json_ap += '"approval_email":null,'
                    bulk_json_ap += '"approval_organization":null,'
                if 'username' in app['by']:
                    bulk_json_ap += '"approval_username":"%s"' % app['by']['username']
                else:
                    bulk_json_ap += '"approval_username":null'
                bulk_json_event = '{%s,%s,%s}' % (bulk_json_review,
                                                  bulk_json_patch, bulk_json_ap)

                event_id = "%s_%s_%s" % (review['id'], patch['number'], app_count)
                bulk_json += '{"index" : {"_id" : "%s" } }\n' % (event_id)  # Bulk operation
                bulk_json += bulk_json_event +"\n"  # Bulk document
                app_count += 1

    url = elasticsearch_url+'/gerrit/reviews_events/_bulk'
    url = elasticsearch_url+'/gerrit_openstack/reviews_events/_bulk'
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

    elasticsearch_type = "reviews"
    for item in gerrit_json:
        if 'project' in item.keys():
            # Detected review JSON object

            fix_review_dates(item)

            data_json = json.dumps(item)
            url = elasticsearch_url + "/"+elasticsearch_index
            url += "/"+elasticsearch_type
            url += "/"+str(item["id"])
            request = urllib2.Request(url, data=data_json)
            request.get_method = lambda: 'PUT'
            opener.open(request)

            fetch_events(item)

def sortinghat_to_es():
    """ Load all identities data in SH """

    elasticsearch_type = "profiles"
    url = elasticsearch_url + "/"+elasticsearch_index
    url_type = url + "/" + elasticsearch_type

    profile_map = """
    {
        "properties": {
           "email": {
              "type": "string",
              "index":"not_analyzed"
           },
           "uuid": {
              "type": "string"
           },
           "is_bot": {
              "type": "boolean"
           },
           "organization": {
              "type": "string",
              "index":"not_analyzed"
           }
        }
    }
    """

    # Create mappings
    url_map = url_type+"/_mapping"
    request = urllib2.Request(url_map, data=profile_map)
    request.get_method = lambda: 'PUT'
    opener.open(request)

    sortinghat_db = "acs_sortinghat_mediawiki_5879"
    sortinghat_db = "amartin_sortinghat_openstack_sh"
    db = Database (user = "root", passwd = "",
                   host = "localhost", port = 3306,
                   scrdb = None, shdb = sortinghat_db, prjdb = None)

    sql = """
        SELECT p.uuid, email, is_bot, o.name as organization
        FROM profiles p
        JOIN enrollments e ON e.uuid=p.uuid
        JOIN organizations o ON o.id = e.organization_id
        WHERE email is not NULL
        """
    profiles = db.execute(sql)

    # bulk_json = ""# To Load in ES quickly
    profiles_count = 0
    profiles_error = 0


    for profile in profiles:
        uuid = profile[0]
        email = profile[1]
        is_bot = profile[2]
        organization = profile[3]

        email2org[email] = organization

        if email is None: continue
        profile_json = "{"
        profile_json += '"uuid":"%s",' % uuid
        profile_json += '"email":"%s",' % email
        profile_json += '"is_bot":"%s",' % is_bot
        profile_json += '"organization":"%s"' % organization
        profile_json += "}"

        url_profile = url_type + "/"+str(uuid)
        request = urllib2.Request(url_profile, data=profile_json)
        request.get_method = lambda: 'PUT'
        try:
            opener.open(request)
            profiles_count += 1
        except:
            logging.error("Can't add: " + email)
            profiles_error += 1

#         bulk_json += '{"index" : {"_id" : "%s" } }\n' % (uuid)  # Bulk operation
#         bulk_json += profile_json +"\n"  # Bulk document


    print "Profiles loaded: %i" % profiles_count
    print "Profiles error: %i" % profiles_error

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    cache_dir = "cache"
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    # Reviews will be added to ES using its API REST
    elasticsearch_url = "http://sega.bitergia.net:9200"
    elasticsearch_url = "http://localhost:9200"
    elasticsearch_index = "gerrit"
    elasticsearch_index = "gerrit_openstack"

    # Get reviews JSON data from gerrit using SSH
    max_items = "500"
    gerrit_cmd  = "ssh -p 29418 acs@gerrit.wikimedia.org "
    gerrit_cmd  = "ssh -p 29418 acs@review.openstack.org "
    gerrit_cmd += "gerrit "
    # Feed the reviews items in EL
    opener = urllib2.build_opener(urllib2.HTTPHandler)

    # Add profiles to sortinghat
    email2org = {}
    sortinghat_to_es()

    # First we need all projects
    projects = get_projects()

    total = len(projects)
    done = 0

    for project in projects:
        logging.info("Processing project:" + project + " " +
                     str(done) + "/" + str(total))
        project_reviews_to_es(project)
        done += 1
