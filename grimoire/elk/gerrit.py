#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Gerrit to Elastic class helper
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

from datetime import datetime
import logging
import requests
import time


from grimoire.elk.enrich import Enrich
from grimoire.ocean.gerrit import GerritOcean


class GerritEnrich(Enrich):

    def __init__(self, gerrit, **nouse):
        super().__init__()
        self.gerrit = gerrit
        self.elastic = None

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_date(self):
        return "approval_grantedOn"

    def get_fields_uuid(self):
        return ["review_uuid", "patchSet_uuid", "approval_uuid"]

    def _fix_review_dates(self, item):
        ''' Convert dates so ES detect them '''


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
                        approval['grantedOn'] = \
                            time.strftime('%Y-%m-%dT%H:%M:%S', 
                                          time.localtime(adate_ts))
        if 'comments' in item.keys():
            for comment in item['comments']:
                cdate_ts = comment['timestamp']
                comment['timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%S',
                                                     time.localtime(cdate_ts))


    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
               "approval_email": {
                  "type": "string",
                  "index":"not_analyzed"
               },
               "approval_organization": {
                  "type": "string",
                  "index":"not_analyzed"
               },
               "approval_type": {
                  "type": "string",
                  "index":"not_analyzed"
               },
               "patchSet_email": {
                  "type": "string",
                  "index":"not_analyzed"
               },
               "patchSet_organization": {
                  "type": "string",
                  "index":"not_analyzed"
               },
               "review_branch": {
                  "type": "string",
                  "index":"not_analyzed"
               },
               "review_email": {
                  "type": "string",
                  "index":"not_analyzed"
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

        return {"items":mapping}


    def review_events(self, review):

        self._fix_review_dates(review)

        bulk_json = ""  # Bulk JSON to be feeded in ES

        # Review fields included in all events
        bulk_json_review  = '"review_id":"%s",' % review['id']
        bulk_json_review += '"review_createdOn":"%s",' % review['createdOn']
        if 'owner' in review and 'email' in review['owner']:
            identity = GerritOcean.get_sh_identity(review['owner'])
            ruuid = self.get_uuid(identity, self.gerrit.get_name())
            remail = review['owner']['email']
            bulk_json_review += '"review_email":"%s",' % remail
            bulk_json_review += '"review_uuid":"%s",' % ruuid
        else:
            bulk_json_review += '"review_email":null,'
            bulk_json_review += '"review_uuid":null,'
        bulk_json_review += '"review_status":"%s",' % review['status']
        bulk_json_review += '"review_project":"%s",' % review['project']
        bulk_json_review += '"review_branch":"%s"' % review['branch']
        # bulk_json_review += '"review_subject":"%s"' % review['subject']
        # bulk_json_review += '"review_topic":"%s"' % review['topic']

        # To be used as review['createdOn'] which is wrong in OpenStack/Wikimedia
        firstPatchCreatedOn = review['patchSets'][0]['createdOn']

        for patch in review['patchSets']:
            # Patch fields included in all patch events
            bulk_json_patch  = '"patchSet_id":"%s",' % patch['number']
            bulk_json_patch += '"patchSet_createdOn":"%s",' % patch['createdOn']
            if 'author' in patch and 'email' in patch['author']:
                identity = GerritOcean.get_sh_identity(patch['author'])
                puuid = self.get_uuid(identity, self.gerrit.get_name())
                pemail = patch['author']['email']
                bulk_json_patch += '"patchSet_email":"%s",' % pemail
                bulk_json_patch += '"patchSet_uuid":"%s"' % puuid
            else:
                bulk_json_patch += '"patchSet_email":null,'
                bulk_json_patch += '"patchSet_uuid":null'

            app_count = 0  # Approval counter for unique id
            if 'approvals' not in patch:
                bulk_json_ap  = '"approval_type":null,'
                bulk_json_ap += '"approval_value":null,'
                bulk_json_ap += '"approval_email":null,'
                bulk_json_ap += '"approval_uuid":null'

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
                        identity = GerritOcean.get_sh_identity(app['by'])
                        auuid = self.get_uuid(identity, self.gerrit.get_name())
                        aemail = app['by']['email']
                        bulk_json_ap += '"approval_email":"%s",' % aemail
                        bulk_json_ap += '"approval_uuid":"%s",' % auuid
                    else:
                        bulk_json_ap += '"approval_email":null,'
                        bulk_json_ap += '"approval_uuid":null,'
                    if 'username' in app['by']:
                        bulk_json_ap += '"approval_username":"%s",' % app['by']['username']
                    else:
                        bulk_json_ap += '"approval_username":null,'

                    # Time to add the time diffs
                    app_time = \
                        datetime.strptime(app['grantedOn'], "%Y-%m-%dT%H:%M:%S")
                    patch_time = \
                        datetime.strptime(patch['createdOn'], "%Y-%m-%dT%H:%M:%S")
                    # review_time = \
                    #    datetime.strptime(review['createdOn'], "%Y-%m-%dT%H:%M:%S")
                    review_time = \
                        datetime.strptime(firstPatchCreatedOn, "%Y-%m-%dT%H:%M:%S")

                    seconds_day = float(60*60*24)
                    approval_time = \
                        (app_time-review_time).total_seconds() / seconds_day
                    approval_patch_time = \
                        (app_time-patch_time).total_seconds() / seconds_day
                    patch_time = \
                        (patch_time-review_time).total_seconds() / seconds_day
                    bulk_json_ap += '"approval_time_days":%.2f,' % approval_time
                    bulk_json_ap += '"approval_patch_time_days":%.2f,' % \
                        approval_patch_time
                    bulk_json_ap += '"patch_time_days":%.2f' % patch_time

                    bulk_json_event = '{%s,%s,%s}' % (bulk_json_review,
                                                      bulk_json_patch, bulk_json_ap)

                    event_id = "%s_%s_%s" % (review['id'], patch['number'], app_count)
                    bulk_json += '{"index" : {"_id" : "%s" } }\n' % (event_id)  # Bulk operation
                    bulk_json += bulk_json_event +"\n"  # Bulk document

                    app_count += 1

        return bulk_json


    def enrich_items(self, items):
        """ Fetch in ES patches and comments (events) as documents """

        for review in items:

            bulk_json = self.review_events(review)
            url = self.elastic.index_url+'/reviews_events/_bulk'

            try:
                requests.put(url, data=bulk_json)
            except UnicodeEncodeError:
                # Why is requests encoding the POST data as ascii?
                logging.error("Unicode error for events in review: " + review['id'])
                safe_json = str(bulk_json.encode('ascii', 'ignore'),'ascii')
                requests.put(url, data=safe_json)
                # Continue with execution.

