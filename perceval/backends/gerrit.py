#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# GitHub Pull Requests for Elastic Search
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

'''Gerrit backend for Gerrit'''


from datetime import datetime
from dateutil import parser
import json
import logging
import re
import subprocess
from time import time
from perceval.utils import get_eta

from perceval.backend import Backend



class Gerrit(Backend):

    name = "gerrit"

    @classmethod
    def add_params(cls, cmdline_parser):
        parser = cmdline_parser

        parser.add_argument("--user",
                            help="Gerrit ssh user")
        parser.add_argument("-u", "--url", required=True,
                            help="Gerrit url")
        parser.add_argument("--nreviews",  default=500, type=int,
                            help="Number of reviews per ssh query")


    def __init__(self, user = None, url = None, nreviews = None, 
                 cache = False, **nouse):

        self.repository = url
        self.nreviews = nreviews
        self.client = GerritClient(self.repository, user, nreviews)

        self.last_item = None  # Start item for next iteration
        self.more_updates = True  # To check if reviews are updates
        self.number_results = self.nreviews  # To control if there are more items

        super(Gerrit, self).__init__(cache)


    def get_id(self):
        ''' Return gerrit unique identifier '''

        return self.repository


    def get_field_unique_id(self):
        return "id"


    def fetch_live(self, startdate):

        reviews = self.get_reviews(startdate)
        self.cache_items.items_to_cache(reviews)

        while reviews:
            issue = reviews.pop(0)
            yield issue

            if not reviews:
                reviews = self.get_reviews(startdate)


    def fetch(self, start = None, end = None, cache = False,
              project = None):
        ''' Returns an iterator for feeding data '''

        if self.cache:
            # If cache, work directly with the cache iterator
            logging.info("Using cache")
            return self.cache_items
        else:
            return self.fetch_live(start)


    def get_url(self):

        return self.url

    def get_reviews(self, startdate = None):
        """ Get all reviews from repository """

        reviews = []

        if self.number_results < self.nreviews or not self.more_updates:
            # No more reviews after last iteration
            return reviews

        task_init = time()
        raw_data = self.client.get_items(self.last_item)
        raw_data = str(raw_data, "UTF-8")
        tickets_raw = "[" + raw_data.replace("\n", ",") + "]"
        tickets_raw = tickets_raw.replace(",]", "]")

        tickets = json.loads(tickets_raw)

        for entry in tickets:
            if 'project' in entry.keys():
                entry_lastUpdated =  datetime.fromtimestamp(entry['lastUpdated'])
                entry['lastUpdated_date'] = entry_lastUpdated.isoformat()

                if startdate: # Incremental mode
                    if entry_lastUpdated <= parser.parse(startdate):
                        logging.debug("No more updates for %s" % (self.repository))
                        self.more_updates = False
                        break

                reviews.append(entry)

                self.last_item = self.client.get_next_item(self.last_item,
                                                           entry)
            elif 'rowCount' in entry.keys():
                # logging.info("CONTINUE FROM: " + str(last_item))
                self.number_results = entry['rowCount']

        logging.info("Received %i reviews in %.2fs" % (len(reviews),
                                                       time()-task_init))
        return reviews


class GerritClient():

    def __init__(self, repository, user, nreviews):
        self.gerrit_user = user
        self.nreviews = nreviews
        self.repository = repository
        self.project = None
        self.version = None
        self.gerrit_cmd  = "ssh -p 29418 %s@%s" % (self.gerrit_user, self.repository)
        self.gerrit_cmd += " gerrit "

    def _get_version(self):

        if self.version:
            return self.version

        cmd = self.gerrit_cmd + " version "

        logging.debug("Getting version: %s" % (cmd))
        raw_data = subprocess.check_output(cmd, shell = True)
        raw_data = str(raw_data, "UTF-8")
        logging.debug("Gerrit version: %s" % (raw_data))

        # output: gerrit version 2.10-rc1-988-g333a9dd
        m = re.match("gerrit version (\d+)\.(\d+).*", raw_data)

        if not m:
            raise Exception("Invalid gerrit version %s" % raw_data)

        try:
            mayor = int(m.group(1))
            minor = int(m.group(2))
        except:
            raise Exception("Invalid gerrit version %s " %
                            (raw_data))

        self.version = [mayor, minor]
        return  self.version

    def _get_gerrit_cmd(self, last_item):

        cmd = self.gerrit_cmd + " query "
        if self.project:
            cmd +="project:"+self.project+" "
        cmd += "limit:" + str(self.nreviews)

        # This does not work for Wikimedia 2.8.1 version
        cmd += " '(status:open OR status:closed)' "

        cmd += " --all-approvals --comments --format=JSON"

        gerrit_version = self._get_version()

        if last_item is not None:
            if gerrit_version[0] == 2 and gerrit_version[1] >= 9:
                cmd += " --start=" + str(last_item)
            else:
                cmd += " resume_sortkey:" + last_item

        return cmd

    def get_items(self, last_item):
        cmd = self._get_gerrit_cmd(last_item)

        logging.debug(cmd)
        raw_data = subprocess.check_output(cmd, shell = True)

        return raw_data


    def get_next_item(self, last_item, entry):

        next_item = None

        gerrit_version = self._get_version()

        if gerrit_version[0] == 2 and gerrit_version[1] >= 9:
            if last_item is None:
                next_item = 0
            else:
                next_item = last_item+1

        else:
            if last_item is None:
                next_item = None
            else:
                next_item = entry['sortKey']

        return next_item