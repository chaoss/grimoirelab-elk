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

from perceval.backends.backend import Backend



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
                 use_cache = False, args = None):


        if not args:
            self.gerrit_user = user
            self.nreviews = nreviews
            self.url = url
            use_cache = use_cache
        else:
            self.gerrit_user = args.user
            self.nreviews = args.nreviews
            self.url = args.url
            use_cache = args.cache

        self.project = None
        self.version = None
        self.gerrit_cmd  = "ssh -p 29418 %s@%s" % (self.gerrit_user, self.url)
        self.gerrit_cmd += " gerrit "

        super(Gerrit, self).__init__(use_cache)


    def get_id(self):
        ''' Return gerrit unique identifier '''

        return self.url


    def get_field_unique_id(self):
        return "id"


    def get_url(self):

        return self.url


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

    def _get_gerrit_cmd(self):

        cmd = self.gerrit_cmd + " query "
        if self.project:
            cmd +="project:"+self.project+" "
        cmd += "limit:" + str(self.nreviews)

        # This does not work for Wikimedia 2.8.1 version
        cmd += " '(status:open OR status:closed)' "

        cmd += " --all-approvals --comments --format=JSON"

        self.number_results = self.nreviews

        return cmd 

    def _get_items(self):
        """ Get all reviews for all or for a project """

        reviews = []
        self.more_updates = True

        gerrit_version = self._get_version()

        cmd = self._get_gerrit_cmd()

        if self.last_item is not None:
            if gerrit_version[0] == 2 and gerrit_version[1] >= 9:
                cmd += " --start=" + str(self.last_item)
            else:
                cmd += " resume_sortkey:" + self.last_item

        logging.debug(cmd)
        task_init = time()
        raw_data = subprocess.check_output(cmd, shell = True)
        raw_data = str(raw_data, "UTF-8")
        tickets_raw = "[" + raw_data.replace("\n", ",") + "]"
        tickets_raw = tickets_raw.replace(",]", "]")

        tickets = json.loads(tickets_raw)

        for entry in tickets:
            if self.start:
                if 'project' in entry.keys():
                    entry_lastUpdated = \
                        datetime.fromtimestamp(entry['lastUpdated'])
                    if entry_lastUpdated <= parser.parse(self.start):
                        logging.debug("No more updates for %s" % (self.url))
                        self.more_updates = False
                        break

            if 'project' in entry.keys():
                entry['lastUpdated_date'] = \
                    datetime.fromtimestamp(entry['lastUpdated']).isoformat()
                reviews.append(entry)
                if gerrit_version[0] == 2 and gerrit_version[1] >= 9:
                    self.last_item += 1
                else:
                    self.last_item = entry['sortKey']
            elif 'rowCount' in entry.keys():
                # logging.info("CONTINUE FROM: " + str(last_item))
                self.number_results = entry['rowCount']

        logging.info("Received %i reviews in %.2fs" % (len(reviews),
                                                       time()-task_init))
        self.cache.items_to_cache(reviews)

        return reviews

    def __iter__(self):
        self.last_item = None

        gerrit_version = self._get_version()

        if gerrit_version[0] == 2 and gerrit_version[1] >= 9:
            self.last_item = 0

        self.items_pool = self._get_items()

        return self

    def __next__(self):
        if len(self.items_pool) == 0:
            # wikimedia gerrit returns limit+1
            if (self.number_results == self.nreviews + 1 or
                self.number_results == self.nreviews) and \
                self.more_updates:
                    self.items_pool = self._get_items()
            else:
                raise StopIteration

        return self.items_pool.pop()

