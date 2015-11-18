#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Perceval Cache Item library 
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
import json
import logging
import os

class CacheItems(object):

    def __init__(self, cache_dir, field_id):
        self.cache_dir = cache_dir
        self.field_id = field_id
        self.cache = []
        pass

    def clean(self):
        logging.debug("Cleaning cache")
        filelist = [ f for f in os.listdir(self.cache_dir) if
                    f.startswith("cache_item_") ]
        for f in filelist:
            os.remove(os.path.join(self.cache_dir, f))


    def item_to_cache(self, item):
        data_json = json.dumps(item)
        cache_file = os.path.join(self.cache_dir,
                      "cache_item_%s.json" % (item[self.field_id]))
        with open(cache_file, "w") as cache:
            cache.write(data_json)


    def items_to_cache(self, items):
        for item in items:
            self.item_to_cache(item)


    def items_from_cache(self):

        task_init = datetime.now()

        if len(self.cache) > 0:
            logging.debug("Cache already read")
            return self.cache

        logging.debug("Reading items from cache %s" % (self.cache_dir))
        # Just read all issues cache files
        filelist = [ f for f in os.listdir(self.cache_dir) if
                    f.startswith("cache_item_") ]
        logging.debug("Total issues in cache: %i" % (len(filelist)))
        for f in filelist:
            fname = os.path.join(self.cache_dir, f)
            with open(fname,"r") as f:
                item = json.loads(f.read())
                self.cache.append(item)

        task_time = (datetime.now() - task_init).total_seconds()
        logging.debug("Cache read in %.2f secs" % (task_time))

        return self.cache
