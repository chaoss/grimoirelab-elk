#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Perceval Backend base class 
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

''' Backend define the API to be supported in Perceval data sources backends '''

import json
import logging
import os
from os.path import expanduser, join
import requests
import traceback

from perceval.cache import CacheItems


class Backend(object):

    @classmethod
    def get_name(cls):
        ''' Human name for the backend class '''

        return cls.name


    def __init__(self, cache = False):

        self.cache = cache
        cache_dir = os.path.join(self._get_storage_dir(),"cache")
        self.cache_items = CacheItems(cache_dir, self.get_field_unique_id())

        # Create storage dir if it not exists
        storage_dir = self._get_storage_dir()
        if not os.path.isdir(storage_dir):
            os.makedirs(storage_dir)


    def get_id(self):
        '''Unique identifier for a backend instance '''
        raise NotImplementedError

    def get_field_unique_id(self):
        ''' Field with the unique id for the JSON items '''
        raise NotImplementedError

    def _get_storage_dir(self):

        home = expanduser("~")
        '''Get directory in which to store backend data'''

        _dir = join(home, ".perceval", self.get_name(), self.get_id())

        return _dir

    def fetch(self, start = None, end = None, cache = False,
              project = None):
        ''' Returns an iterator for feeding data '''

        iter_ = self

        if self.cache:
            # If cache, work directly with the cache iterator
            logging.info("Using cache")
            iter_ = self.cache_items
        else:
            self.start = start
            self.end = end
            self.cache = cache
            self.project = project

        return iter_


    def __iter__(self):
        ''' Specific iterator implementatio for Backed '''
        raise NotImplementedError

    def __next__(self):
        ''' Specific iterator implementatio for Backed '''
        raise NotImplementedError
