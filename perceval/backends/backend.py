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
# Bugzilla backend for Perseval

''' Backend define the API to be supported in Perceval data sources backends '''

import logging
import os
from os.path import expanduser, join
import traceback

class Backend(object):

    # Public Perceval backend API 
    def fetch(self):
        ''' Returns an iterator for the data gathered '''
        raise NotImplementedError


    def get_id(self):
        '''Unique identifier for a backend instance '''
        raise NotImplementedError

    # Internal class implementation

    @classmethod
    def add_params(cls, cmdline_parser):
        ''' Shared params in all backends '''

        parser = cmdline_parser

        parser.add_argument("--no_history",  action='store_true',
                            help="don't use history for repository")
        parser.add_argument("--cache",  action='store_true',
                            help="Use perseval cache")
        parser.add_argument("--debug",  action='store_true',
                            help="Increase logging to debug")



    def __init__(self, use_cache = False, history = False):

        self.cache = {}  # cache projects listing
        self.use_cache = use_cache
        self.use_history = history

        # Create storage dir if it not exists
        dump_dir = self._get_storage_dir()
        if not os.path.isdir(dump_dir):
            os.makedirs(dump_dir)

        if self.use_cache:
            # Don't use history data. Will be generated from cache.
            self.use_history = False

        if self.use_history:
            self._restore()  # Load history

        else:
            if self.use_cache:
                logging.info("Getting all data from cache")
                try:
                    self._load_cache()
                except:
                    # If any error loading the cache, clean it
                    traceback.print_exc(file=os.sys.stdout)
                    logging.debug("Cache corrupted")
                    self.use_cache = False
                    self._clean_cache()
            else:
                self._clean_cache()  # Cache will be refreshed

    def _load_cache(self):
        logging.info("Cache loading not implemented. Cache disabled.")
        self.use_cache = False


    def _clean_cache(self):
        logging.info("Cache cleaning not implemented. Cache disabled.")
        self.use_cache = False


    def _restore(self):
        ''' Restore data source status from last execution '''
        logging.info("History restore not implemented. History disabled.")
        self.use_history = False


    def _get_name(self):
        ''' Human name for the backend class '''
        raise NotImplementedError


    def _get_storage_dir(self):

        home = expanduser("~")
        '''Get directory in which to store backend data'''
        _dir = join(home, ".perceval", self._get_name(), self.get_id())

        return _dir

