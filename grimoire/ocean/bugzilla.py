#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Gerrit Ocean feeder
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

'''Gerrit Ocean feeder'''

from dateutil import parser
from grimoire.ocean.elastic import ElasticOcean

class BugzillaOcean(ElasticOcean):

    def get_field_date(self):
        field = 'metadata__updated_on'
        return field

    def get_field_unique_id(self):
        return "ocean-unique-id"

    def get_last_update_from_es(self):
        ''' Find in JSON storage the last update date '''

        last_update = self.elastic.get_last_date(self.get_field_date())

            # Format date so it can be used as URL param in bugzilla
        if last_update is not None:
            last_update = last_update

        return last_update

    def _fix_item(self, item):
        item["ocean-unique-id"] = item["bug_id"]+"_"+item["__metadata__"]['origin']

    def add_update_date(self, item):
        # entry_lastUpdated = parser.parse(item['delta_ts'][0]["__text__"])
        entry_lastUpdated = parser.parse(item['__metadata__']['updated_on'])
        # Use local server time for incremental updates
        update = entry_lastUpdated.replace(tzinfo=None)
        item['metadata__updated_on'] = update.isoformat()
