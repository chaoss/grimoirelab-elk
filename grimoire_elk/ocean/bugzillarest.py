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

'''BugzillaREST Ocean feeder'''

from .elastic import ElasticOcean

class BugzillaRESTOcean(ElasticOcean):

    def _fix_item(self, item):
        bug_id = str(item["data"]["id"])
        item["ocean-unique-id"] = bug_id+"_"+item['origin']

        # Remove all custom fields to avoid the 1000 fields limit in ES
        fields = list(item["data"].keys())
        for field in fields:
            if field.startswith("cf_"):
                item["data"].pop(field)
        try:
            # Make this type always float (it changes between long and float)
            item["data"]['fields']["priority"]['subpriority'] = \
                float(item["data"]['fields']["priority"]['subpriority'])
        except:
            pass


    def get_elastic_mappings(self):
        # data.comments.text inmense term
        # data.history.changes.removed immense term
        mapping = '''
         {
            "dynamic":true,
            "properties": {
                "data": {
                    "properties": {
                        "comments": {
                            "dynamic":false,
                            "properties": {}
                        },
                        "history": {
                            "dynamic":false,
                            "properties": {}
                        }
                    }
                }
            }
        }
        '''

        return {"items":mapping}
