#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Bugzilla Ocean feeder
#
# Copyright (C) 2016 Bitergia
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

'''Bugzilla Ocean feeder'''

from .elastic import ElasticOcean

class BugzillaOcean(ElasticOcean):

    def _fix_item(self, item):
        bug_id = item["data"]["bug_id"][0]['__text__']
        item["ocean-unique-id"] = bug_id+"_"+item['origin']
        # Could be used for filtering
        product = item['data']['product'][0]['__text__']
        item['product'] = product
