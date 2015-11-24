#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Elastic Enrichment API
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


class Enrich(object):

    @classmethod
    def add_params(cls, cmdline_parser):
        pass

    def set_elastic(self, elastic):
        self.elastic = elastic

    def enrich_items(self, items):
        raise NotImplementedError

    def get_elastic_mappings(self):
        ''' Mappings for enriched indexes '''
        pass
