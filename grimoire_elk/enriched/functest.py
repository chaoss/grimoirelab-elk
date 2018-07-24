# -*- coding: utf-8 -*-
#
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

import logging

from .enrich import Enrich, metadata


logger = logging.getLogger(__name__)


class FunctestEnrich(Enrich):

    BOOST_PROJECTS = ['functest', 'storperf', 'vsperf', 'bottlenecks', 'qtip', 'yardstick']

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        return identities

    def get_field_author(self):
        # In Functest there is no identities support
        return None

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None

        # The real data
        func_test = item['data']

        # data fields to copy
        copy_fields = ["start_date", "stop_date", "case_name", "criteria", "scenario",
                       "version", "pod_name", "installer", "build_tag", "trust_indicator"]
        for f in copy_fields:
            if f in func_test:
                eitem[f] = func_test[f]
            else:
                eitem[f] = None

        # Fields which names are translated
        map_fields = {"_id": "api_id",
                      "project_name": "project"
                      }
        for fn in map_fields:
            if fn in func_test:
                eitem[map_fields[fn]] = func_test[fn]
            else:
                eitem[map_fields[fn]] = None

        if 'details' in func_test and func_test['details']:
            if 'tests' in func_test['details']:
                if isinstance(func_test['details']['tests'], int):
                    # Only propagate tests if it is a number
                    eitem['tests'] = func_test['details']['tests']
            if 'failures' in func_test['details']:

                if type(func_test['details']['failures']) == list:
                    eitem['failures'] = len(func_test['details']['failures'])
                else:
                    eitem['failures'] = func_test['details']['failures']

            if 'duration' in func_test['details']:
                eitem['duration'] = func_test['details']['duration']

        if 'duration' not in eitem:
            eitem['duration'] = None
        if 'failures' not in eitem:
            eitem['failures'] = None
        if 'tests' not in eitem:
            eitem['tests'] = None

        eitem.update(self.get_grimoire_fields(func_test['start_date'], "func_test"))

        # The project is a field already included in the raw data
        # if self.prjs_map:
        #     eitem.update(self.get_item_project(eitem))

        # Hack to show BOOST_PROJECTS first in the donut vis
        eitem['boost_list'] = ['all']
        if eitem['project'] and eitem['project'].lower() in self.BOOST_PROJECTS:
            eitem['boost_list'] += ['boosted']

        return eitem
