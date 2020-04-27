# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import logging
import re

from grimoirelab_toolkit.datetime import str_to_datetime, unixtime_to_datetime

from .enrich import Enrich, metadata


logger = logging.getLogger(__name__)


class FunctestEnrich(Enrich):

    BOOST_PROJECTS = ['functest', 'storperf', 'vsperf', 'bottlenecks', 'qtip', 'yardstick']

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        return identities

    def has_identities(self):
        """ Return whether the enriched items contains identities """

        return False

    def get_field_author(self):
        # In Functest there is no identities support
        return None

    def __process_duration(self, start_date, stop_date):
        difference_in_seconds = None

        if not start_date or not stop_date:
            return difference_in_seconds

        difference_in_seconds = abs((stop_date - start_date).seconds)
        return difference_in_seconds

    def __process_duration_from_api(self, duration):
        processed_duration = None
        try:
            processed_duration = float(duration)
        except Exception:
            match = re.fullmatch(r'(\d+)m(\d+)s', duration)
            if match:
                minutes = float(match.group(1))
                seconds = float(match.group(2))
                total_secs = (60.0 * minutes) + seconds
                processed_duration = total_secs
                return processed_duration

            match = re.fullmatch(r'\d+:\d+:\d+(\.\d+)?', duration)
            if match:
                total_secs = sum(
                    [a * b for a, b in zip([3600.0, 60.0, 1.0], map(float, duration.split(':')))])
                processed_duration = total_secs
                return processed_duration

        return processed_duration

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)

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
                eitem['duration_from_api'] = self.__process_duration_from_api(func_test['details']['duration'])

                if eitem['duration_from_api'] is None:
                    logger.debug("[functest] Duration from api {} not processed for enriched item {}".format(
                                 func_test['details']['duration'], eitem))

            if 'start_date' in func_test and 'stop_date' in func_test and func_test['stop_date']:
                start_date = self.__convert_str_to_datetime(func_test['start_date'])
                stop_date = self.__convert_str_to_datetime(func_test['stop_date'])
                eitem['duration'] = self.__process_duration(start_date, stop_date)

                if eitem['duration'] is None:
                    logger.debug("[functest] Duration not calculated for enriched item {} with start_date,"
                                 " stop_date: {}, {}".format(eitem, start_date, stop_date))

        if 'duration_from_api' not in eitem:
            eitem['duration_from_api'] = None
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

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem

    @staticmethod
    def __convert_str_to_datetime(text):
        try:
            str_date = str_to_datetime(text)
        except Exception:
            try:
                str_date = unixtime_to_datetime(text)
            except Exception:
                str_date = None

        return str_date
