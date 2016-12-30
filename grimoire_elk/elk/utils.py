#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Grimoire ELK general utils
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

import datetime
from dateutil import parser, tz

def get_time_diff_days(start, end):
    ''' Number of days between two dates in UTC format  '''

    if start is None or end is None:
        return None

    if type(start) is not datetime.datetime:
        start = parser.parse(start).replace(tzinfo=None)
    if type(end) is not datetime.datetime:
        end = parser.parse(end).replace(tzinfo=None)

    seconds_day = float(60*60*24)
    diff_days = \
        (end-start).total_seconds() / seconds_day
    diff_days = float('%.2f' % diff_days)

    return diff_days

# https://github.com/grimoirelab/perceval/blob/master/perceval/utils.py#L149
def unixtime_to_datetime(ut):
    """Convert a unixtime timestamp to a datetime object.
    The function converts a timestamp in Unix format to a
    datetime object. UTC timezone will also be set.
    :param ut: Unix timestamp to convert
    :returns: a datetime object
    :raises InvalidDateError: when the given timestamp cannot be
        converted into a valid date
    """

    dt = datetime.datetime.utcfromtimestamp(ut)
    dt = dt.replace(tzinfo=tz.tzutc())
    return dt
