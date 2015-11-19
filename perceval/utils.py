#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Perceval utils library 
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
# Misc utils used in Perceval backends

import logging
from datetime import datetime
from dateutil import parser
from os import SEEK_END

def get_eta(last_update_date, prj_first_date, prj_last_date):
    ''' Get the time needed to analyze a day in the project and multiply
    for the number of days in the project '''

    if last_update_date == prj_first_date:
        logging.error("Data error: first date and last update the same.")
        return None

    proj_total_days = (prj_last_date - prj_first_date).total_seconds() / (60*60*24)

    app_spend_time_sec = (datetime.now() - prj_last_date).total_seconds()
    prj_seconds_done = (last_update_date-prj_first_date).total_seconds()
    prj_days_done = prj_seconds_done / (60*60*24)

    # Number of seconds needed to analyze a project day
    app_sec_per_proj_day = app_spend_time_sec/prj_days_done

    prj_pending_days = proj_total_days - prj_days_done
    pending_eta_min = (app_sec_per_proj_day * prj_pending_days)/60

    return pending_eta_min

def get_time_diff_days(start_txt, end_txt):
    ''' Number of days between two days  '''

    if start_txt is None or end_txt is None:
        return None

    start = parser.parse(start_txt)
    end = parser.parse(end_txt)

    seconds_day = float(60*60*24)
    diff_days = \
        (end-start).total_seconds() / seconds_day
    diff_days = float('%.2f' % diff_days)

    return diff_days

def remove_last_char_from_file(fname):
    ''' Remove last char from a file '''
    with open(fname, 'rb+') as f:
        f.seek(-1, SEEK_END)
        f.truncate()






