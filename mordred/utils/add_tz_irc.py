#!/usr/bin/python3
# -*- coding: utf-8 -*-

#######################################################
# Script to add timezones to the timestamp.           #
# Input: log file you want to parse and the timezone  #
# Output: the same log file with the new timestamp    #
#######################################################

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
#   David Pose Fernández <dpose@bitergia.com>
#


import argparse
import re


# ArgumentParser
parser = parser = argparse.ArgumentParser(description="Script to change date format from 2016-01-01T00:00:00 to 2016-01-01T:00:00:00+0100. It needs the log file which will be changed and the timezone. The output is the same log file with the new timestamp.")
parser.add_argument("file", help="Input file")
parser.add_argument("timezone", help="Timezone e.g.: +0000, +0200, -0100")
args = parser.parse_args()


if args.file:
    regex = re.compile("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    logfile = open(args.file, 'r')
    lines = logfile.read()
    logfile.close()

    logfile = open(args.file, 'w')
    for line in lines.split("\n"):
        match = re.search(regex, line)
        if (match):
            line = re.sub(regex, match.group()+args.timezone, line.rstrip())
        logfile.write(line+"\n")
    logfile.close()
