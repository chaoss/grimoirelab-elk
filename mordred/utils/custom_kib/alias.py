#!/usr/bin/env python3

# -*- coding: utf-8 -*-
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
#     David Pose Fernández <dpose@bitergia.com>
#


# imports
import io
import http


# get the list of aliases from an instance of elasticsearch
def get_alias(es_url):
    response = io.BytesIO()
    response = http.get(es_url, response)
    value = response.getvalue().decode('UTF-8')
    response.close()
    print("\nList of aliases (" + es_url.split("@")[1].split("/_cat")[0] + "):\n" + value)

# add a new alias to elasticsearch
def add_alias(es_url, alias, index):
    value = '{"actions" : [{ "add" : { "index" : "' + index + '", "alias" : "' + alias + '" } }]}'
    http.post(es_url, value, "\nThe alias below has been added successfully:\n\
alias: " + alias + " --> " + "index: " + index + "\n")

# replace the index where an existing alias points to
def replace_alias(es_url, alias, oldindex, newindex):
    value = '{"actions" : [{ "remove" : { "index" : "' + oldindex + '", "alias" : "' + alias + '" } },{ "add" : { "index" : "' + newindex + '", "alias" : "' + alias + '" } }]}'
    http.post(es_url, value, "\nThe alias below has been replaced successfully:\n" + alias + " -/-> " + oldindex + "\n" + alias + " --> " + newindex + "\n")

# remove an alias from elasticsearch
def remove_alias(es_url, alias, index):
    value = '{"actions" : [{ "remove" : { "index" : "' + index + '", "alias" : "' + alias + '" } }]}'
    http.post(es_url, value, "\nThe alias below has been removed successfully:\n" + alias + " -/-> " + index + "\n")
