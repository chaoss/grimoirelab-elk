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
import pycurl


# function GET
def get(es_url, response):
    c = pycurl.Curl()
    c.setopt(pycurl.URL, es_url)
    c.setopt(c.SSL_VERIFYPEER, 0)
    c.setopt(c.SSL_VERIFYHOST, 0)
    c.setopt(pycurl.HTTPGET, 1)
    c.setopt(c.WRITEFUNCTION, response.write)
    c.perform()

    return response

# function POST
def post(es_url, value, msg):
    c = pycurl.Curl()
    c.setopt(pycurl.URL, es_url)
    c.setopt(c.SSL_VERIFYPEER, 0)
    c.setopt(c.SSL_VERIFYHOST, 0)
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.POSTFIELDS, value)
    c.perform()

    if c.getinfo(c.RESPONSE_CODE) == 200:
        print(msg + value)
    else:
        print("\nFAILED! ERROR CODE: " + str(c.getinfo(c.RESPONSE_CODE)))

# function DELETE
def delete(es_url, value, msg):
    c = pycurl.Curl()
    c.setopt(pycurl.URL, es_url + "/" + value)
    c.setopt(c.SSL_VERIFYPEER, 0)
    c.setopt(c.SSL_VERIFYHOST, 0)
    c.setopt(pycurl.CUSTOMREQUEST, "DELETE")
    c.perform()

    if c.getinfo(c.RESPONSE_CODE) == 200:
        print(msg + value)
    else:
        print("\nFAILED! ERROR CODE: " + str(c.getinfo(c.RESPONSE_CODE)))
