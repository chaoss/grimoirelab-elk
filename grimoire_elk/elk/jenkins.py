#!/usr/bin/python3
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

import json
import logging

from dateutil import parser

from .enrich import Enrich, metadata

class JenkinsEnrich(Enrich):

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "fullDisplayName_analyzed": {
                  "type": "string",
                  "index":"analyzed"
                  }
           }
        } """

        return {"items":mapping}


    def get_field_author(self):
        # In Jenkins there are no identities support
        return None

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        return identities

    def get_fields_from_job_name(self, job_name):
        """Analyze a Jenkins job name, producing a dictionary

        The produced dictionary will include information about the category
        and subcategory of the job name, and any extra information which
        could be useful.

        For each deployment of a Jenkins dashboard, an implementation of
        this function should be produced, according to the needs of the users.

        :param job: job name to Analyze
        :returns:   dictionary with categorization information

        """

        extra_fields = {
            'category' : None,
            'installer' : None,
            'scenario' : None,
            'testproject' : None,
            'pod' : None,
            'loop' : None,
            'branch' : None
        }

        try:
            components = job_name.split('-')

            if len(components) < 2:
                return extra_fields

            kind = components[1]
            if kind == 'os':
                extra_fields['category'] = 'parent/main'
                extra_fields['installer'] = components[0]
                extra_fields['scenario'] = '-'.join(components[2:-3])
            elif kind == 'deploy':
                extra_fields['category'] = 'deploy'
                extra_fields['installer'] = components[0]
            else:
                extra_fields['category'] = 'test'
                extra_fields['testproject'] = components[0]
                extra_fields['installer'] = components[1]

            extra_fields['pod'] = components[-3]
            extra_fields['loop'] = components[-2]
            extra_fields['branch'] = components[-1]
        except IndexError as ex:
            logging.error('Problems parsing job name %s', job_name)
            logging.error(ex)

        return extra_fields

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        # metadata fields to copy
        copy_fields = ["metadata__updated_on","metadata__timestamp","ocean-unique-id","origin"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        build = item['data']

        # data fields to copy
        copy_fields = ["fullDisplayName","url","result","duration","builtOn"]
        for f in copy_fields:
            if f in build:
                eitem[f] = build[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"fullDisplayName": "fullDisplayName_analyzed",
                      "number": "build"
                      }
        for fn in map_fields:
            eitem[map_fields[fn]] = build[fn]

        # Job url: remove the last /build_id from job_url/build_id/
        eitem['job_url'] = eitem['url'].rsplit("/", 2)[0]
        eitem['job_name'] = eitem['url'].rsplit('/', 3)[1]
        eitem['job_build'] = eitem['job_name']+'/'+str(eitem['build'])

        # Enrich dates
        eitem["build_date"] = parser.parse(item["metadata__updated_on"]).isoformat()

        # Add duration in days
        if "duration" in eitem:
            seconds_day = float(60*60*24)
            duration_days = eitem["duration"]/(1000*seconds_day)
            eitem["duration_days"] = float('%.2f' % duration_days)

        # Add extra fields extracted from job name
        eitem.update(self.get_fields_from_job_name(eitem['job_name']))

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "job"))

        return eitem
