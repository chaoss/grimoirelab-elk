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

import csv
import logging

from dateutil import parser

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping


logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
                "fullDisplayName_analyzed": {
                    "type": "text",
                    "index": true
                }
           }
        } """

        return {"items": mapping}


class JenkinsEnrich(Enrich):

    mapping = Mapping

    MAIN_NODE_NAME = "main"

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)
        self.nodes_rename_file = None
        self.nodes_rename = {}

    def set_jenkins_rename_file(self, nodes_rename_file):
        """ File with nodes renaming mapping:

        Node,Comment
        arm-build1,remove
        arm-build2,keep
        ericsson-build3,merge into ericsson-build1
        ....

        Once set in the next enrichment the rename will be done
        """
        self.nodes_rename_file = nodes_rename_file
        self.__load_node_renames()
        logger.info("Jenkis node rename file active: %s", nodes_rename_file)

    def __load_node_renames(self):
        # In OPNFV nodes could be renamed
        if not self.nodes_rename_file:
            logger.debug("Jenkis node rename file not defined.")
            return
        try:
            with open(self.nodes_rename_file, 'r') as csvfile:
                nodes = csv.reader(csvfile, delimiter=',')
                for node in nodes:
                    name = node[0]
                    action = node[1]
                    rename = action.split("merge into ")
                    if len(rename) > 1:
                        self.nodes_rename[name] = rename[1]
                logger.debug("Total node renames: %i", len(self.nodes_rename.keys()))
        except FileNotFoundError:
            logger.info("Jenkis node rename file not found %s",
                        self.nodes_rename_file)

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
            'category': None,
            'installer': None,
            'scenario': None,
            'testproject': None,
            'pod': None,
            'loop': None,
            'branch': None
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
            # Just DEBUG level because it is just for OPNFV
            logger.debug('Problems parsing job name %s', job_name)
            logger.debug(ex)

        return extra_fields

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        build = item['data']

        # data fields to copy
        copy_fields = ["fullDisplayName", "url", "result", "duration", "builtOn"]
        for f in copy_fields:
            if f in build:
                eitem[f] = build[f]
            else:
                eitem[f] = None
        # main node names
        if not eitem["builtOn"]:
            eitem["builtOn"] = self.MAIN_NODE_NAME
        # Nodes renaming
        if eitem["builtOn"] in self.nodes_rename:
            eitem["builtOn"] = self.nodes_rename[eitem["builtOn"]]
        # Fields which names are translated
        map_fields = {"fullDisplayName": "fullDisplayName_analyzed",
                      "number": "build"
                      }
        for fn in map_fields:
            eitem[map_fields[fn]] = build[fn]

        # Job url: remove the last /build_id from job_url/build_id/
        eitem['job_url'] = eitem['url'].rsplit("/", 2)[0]
        eitem['job_name'] = eitem['url'].rsplit('/', 3)[1]
        eitem['job_build'] = eitem['job_name'] + '/' + str(eitem['build'])

        # Enrich dates
        eitem["build_date"] = parser.parse(item["metadata__updated_on"]).isoformat()

        # Add duration in days
        if "duration" in eitem:
            seconds_day = float(60 * 60 * 24)
            duration_days = eitem["duration"] / (1000 * seconds_day)
            eitem["duration_days"] = float('%.2f' % duration_days)

        # Add extra fields extracted from job name
        eitem.update(self.get_fields_from_job_name(eitem['job_name']))

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "job"))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        return eitem
