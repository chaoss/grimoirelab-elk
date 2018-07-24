# -*- coding: utf-8 -*-
#
# RSS Ocean feeder
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

from .elastic import ElasticOcean
from ..elastic_mapping import Mapping as BaseMapping


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        Non dynamic discovery of type for:
            * data.runs
            * data.actions

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = '''
         {
            "dynamic":true,
                "properties": {
                    "data": {
                        "properties": {
                            "_class" : {
                                "type" : "keyword"
                            },
                            "runs": {
                                "dynamic":false,
                                "properties": {}
                            },
                            "actions": {
                                "dynamic":false,
                                "properties": {}
                            },
                            "building" : {
                                "type" : "boolean"
                            },
                            "builtOn" : {
                                "type" : "keyword"
                            },
                            "changeSet": {
                                "properties": {
                                    "items": {
                                        "properties": {
                                            "comment": {
                                                "type": "text"
                                            }
                                        }
                                    }
                                }
                            },
                            "description" : {
                                "type" : "keyword"
                            },
                            "displayName" : {
                                "type" : "keyword"
                            },
                            "duration" : {
                                "type" : "long"
                            },
                            "estimatedDuration" : {
                                "type" : "long"
                            },
                            "executor" : {
                                "type" : "object"
                            },
                            "fullDisplayName" : {
                                "type" : "keyword"
                            },
                            "id" : {
                                "type" : "keyword"
                            },
                              "keepLog" : {
                                "type" : "boolean"
                            },
                              "number" : {
                                "type" : "long"
                            },
                            "queueId" : {
                                "type" : "long"
                            },
                            "result" : {
                                "type" : "keyword"
                            },
                            "subBuilds" : {
                                "type" : "object"
                            },
                            "timestamp" : {
                                "type" : "long"
                            },
                            "url" : {
                                "type" : "keyword"
                            }
                        }
                    }
                }
        }
        '''

        return {"items": mapping}


class JenkinsOcean(ElasticOcean):
    """Jenkins Ocean feeder"""

    mapping = Mapping

    @classmethod
    def get_p2o_params_from_url(cls, url):
        # Jenkins could include in the URL a jenkins-rename-file T1746
        params = {}

        tokens = url.split(' ', 1)  # Just split the URL not the filter
        params['url'] = tokens[0]

        if len(tokens) > 1:
            params['jenkins-rename-file'] = tokens[1].split(" ", 1)[1]

        return params

    @classmethod
    def get_perceval_params_from_url(cls, url):
        params = []
        tokens = url.split(' ', 1)  # Just split the URL not jenkins-rename-file
        url = tokens[0]
        params.append(url)

        return params
