# -*- coding: utf-8 -*-
#
# Copyright (C) Bitergia
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

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = {
            "dynamic": True,
            "properties": {
                "data": {
                    "properties": {
                        "user": {
                            "properties": {
                                "entities": {
                                    "dynamic": False,
                                    "properties": {}
                                }
                            }
                        },
                        "hashtags": {
                            "properties": {
                                "indices": {
                                    "dynamic": False,
                                    "properties": {}
                                }
                            }
                        },
                        "metadata": {
                            "dynamic": False,
                            "properties": {}
                        },
                        "entities": {
                            "properties": {
                                "media": {
                                    "dynamic": False,
                                    "properties": {}
                                },
                                "symbols": {
                                    "dynamic": False,
                                    "properties": {}
                                },
                                "urls": {
                                    "dynamic": False,
                                    "properties": {}
                                },
                                "user_mentions": {
                                    "dynamic": False,
                                    "properties": {}
                                }
                            }
                        },
                        "extended_entities": {
                            "dynamic": False,
                            "properties": {}
                        },
                        "extended_tweet": {
                            "dynamic": False,
                            "properties": {}
                        },
                        "quoted_status": {
                            "dynamic": False,
                            "properties": {}
                        },
                        "retweeted_status": {
                            "dynamic": False,
                            "properties": {}
                        }
                    }
                }
            }
        }

        return {"items": mapping}


class TwitterOcean(ElasticOcean):
    """Twitter Ocean feeder"""

    mapping = Mapping

    @classmethod
    def get_arthur_params_from_url(cls, query):
        # The query is directly passed to the Twitter API

        params = {"query": query}

        return params
