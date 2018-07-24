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
import sys

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
                "description": {
                    "type": "text",
                    "index": true
                },
                "description_analyzed": {
                    "type": "text",
                    "index": true
                },
                "full_description_analyzed": {
                    "type": "text",
                    "index": true
                }
           }
        }
        """

        return {"items": mapping}


class DockerHubEnrich(Enrich):

    mapping = Mapping

    def get_field_author(self):
        return "nick"

    def get_identities(self, item):
        """ Return the identities from an item """
        # In DockerHub there are no identities. Just the organization and
        # the repository name for the docker image
        identities = []
        return identities

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        image = item['data']

        # data fields to copy
        copy_fields = ["affiliation", "build_on_cloud", "description",
                       "is_automated", "is_private", "pull_count", "repository_type",
                       "star_count", "status", "user"]
        for f in copy_fields:
            if f in image:
                eitem[f] = image[f]
            else:
                eitem[f] = None

        # Fields which names are translated
        map_fields = {}
        for fn in map_fields:
            eitem[map_fields[fn]] = image[fn]

        eitem["id"] = image["name"] + '-' + image["namespace"]
        eitem['is_event'] = 1
        eitem['is_docker_image'] = 0

        eitem['last_updated'] = image['last_updated']
        eitem['description_analyzed'] = image['description']
        eitem['full_description_analyzed'] = image['description']

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "dockerhub"))

        return eitem

    def enrich_items(self, ocean_backend, events=False):
        """ A custom enrich items is needed because apart from the enriched
        events from raw items, a image item with the last data for an image
        must be created """

        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        items = ocean_backend.fetch()
        images_items = {}

        url = self.elastic.index_url + '/items/_bulk'

        logger.debug("Adding items to %s (in %i packs)", url, max_items)

        for item in items:
            if current >= max_items:
                total += self.elastic.safe_put_bulk(url, bulk_json)
                json_size = sys.getsizeof(bulk_json) / (1024 * 1024)
                logger.debug("Added %i items to %s (%0.2f MB)", total, url, json_size)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (item[self.get_field_unique_id()])
            bulk_json += data_json + "\n"  # Bulk document
            current += 1

            if rich_item['id'] not in images_items:
                # Let's transform the rich_event in a rich_image
                rich_item['is_docker_image'] = 1
                rich_item['is_event'] = 0
                images_items[rich_item['id']] = rich_item
            else:
                image_date = images_items[rich_item['id']]['last_updated']
                if image_date <= rich_item['last_updated']:
                    # This event is newer for the image
                    rich_item['is_docker_image'] = 1
                    rich_item['is_event'] = 0
                    images_items[rich_item['id']] = rich_item

        if current > 0:
            total += self.elastic.safe_put_bulk(url, bulk_json)

        if total == 0:
            # No items enriched, nothing to upload to ES
            return total

        # Time to upload the images enriched items. The id is uuid+"_image"
        # Normally we are enriching events for a unique image so all images
        # data can be upload in one query
        for image in images_items:
            data = images_items[image]
            data_json = json.dumps(data)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (data['id'] + "_image")
            bulk_json += data_json + "\n"  # Bulk document

        total += self.elastic.safe_put_bulk(url, bulk_json)
        return total
