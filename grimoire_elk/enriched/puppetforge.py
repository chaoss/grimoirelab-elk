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

from dateutil import parser

from .enrich import Enrich, metadata


class PuppetForgeEnrich(Enrich):

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "summary_analyzed": {
                  "type": "string",
                  "index":"analyzed"
                  }
           }
        } """

        return {"items": mapping}

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        user = self.get_sh_identity(item, self.get_field_author())
        identities.append(user)

        # Get the identities from the releases
        for release in item['data']['releases']:
            user = self.get_sh_identity(release['module'], self.get_field_author())
            identities.append(user)

        return identities

    def get_field_author(self):
        return 'owner'

    def get_sh_identity(self, item, identity_field=None):

        entry = item

        if 'data' in item and type(item) == dict:
            entry = item['data']

        identity = {f: None for f in ['email', 'name', 'username']}

        if identity_field in entry:
            identity['username'] = entry[identity_field]['username']

        return identity

    def get_field_event_unique_id(self):
        """ Field in the rich event with the unique id """
        return "uuid"

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        entry = item['data']

        # data fields to copy
        copy_fields = ["downloads", "uri", "name", "slug", "issues_url",
                       "homepage_url"]
        for f in copy_fields:
            if f in entry:
                eitem[f] = entry[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"feedback_score": "module_feedback_score",
                      "uri": "module_uri",
                      "name": "module_name",
                      "slug": "module_slug",
                      "downloads": "module_downloads"}
        for f in map_fields:
            if f in entry:
                eitem[map_fields[f]] = entry[f]

        eitem["is_release"] = 0
        eitem["type"] = "module"
        eitem["url"] = 'https://forge.puppet.com/' + entry['owner']['username'] + '/' + entry['name']
        eitem["author_url"] = 'https://forge.puppet.com/' + entry['owner']['username']
        eitem["gravatar_id"] = entry['owner']['gravatar_id']
        eitem["owner_release_count"] = entry['owner_data']['release_count']
        eitem["owner_module_count"] = entry['owner_data']['module_count']
        eitem["module_downloads"] = entry['downloads']
        eitem["release_count"] = len(entry['releases'])
        if 'current_release' in entry:
            eitem["module_version"] = entry['current_release']['version']
            eitem["version"] = entry['current_release']['version']
            eitem["validation_score"] = entry['current_release']['validation_score']
            eitem["tags"] = entry['current_release']['tags']
            eitem["license"] = entry['current_release']['metadata']['license']
            eitem["source_url"] = entry['current_release']['metadata']['source']
            eitem["summary"] = entry['current_release']['metadata']['summary']

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(entry["created_at"], "module"))

        return eitem

    def get_rich_events(self, item):
        """
        Get the enriched events related to a module
        """
        events = []
        module = item['data']

        for release in item['data']['releases']:
            event = self.get_rich_item(item)
            # Update specific fields for this release
            event["uuid"] += "_" + release['slug']
            event["author_url"] = 'https://forge.puppet.com/' + release['module']['owner']['username']
            event["gravatar_id"] = release['module']['owner']['gravatar_id']
            event["downloads"] = release['downloads']
            event["slug"] = release['slug']
            event["version"] = release['version']
            event["uri"] = release['uri']
            event["validation_score"] = release['validation_score']
            event["homepage_url"] = None
            if 'project_page' in release['metadata']:
                event["homepage_url"] = release['metadata']['project_page']
            event["issues_url"] = None
            if "issues_url" in release['metadata']:
                event["issues_url"] = release['metadata']['issues_url']
            event["tags"] = release['tags']
            event["license"] = release['metadata']['license']
            event["source_url"] = release['metadata']['source']
            event["summary"] = release['metadata']['summary']

            event["metadata__updated_on"] = parser.parse(release['updated_at']).isoformat()

            if self.sortinghat:
                release["metadata__updated_on"] = event["metadata__updated_on"]  # Needed in get_item_sh logic
                event.update(self.get_item_sh(release))

            if self.prjs_map:
                event.update(self.get_item_project(event))

            event.update(self.get_grimoire_fields(release["created_at"], "release"))

            events.append(event)

        return events

    def enrich_items(self, ocean_backend):
        total = super(PuppetForgeEnrich, self).enrich_items(ocean_backend)
        # Always generate also the events
        super(PuppetForgeEnrich, self).enrich_items(ocean_backend, events=True)

        return total  # return just the enriched items, not the events
