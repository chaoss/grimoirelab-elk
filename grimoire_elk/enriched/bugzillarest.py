# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2023 Bitergia
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#   Quan Zhou <quan@bitergia.com>
#

import json
import logging

from ..elastic_mapping import Mapping as BaseMapping
from .enrich import Enrich, metadata
from .utils import get_time_diff_days, anonymize_url

from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)


logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns: dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
               "main_description_analyzed": {
                    "type": "text",
                    "index": true
               },
               "summary_analyzed": {
                    "type": "text",
                    "index": true
               }
           }
        }"""

        return {"items": mapping}


class BugzillaRESTEnrich(Enrich):

    mapping = Mapping
    roles = ['assigned_to_detail', 'qa_contact_detail', 'creator_detail']
    comment_roles = ['author', 'creator']

    # Common fields in bugs and comments
    common_fields = [
        "bug_id",
        "status",
        "is_open",
        "product",
        "component",
        "version",
    ]

    def get_field_author(self):
        return 'creator_detail'

    def get_project_repository(self, eitem):
        return eitem['origin']

    def get_field_unique_id(self):
        return 'id'

    def get_identities(self, item):
        """ Return the identities from an item """

        # We guess the item is a bug and not a comment
        roles = self.roles if 'data' in item else self.comment_roles

        # The item is a bug
        for rol in roles:
            if rol in item['data']:
                yield self.get_sh_identity(item["data"][rol])

    def get_sh_identity(self, item, identity_field=None):
        identity = {
            'username': None,
            'email': None,
            'name': None,
        }

        user = item  # by default a specific user dict is used
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]
            identity['username'] = user['name'].split("@")[0] if user.get('name', None) else None
            identity['email'] = user.get('email', None)
            identity['name'] = user.get('real_name', None)
        elif identity_field:
            user = item[identity_field]
            identity['username'] = user.split("@")[0]
            identity['email'] = user
            identity['name'] = None

        return identity

    @metadata
    def get_rich_item(self, item, kind="bug", ebug=None):
        if kind == "bug":
            return self._enrich_bugzilla_bug(item)
        elif kind == "comment":
            return self._enrich_bugzilla_comment(item, ebug)
        else:
            logger.error(f"[bugzillarest] Invalid type for bugzilla item; kind={kind}")

    def _enrich_bugzilla_bug(self, item):
        """Enrich a Bugzilla bug item."""

        if 'id' not in item['data']:
            logger.warning("[bugzillarest] Dropped bug without bug_id {}".format(item))
            return None

        logger.debug(f"[bugzillarest] Enriching bug; bug_id={item['data']['id']}")

        eitem = {
            "type": "bug",
        }

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)

        # The real data
        issue = item['data']

        if "assigned_to_detail" in issue and "real_name" in issue["assigned_to_detail"]:
            eitem["assigned_to"] = issue["assigned_to_detail"]["real_name"]

        if "creator_detail" in issue and "real_name" in issue["creator_detail"]:
            eitem["creator"] = issue["creator_detail"]["real_name"]

        eitem["id"] = f"bug_{issue['id']}"
        eitem["bug_id"] = issue['id']

        eitem["status"] = issue['status']
        if "summary" in issue:
            eitem["summary"] = issue['summary'][:self.KEYWORD_MAX_LENGTH]
            # Share the name field with bugzilla and share the panel
            eitem["main_description"] = eitem["summary"]

            eitem["summary_analyzed"] = issue['summary']
            eitem["main_description_analyzed"] = issue['summary']

        # Component and product
        eitem["component"] = issue['component']
        eitem["product"] = issue['product']
        eitem["version"] = issue.get('version', None)

        # Keywords
        eitem["keywords"] = issue['keywords']

        # Fix dates
        date_ts = str_to_datetime(issue['creation_time'])
        eitem['creation_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')
        date_ts = str_to_datetime(issue['last_change_time'])
        eitem['changeddate_date'] = date_ts.isoformat()
        eitem['delta_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')

        # Add extra JSON fields used in Kibana (enriched fields)
        eitem['comments'] = 0
        eitem['last_comment_date'] = None
        eitem['number_of_comments'] = 0
        eitem['time_to_last_update_days'] = None
        eitem['time_to_first_attention'] = None
        eitem['url'] = None

        # Add the field to know if the ticket is open
        eitem['is_open'] = issue.get('is_open', None)

        if 'long_desc' in issue:
            eitem['number_of_comments'] = len(issue['long_desc'])

        if 'comments' in issue:
            eitem['comments'] = len(issue['comments'])

            last_comment_date = None

            if eitem['comments'] > 1:
                last_comment_date = str_to_datetime(issue['comments'][-1]['time'])
                last_comment_date = last_comment_date.isoformat()

            eitem['last_comment_date'] = last_comment_date

        eitem['url'] = item['origin'] + "/show_bug.cgi?id=" + str(issue['id'])
        eitem['time_to_last_update_days'] = \
            get_time_diff_days(eitem['creation_ts'], eitem['delta_ts'])

        eitem['timeopen_days'] = get_time_diff_days(eitem['creation_ts'], datetime_utcnow().replace(tzinfo=None))
        if 'is_open' in issue and not issue['is_open']:
            eitem['timeopen_days'] = eitem['time_to_last_update_days']

        eitem['time_to_first_attention'] = \
            get_time_diff_days(eitem['creation_ts'], self.get_time_to_first_attention(issue))

        eitem['changes'] = 0
        for history in issue['history']:
            if 'changes' in history:
                eitem['changes'] += len(history['changes'])

        if issue['whiteboard'] != "":
            eitem['whiteboard'] = issue['whiteboard']

        if self.sortinghat:
            eitem.update(self.get_item_sh(item, self.roles))
            # To reuse the name of the fields in Bugzilla and share the panel
            eitem['assigned_to_org_name'] = eitem['assigned_to_detail_org_name']
            eitem['assigned_to_uuid'] = eitem['assigned_to_detail_uuid']

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(issue['creation_time'], "bugrest"))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem

    def _enrich_bugzilla_comment(self, item, ebug):
        logger.debug(
            f"[bugzillarest] Enriching comment; bug_id={item['bug_id']}, comment_id={item['id']}"
        )

        eitem = {
            "type": "comment",
        }

        # Copy raw fields but update specific ones
        self.copy_raw_fields(self.RAW_FIELDS_COPY, ebug, eitem)
        eitem[self.get_field_date()] = str_to_datetime(item['creation_time']).isoformat()

        eitem["id"] = f"{ebug['id']}_comment_{item['id']}"
        eitem["text"] = item["text"]
        eitem["creator"] = item["creator"]

        # Copy common fields
        for field in self.common_fields:
            eitem[field] = ebug.get(field, None)

        eitem['url'] = f"{ebug['url']}#c{item['count']}"

        if self.prjs_map:
            # Find the project in the enriched bug
            eitem.update(self.get_item_project(ebug))

        # Set time values
        date_ts = str_to_datetime(item['creation_time'])
        eitem['creation_ts'] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')
        eitem.update(self.get_grimoire_fields(item['creation_time'], "bugrest"))

        if self.sortinghat:
            # Update original with missing values needed for SortingHat
            item[self.get_field_date()] = eitem[self.get_field_date()]
            item[self.get_field_author()] = eitem['creator']
            eitem.update(self.get_item_sh(item, self.comment_roles))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)

        return eitem

    def enrich_items(self, ocean_backend):
        """Enrich Bugzilla items (bugs and comments)."""

        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""
        total = 0

        url = self.elastic.get_bulk_url()

        logger.debug("[bugzillarest] Adding items to {} (in {} packs)".format(anonymize_url(url), max_items))

        items = ocean_backend.fetch()

        for item in items:
            if current >= max_items:
                total += self.elastic.safe_put_bulk(url, bulk_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (rich_item[self.get_field_unique_id()])
            bulk_json += data_json + "\n"  # Bulk document
            current += 1

            # Enrich comments
            if "comments" not in item["data"]:
                continue

            for comment in item['data']['comments'][1:]:
                rich_comment = self.get_rich_item(comment, kind="comment", ebug=rich_item)

                data_json = json.dumps(rich_comment)
                bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                    (rich_comment[self.get_field_unique_id()])
                bulk_json += data_json + "\n"  # Bulk document
                current += 1

        if current > 0:
            total += self.elastic.safe_put_bulk(url, bulk_json)

        return total

    def get_time_to_first_attention(self, item):
        """Set the time to first attention.

        This date is defined as the first date at which a comment by someone
        other than the user who created the issue.
        """
        if 'comments' not in item:
            return None

        comment_dates = []
        creator = item['creator']

        # First comment is the description of the issue
        # Real comments start at the second position (index 1)
        for comment in item['comments'][1:]:
            user = comment['creator']
            if user == creator:
                continue
            comment_dates.append(str_to_datetime(comment['time']).replace(tzinfo=None))

        if comment_dates:
            return min(comment_dates)
        else:
            return None
