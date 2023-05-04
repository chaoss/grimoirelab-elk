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
#   Miguel Ángel Fernández <mafesan@bitergia.com>
#

import logging

from .enrich import Enrich, metadata
from .utils import get_time_diff_days
from ..elastic_mapping import Mapping as BaseMapping

from grimoirelab_toolkit.datetime import (str_to_datetime,
                                          datetime_utcnow,
                                          unixtime_to_datetime)


MAX_SIZE_BULK_ENRICHED_ITEMS = 200
REVIEW_TYPE = 'review'
CHANGESET_TYPE = 'changeset'
COMMENT_TYPE = 'comment'
PATCHSET_TYPE = 'patchset'
APPROVAL_TYPE = 'approval'

CODE_REVIEW_TYPE = 'Code-Review'
VERIFIED_TYPE = 'Verified'

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
               "approval_description_analyzed": {
                  "type": "text",
                  "index": true
               },
               "comment_message_analyzed": {
                  "type": "text",
                  "index": true
               },
               "status": {
                  "type": "keyword"
               },
               "summary_analyzed": {
                  "type": "text",
                  "index": true
               },
               "timeopen": {
                  "type": "double"
               }
            }
        }
        """

        return {"items": mapping}


class GerritEnrich(Enrich):

    mapping = Mapping

    def __init__(self, db_sortinghat=None, json_projects_map=None,
                 db_user='', db_password='', db_host='', db_path=None,
                 db_port=None, db_ssl=False, db_verify_ssl=True, db_tenant=None):
        super().__init__(db_sortinghat=db_sortinghat, json_projects_map=json_projects_map,
                         db_user=db_user, db_password=db_password, db_host=db_host,
                         db_port=db_port, db_path=db_path, db_ssl=db_ssl, db_verify_ssl=db_verify_ssl,
                         db_tenant=db_tenant)

        self.studies = []
        self.studies.append(self.enrich_demography)
        self.studies.append(self.enrich_demography_contribution)
        self.studies.append(self.enrich_onion)

    roles = ["author", "by", "changeset_author", "reviewer", "uploader"]

    def get_field_author(self):
        return "owner"

    def get_sh_identity(self, item, identity_field=None):
        identity = {
            'name': None,
            'username': None,
            'email': None
        }

        if not item:
            return identity

        user = item  # by default a specific user dict is expected
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]
        elif identity_field:
            user = item[identity_field]

        identity['name'] = user.get('name', None)
        identity['email'] = user.get('email', None)
        identity['username'] = user.get('username', None)

        return identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        repo += "_" + eitem['repository']
        return repo

    def get_identities(self, item):
        """Return the identities from an item"""

        item = item['data']

        # Changeset owner
        user = item.get('owner', None)
        identity = self.get_sh_identity(user)
        yield identity

        # Patchset uploader and author
        if 'patchSets' in item:
            for patchset in item['patchSets']:
                if 'uploader' in patchset:
                    user = patchset.get('uploader', None)
                    identity = self.get_sh_identity(user)
                    yield identity
                if 'author' in patchset:
                    user = patchset.get('author', None)
                    identity = self.get_sh_identity(user)
                    yield identity
                if 'approvals' in patchset:
                    # Approvals by
                    for approval in patchset['approvals']:
                        if 'by' in approval:
                            identity = self.get_sh_identity(approval.get('by', None))
                            yield identity

        # Comments reviewers
        if 'comments' in item:
            for comment in item['comments']:
                if 'reviewer' in comment:
                    user = comment.get('reviewer', None)
                    identity = self.get_sh_identity(user)
                    yield identity

    def get_item_id(self, eitem):
        """ Return the item_id linked to this enriched eitem """

        # The eitem _id includes also the patch.
        return eitem["_source"]["review_id"]

    def _fix_review_dates(self, item):
        """Convert dates so ES detect them"""

        for date_field in ['timestamp', 'createdOn', 'lastUpdated']:
            if date_field in item.keys():
                date_ts = item[date_field]
                item[date_field] = unixtime_to_datetime(date_ts).isoformat()

        if 'patchSets' in item.keys():
            for patch in item['patchSets']:
                pdate_ts = patch['createdOn']
                patch['createdOn'] = unixtime_to_datetime(pdate_ts).isoformat()

                if 'approvals' in patch:
                    for approval in patch['approvals']:
                        adate_ts = approval['grantedOn']
                        approval['grantedOn'] = unixtime_to_datetime(adate_ts).isoformat()

        if 'comments' in item.keys():
            for comment in item['comments']:
                cdate_ts = comment['timestamp']
                comment['timestamp'] = unixtime_to_datetime(cdate_ts).isoformat()

    @metadata
    def get_rich_item(self, item):
        eitem = {}  # Item enriched

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)
        eitem['closed'] = item['metadata__updated_on']
        # The real data
        review = item['data']
        self._fix_review_dates(review)

        # data fields to copy
        copy_fields = ["status", "branch", "url"]
        for f in copy_fields:
            eitem[f] = review[f]
        # Fields which names are translated
        map_fields = {"subject": "summary",
                      "id": "githash",
                      "createdOn": "opened",
                      "project": "repository",
                      "number": "changeset_number"
                      }
        for fn in map_fields:
            eitem[map_fields[fn]] = review[fn]

        # Add id info to allow to coexistence of items of different types in the same index
        eitem['id'] = '{}_changeset_{}'.format(eitem['uuid'], eitem['changeset_number'])
        eitem["summary_analyzed"] = eitem["summary"]
        eitem["summary"] = eitem["summary"][:self.KEYWORD_MAX_LENGTH]
        eitem["name"] = None
        eitem["domain"] = None
        if 'owner' in review and 'name' in review['owner']:
            eitem["name"] = review['owner']['name']
            if 'email' in review['owner']:
                if '@' in review['owner']['email']:
                    eitem["domain"] = review['owner']['email'].split("@")[1]
        # New fields generated for enrichment
        patchsets = review['patchSets']
        eitem["patchsets"] = len(patchsets)

        # Time to add the time diffs
        created_on = review['createdOn']
        if len(patchsets) > 0:
            created_on = patchsets[0]['createdOn']

        status_value_code_review, status_value_verified = self.last_changeset_approval_value(patchsets)
        eitem['status_value'] = status_value_code_review
        eitem['changeset_status_value'] = eitem['status_value']
        eitem['changeset_status_value_verified'] = status_value_verified
        eitem['changeset_status'] = eitem['status']

        created_on_date = str_to_datetime(created_on)
        eitem["created_on"] = created_on

        time_first_review = self.get_time_first_review(review)
        eitem['time_to_first_review'] = get_time_diff_days(created_on, time_first_review)

        eitem["last_updated"] = review['lastUpdated']
        last_updated_date = str_to_datetime(review['lastUpdated'])

        seconds_day = float(60 * 60 * 24)
        if eitem['status'] in ['MERGED', 'ABANDONED']:
            timeopen = \
                (last_updated_date - created_on_date).total_seconds() / seconds_day
        else:
            timeopen = \
                (datetime_utcnow() - created_on_date).total_seconds() / seconds_day
        eitem["timeopen"] = '%.2f' % timeopen

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

            # add changeset author
            self.add_changeset_author(eitem, eitem, rol='owner')

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem['type'] = CHANGESET_TYPE
        # common field to count all items related to a review
        eitem.update(self.get_grimoire_fields(review['createdOn'], REVIEW_TYPE))
        # specific field to count changeset review
        eitem.update(self.get_grimoire_fields(review['createdOn'], CHANGESET_TYPE))

        eitem['wip'] = review.get('wip', False)
        eitem['open'] = review.get('open', None)

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem

    def get_rich_item_comments(self, comments, eitem):
        ecomments = []

        for comment in comments:
            ecomment = {}

            for f in self.RAW_FIELDS_COPY:
                ecomment[f] = eitem[f]

            # Copy data from the enriched review
            ecomment['wip'] = eitem['wip']
            ecomment['open'] = eitem['open']
            ecomment['url'] = eitem['url']
            ecomment['summary'] = eitem['summary']
            ecomment['repository'] = eitem['repository']
            ecomment['branch'] = eitem['branch']
            ecomment['changeset_number'] = eitem['changeset_number']

            # Add reviewer info
            ecomment["reviewer_name"] = None
            ecomment["reviewer_domain"] = None
            if 'reviewer' in comment and 'name' in comment['reviewer']:
                ecomment["reviewer_name"] = comment['reviewer']['name']
                if 'email' in comment['reviewer']:
                    if '@' in comment['reviewer']['email']:
                        ecomment["reviewer_domain"] = comment['reviewer']['email'].split("@")[1]

            # Add comment-specific data
            created = str_to_datetime(comment['timestamp'])
            ecomment['comment_created_on'] = created.isoformat()
            ecomment['comment_message'] = comment['message'][:self.KEYWORD_MAX_LENGTH]
            ecomment['comment_message_analyzed'] = comment['message']

            # Add id info to allow to coexistence of items of different types in the same index
            ecomment['type'] = COMMENT_TYPE
            ecomment['id'] = '{}_comment_{}'.format(eitem['id'], created.timestamp())

            if self.sortinghat:
                if 'reviewer' in comment:
                    ecomment.update(self.get_item_sh(comment, ['reviewer'], 'timestamp'))

                    ecomment['author_id'] = ecomment['reviewer_id']
                    ecomment['author_uuid'] = ecomment['reviewer_uuid']
                    ecomment['author_name'] = ecomment['reviewer_name']
                    ecomment['author_user_name'] = ecomment['reviewer_user_name']
                    ecomment['author_domain'] = ecomment['reviewer_domain']
                    ecomment['author_gender'] = ecomment['reviewer_gender']
                    ecomment['author_gender_acc'] = ecomment['reviewer_gender_acc']
                    ecomment['author_org_name'] = ecomment['reviewer_org_name']
                    ecomment['author_bot'] = ecomment['reviewer_bot']

                    ecomment['author_multi_org_names'] = ecomment.get('reviewer_multi_org_names', [])

                # add changeset author
                self.add_changeset_author(eitem, ecomment)

            if self.prjs_map:
                ecomment.update(self.get_item_project(ecomment))

            # common field to count all items related to a review
            ecomment.update(self.get_grimoire_fields(comment['timestamp'], REVIEW_TYPE))
            # specific field to count comment review
            ecomment.update(self.get_grimoire_fields(comment['timestamp'], COMMENT_TYPE))

            self.add_repository_labels(ecomment)
            self.add_metadata_filter_raw(ecomment)
            self.add_gelk_metadata(ecomment)

            ecomments.append(ecomment)

        return ecomments

    def get_rich_item_patchsets(self, patchsets, eitem):
        eitems = []

        for patchset in patchsets:
            epatchset = {}

            for f in self.RAW_FIELDS_COPY:
                epatchset[f] = eitem[f]

            # Copy data from the enriched review
            epatchset['wip'] = eitem['wip']
            epatchset['open'] = eitem['open']
            epatchset['url'] = eitem['url']
            epatchset['summary'] = eitem['summary']
            epatchset['repository'] = eitem['repository']
            epatchset['branch'] = eitem['branch']
            epatchset['changeset_number'] = eitem['changeset_number']
            epatchset['changeset_status'] = eitem['changeset_status']
            epatchset['changeset_status_value'] = eitem['changeset_status_value']
            epatchset['changeset_status_value_verified'] = eitem['changeset_status_value_verified']

            # Add author info
            epatchset["patchset_author_name"] = None
            epatchset["patchset_author_domain"] = None
            if 'author' in patchset and 'name' in patchset['author']:
                epatchset["patchset_author_name"] = patchset['author']['name']
                if 'email' in patchset['author']:
                    if '@' in patchset['author']['email']:
                        epatchset["patchset_author_domain"] = patchset['author']['email'].split("@")[1]

            # Add uploader info
            epatchset["patchset_uploader_name"] = None
            epatchset["patchset_uploader_domain"] = None
            if 'uploader' in patchset and 'name' in patchset['uploader']:
                epatchset["patchset_uploader_name"] = patchset['uploader']['name']
                if 'email' in patchset['uploader']:
                    if '@' in patchset['uploader']['email']:
                        epatchset["patchset_uploader_domain"] = patchset['uploader']['email'].split("@")[1]

            # Add patchset-specific data
            created = str_to_datetime(patchset['createdOn'])
            epatchset['patchset_created_on'] = created.isoformat()
            epatchset['patchset_number'] = patchset['number']
            epatchset['patchset_isDraft'] = patchset.get('isDraft', None)
            epatchset['patchset_kind'] = patchset.get('kind', None)
            epatchset['patchset_ref'] = patchset.get('ref', None)
            epatchset['patchset_revision'] = patchset.get('revision', None)
            epatchset['patchset_sizeDeletions'] = patchset.get('sizeDeletions', None)
            epatchset['patchset_sizeInsertions'] = patchset.get('sizeInsertions', None)

            time_first_review = self.get_time_first_review_patchset(patchset)
            epatchset['patchset_time_to_first_review'] = get_time_diff_days(epatchset['patchset_created_on'],
                                                                            time_first_review)

            # Add id info to allow to coexistence of items of different types in the same index
            epatchset['type'] = PATCHSET_TYPE
            epatchset['id'] = '{}_patchset_{}'.format(eitem['id'], epatchset['patchset_number'])

            if self.sortinghat:
                epatchset.update(self.get_item_sh(patchset, ['author', 'uploader'], 'createdOn'))

                # add changeset author
                self.add_changeset_author(eitem, epatchset)

            if self.prjs_map:
                epatchset.update(self.get_item_project(epatchset))

            # common field to count all items related to a review
            epatchset.update(self.get_grimoire_fields(patchset['createdOn'], REVIEW_TYPE))
            # specific field to count patchset review
            epatchset.update(self.get_grimoire_fields(patchset['createdOn'], PATCHSET_TYPE))

            self.add_repository_labels(epatchset)
            self.add_metadata_filter_raw(epatchset)
            self.add_gelk_metadata(epatchset)

            eitems.append(epatchset)

            # Add approvals as enriched items
            approvals = patchset.get('approvals', [])
            if approvals:
                epatcheset_approvals = self.get_rich_item_patchset_approvals(approvals, epatchset)
                eitems.extend(epatcheset_approvals)

        return eitems

    def get_rich_item_patchset_approvals(self, approvals, epatchset):
        eapprovals = []

        for approval in approvals:
            eapproval = {}

            for f in self.RAW_FIELDS_COPY:
                eapproval[f] = epatchset[f]

            # Copy data from the enriched patchset
            eapproval['wip'] = epatchset['wip']
            eapproval['open'] = epatchset['open']
            eapproval['url'] = epatchset['url']
            eapproval['summary'] = epatchset['summary']
            eapproval['repository'] = epatchset['repository']
            eapproval['branch'] = epatchset['branch']
            eapproval['changeset_number'] = epatchset['changeset_number']
            eapproval['changeset_status'] = epatchset['changeset_status']
            eapproval['changeset_status_value'] = epatchset['changeset_status_value']
            eapproval['changeset_status_value_verified'] = epatchset['changeset_status_value_verified']
            eapproval['patchset_number'] = epatchset['patchset_number']
            eapproval['patchset_revision'] = epatchset['patchset_revision']
            eapproval['patchset_ref'] = epatchset['patchset_ref']

            # Add author info
            eapproval["approval_author_name"] = None
            eapproval["approval_author_domain"] = None
            if 'by' in approval and 'name' in approval['by']:
                eapproval["approval_author_name"] = approval['by']['name']
                if 'email' in approval['by']:
                    if '@' in approval['by']['email']:
                        eapproval["approval_author_domain"] = approval['by']['email'].split("@")[1]

            # Add approval-specific data
            created = str_to_datetime(approval['grantedOn'])
            eapproval['approval_granted_on'] = created.isoformat()
            eapproval['approval_value'] = approval.get('value', None)
            eapproval['approval_type'] = approval.get('type', None)
            eapproval['approval_description'] = approval.get('description', None)

            if eapproval['approval_description']:
                eapproval['approval_description'] = eapproval['approval_description'][:self.KEYWORD_MAX_LENGTH]
                eapproval['approval_description_analyzed'] = eapproval['approval_description']

            # Add id info to allow to coexistence of items of different types in the same index
            eapproval['id'] = '{}_approval_{}'.format(epatchset['id'], created.timestamp())
            eapproval['type'] = APPROVAL_TYPE

            if self.sortinghat:
                eapproval.update(self.get_item_sh(approval, ['by'], 'grantedOn'))

                eapproval['author_id'] = eapproval.get('by_id', None)
                eapproval['author_uuid'] = eapproval.get('by_uuid', None)
                eapproval['author_name'] = eapproval.get('by_name', None)
                eapproval['author_user_name'] = eapproval.get('by_name', None)
                eapproval['author_domain'] = eapproval.get('by_domain', None)
                eapproval['author_gender'] = eapproval.get('by_gender', None)
                eapproval['author_gender_acc'] = eapproval.get('by_gender_acc', None)
                eapproval['author_org_name'] = eapproval.get('by_org_name', None)
                eapproval['author_bot'] = eapproval.get('by_bot', None)

                eapproval['author_multi_org_names'] = eapproval.get('by_multi_org_names', [])

                # add changeset author
                self.add_changeset_author(epatchset, eapproval)

            if self.prjs_map:
                eapproval.update(self.get_item_project(eapproval))

            # common field to count all items related to a review
            eapproval.update(self.get_grimoire_fields(approval['grantedOn'], REVIEW_TYPE))
            # specific field to count approval review
            eapproval.update(self.get_grimoire_fields(approval['grantedOn'], APPROVAL_TYPE))

            self.add_repository_labels(eapproval)
            self.add_metadata_filter_raw(eapproval)
            self.add_gelk_metadata(eapproval)

            eapprovals.append(eapproval)

        return eapprovals

    def add_changeset_author(self, source_eitem, target_eitem, rol='changeset_author'):
        """Copy SH changeset author info in `source_eitem` to `target_eitem`"""

        target_eitem['changeset_author_id'] = source_eitem.get(rol + '_id', None)
        target_eitem['changeset_author_uuid'] = source_eitem.get(rol + '_uuid', None)
        target_eitem['changeset_author_name'] = source_eitem.get(rol + '_name', None)
        target_eitem['changeset_author_user_name'] = source_eitem.get(rol + '_user_name', None)
        target_eitem['changeset_author_domain'] = source_eitem.get(rol + '_domain', None)
        target_eitem['changeset_author_gender'] = source_eitem.get(rol + '_gender', None)
        target_eitem['changeset_author_gender_acc'] = source_eitem.get(rol + '_gender_acc', None)
        target_eitem['changeset_author_org_name'] = source_eitem.get(rol + '_org_name', None)
        target_eitem['changeset_author_bot'] = source_eitem.get(rol + '_bot', None)

        target_eitem['changeset_author_multi_org_names'] = source_eitem.get(rol + '_multi_org_names', [])

    def get_field_unique_id(self):
        return "id"

    def get_time_first_review_patchset(self, patchset):
        """Get the first date at which a review was made on the patchset by someone
        other than the user who created the patchset
        """
        patchset_author = patchset.get('author', None)
        patchset_author_username = patchset_author.get('username', None) if patchset_author else None
        patchset_author_email = patchset_author.get('email', None) if patchset_author else None
        patchset_created_on = str_to_datetime(patchset['createdOn']).isoformat()

        first_review = None

        approvals = patchset.get('approvals', [])
        for approval in approvals:

            if approval['type'] != CODE_REVIEW_TYPE:
                continue

            approval_granted_on = str_to_datetime(approval['grantedOn']).isoformat()
            if approval_granted_on < patchset_created_on:
                continue

            approval_by = approval.get('by', None)
            approval_by_username = approval_by.get('username', None) if approval_by else None
            approval_by_email = approval_by.get('email', None) if approval_by else None

            if approval_by_username and patchset_author_username:
                first_review = approval['grantedOn'] if approval_by_username != patchset_author_username else None
            elif approval_by_email and patchset_author_email:
                first_review = approval['grantedOn'] if approval_by_email != patchset_author_email else None
            else:
                # if patchset_author or approval_by is None
                first_review = approval['grantedOn']

            if first_review:
                break

        return first_review

    def get_time_first_review(self, review):
        """Get the first date at which a review was made on the changeset by someone
        other than the user who created the changeset
        """
        changeset_owner = review.get('owner', None)
        changeset_owner_username = changeset_owner.get('username', None) if changeset_owner else None
        changeset_owner_email = changeset_owner.get('email', None) if changeset_owner else None
        changeset_created_on = str_to_datetime(review['createdOn']).isoformat()

        first_review = None

        patchsets = review.get('patchSets', [])
        for patchset in patchsets:

            approvals = patchset.get('approvals', [])
            for approval in approvals:

                if approval['type'] != CODE_REVIEW_TYPE:
                    continue

                approval_granted_on = str_to_datetime(approval['grantedOn']).isoformat()
                if approval_granted_on < changeset_created_on:
                    continue

                approval_by = approval.get('by', None)
                approval_by_username = approval_by.get('username', None) if approval_by else None
                approval_by_email = approval_by.get('email', None) if approval_by else None

                if approval_by_username and changeset_owner_username:
                    first_review = approval['grantedOn'] if approval_by_username != changeset_owner_username else None
                elif approval_by_email and changeset_owner_email:
                    first_review = approval['grantedOn'] if approval_by_email != changeset_owner_email else None
                else:
                    # if changeset_owner or approval_by is None
                    first_review = approval['grantedOn']

                if first_review:
                    return first_review

        return first_review

    def last_changeset_approval_value(self, patchsets):
        """Get the last approval value for a given changeset by iterating on the
        corresponding patchsets. Non code-review or non verified patchset approvals,
        and approvals reviewed by the author of the patchset are filtered out.
        """
        def _get_last_status(approvals_list, patchset_author):
            """Get the first-found approval value from a reversed list of approvals.
            The value won't be considered if the patchset author matches with the approval author.
            """
            patchset_author_username = patchset_author.get('username', None) if patchset_author else None
            patchset_author_email = patchset_author.get('email', None) if patchset_author else None

            for approval in approvals_list:
                approval_by = approval.get('by', None)
                approval_by_username = approval_by.get('username', None) if approval_by else None
                approval_by_email = approval_by.get('email', None) if approval_by else None

                if approval_by_username and patchset_author_username:
                    approval_status = approval['value'] if approval_by_username != patchset_author_username else None
                elif approval_by_email and patchset_author_email:
                    approval_status = approval['value'] if approval_by_email != patchset_author_email else None
                else:
                    # if patchset_author or approval_by is None
                    approval_status = approval['value']

                if approval_status:
                    return approval_status

        last_status_code_review = None
        last_status_verified = None
        # reverse the patchsets list to get the latest ones first
        for patchset in reversed(patchsets):
            patchset_author = patchset.get('author', None)
            approvals = patchset.get('approvals', [])
            if not approvals:
                continue

            approvals_code_review = [a for a in reversed(approvals) if a['type'] == CODE_REVIEW_TYPE]
            approvals_verified = [a for a in reversed(approvals) if a['type'] == VERIFIED_TYPE]

            if not last_status_code_review:
                last_status_code_review = _get_last_status(approvals_code_review, patchset_author)
            if not last_status_verified:
                last_status_verified = _get_last_status(approvals_verified, patchset_author)

        return last_status_code_review, last_status_verified

    def enrich_items(self, ocean_backend):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        for item in ocean_backend.fetch():
            eitem = self.get_rich_item(item)

            items_to_enrich.append(eitem)

            comments = item['data'].get('comments', [])
            if comments:
                rich_item_comments = self.get_rich_item_comments(comments, eitem)
                items_to_enrich.extend(rich_item_comments)

            patchsets = item['data'].get('patchSets', [])
            if patchsets:
                rich_item_patchsets = self.get_rich_item_patchsets(patchsets, eitem)
                items_to_enrich.extend(rich_item_patchsets)

            if len(items_to_enrich) < MAX_SIZE_BULK_ENRICHED_ITEMS:
                continue

            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())
            items_to_enrich = []

        if len(items_to_enrich) > 0:
            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())

        if num_items != ins_items:
            missing = num_items - ins_items
            logger.error("[gerrit] {}/{} missing items".format(missing, num_items))
        else:
            logger.info("[gerrit] {} items inserted".format(num_items))

        return num_items

    def enrich_demography(self, ocean_backend, enrich_backend, alias, date_field="grimoire_creation_date",
                          author_field="author_uuid"):

        super().enrich_demography(ocean_backend, enrich_backend, alias, date_field, author_field=author_field)

    def enrich_demography_contribution(self, ocean_backend, enrich_backend, alias, date_field="grimoire_creation_date",
                                       author_field="author_uuid"):

        super().enrich_demography_contribution(ocean_backend, enrich_backend, alias, date_field, author_field=author_field)

    def enrich_onion(self, ocean_backend, enrich_backend, alias,
                     no_incremental=False,
                     in_index='gerrit_onion-src',
                     out_index='gerrit_onion-enriched',
                     data_source='gerrit',
                     contribs_field='uuid',
                     timeframe_field='grimoire_creation_date',
                     sort_on_field='metadata__timestamp',
                     seconds=Enrich.ONION_INTERVAL):

        super().enrich_onion(enrich_backend=enrich_backend,
                             alias=alias,
                             in_index=in_index,
                             out_index=out_index,
                             data_source=data_source,
                             contribs_field=contribs_field,
                             timeframe_field=timeframe_field,
                             sort_on_field=sort_on_field,
                             no_incremental=no_incremental,
                             seconds=seconds)

    def add_gelk_metadata(self, eitem):
        eitem['metadata__gelk_version'] = self.gelk_version
        eitem['metadata__gelk_backend_name'] = self.__class__.__name__
        eitem['metadata__enriched_on'] = datetime_utcnow().isoformat()
