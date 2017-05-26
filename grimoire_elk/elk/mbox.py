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
import email.utils

from .enrich import Enrich, metadata
from .utils import get_time_diff_days


logger = logging.getLogger(__name__)


class MBoxEnrich(Enrich):

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = [self.kafka_kip]


    def get_field_author(self):
        return "From"

    def get_fields_uuid(self):
        return ["from_uuid"]

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                 "Subject_analyzed": {
                   "type": "string",
                   "index":"analyzed"
                 },
                 "body": {
                   "type": "string",
                   "index":"analyzed"
                 }
           }
        } """

        return {"items":mapping}

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        item = item['data']
        for identity in ['From']:
            if identity in item and item[identity]:
                user = self.get_sh_identity(item[identity])
                identities.append(user)
        return identities

    def get_sh_identity(self, item, identity_field=None):
        # "From": "hwalsh at wikiledia.net (Heat Walsh)"

        identity = {}

        from_data = item
        if 'data' in item and type(item) == dict:
            from_data = item['data'][identity_field]

        # First desofuscate the email
        EMAIL_OBFUSCATION_PATTERNS = [' at ', '_at_', ' en ']
        for pattern in EMAIL_OBFUSCATION_PATTERNS:
            if from_data.find(pattern) != -1:
                from_data = from_data.replace(pattern, '@')

        from_ = email.utils.parseaddr(from_data)

        identity['username'] = None  # email does not have username
        identity['email'] = from_[1]
        identity['name'] = from_[0]
        if not identity['name']:
            identity['name'] = identity['email'].split('@')[0]
        return identity

    def get_project_repository(self, eitem):
        mls_list = eitem['origin']
        # Eclipse specific yet
        repo = "/mnt/mailman_archives/"
        repo += mls_list+".mbox/"+mls_list+".mbox"
        return repo

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        message = item['data']

        # Fields that are the same in message and eitem
        copy_fields = ["Date", "From", "Subject", "Message-ID"]
        for f in copy_fields:
            if f in message:
                eitem[f] = message[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {
            "Subject": "Subject_analyzed"
        }
        for fn in map_fields:
            if fn in message:
                eitem[map_fields[fn]] = message[fn]
            else:
                eitem[map_fields[fn]] = None

        # Enrich dates
        eitem["email_date"] = parser.parse(item["metadata__updated_on"]).isoformat()
        eitem["list"] = item["origin"]

        # Root message
        if 'In-Reply-To' in message:
            eitem["root"] = False
        else:
            eitem["root"] = True

        # The body is needed in studies like kafka_kip
        eitem["body"] = message['body']['plain']
        # Size of the message
        try:
            eitem["size"] = len(message['body']['plain'])
        except:
            eitem["size"] = None

        # Time zone
        try:
            message_date = parser.parse(message['Date'])
            eitem["tz"]  = int(message_date.strftime("%z")[0:3])
        except:
            eitem["tz"]  = None

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(message['Date'], "message"))

        return eitem

    def enrich_items(self, ocean_backend):
        # items is a generator but we need to reuse it so we store all items
        # from the generator in a list
        # items = list(items)

        # Use standard method and if fails, use the old one with Unicode control
        total = 0
        try:
            total = super(MBoxEnrich, self).enrich_items(ocean_backend)
        except UnicodeEncodeError:
            total = self.enrich_items_old(ocean_backend.fetch())

        return total

    def enrich_items_old(self, items):
        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        url = self.elastic.index_url+'/items/_bulk'

        logger.debug("Adding items to %s (in %i packs)" % (url, max_items))

        for item in items:
            if current >= max_items:
                self.requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (rich_item[self.get_field_unique_id()])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
            total += 1
        try:
            self.requests.put(url, data = bulk_json)
        except UnicodeEncodeError:
            # Related to body.encode('iso-8859-1'). mbox data
            logger.error("Encoding error ... converting bulk to iso-8859-1")
            bulk_json = bulk_json.encode('iso-8859-1','ignore')
            self.requests.put(url, data=bulk_json)

        return total

    def kafka_kip(self, from_date=None):

        def extract_vote_and_binding(body):
            """ Extracts the vote and binding for a KIP process included in message body """

            vote = 0
            binding = 0

            for line in body.split("\n"):
                if line.startswith("+1"):
                    vote = 1
                    if 'non-binding' in line:
                        binding = 0
                    elif 'binding' in line:
                        binding = 1
                    break
                elif line.startswith("-1"):
                    vote = -1
                    if 'non-binding' in line:
                        binding = 0
                    elif 'binding' in line:
                        binding = 1
                    break

            return (vote, binding)

        def extract_kip(subject):
            """ Extracts a KIP number from an email subject """

            kip = None

            if 'KIP' not in subject:
                return kip

            kip_tokens = subject.split('KIP')
            if len(kip_tokens) > 2:
                # [KIP-DISCUSSION] KIP-7 Security
                for token in kip_tokens:
                    kip = extract_kip("KIP"+token)
                    if kip:
                        break
                # logger.debug("Several KIPs in %s. Found: %i", subject, kip)
                return kip

            str_with_kip = kip_tokens[1]

            if not str_with_kip:
                # Sample use case subject: Create a space template for KIP
                return kip

            if str_with_kip[0] == '-':
                try:
                    # KIP-120: Control
                    str_kip = str_with_kip[1:].split(":")[0]
                    kip = int(str_kip)
                    return kip
                except ValueError:
                    pass
                try:
                    # KIP-8 Add
                    str_kip = str_with_kip[1:].split(" ")[0]
                    kip = int(str_kip)
                    return kip
                except ValueError:
                    pass
                try:
                    # KIP-11- Authorization
                    str_kip = str_with_kip[1:].split("-")[0]
                    kip = int(str_kip)
                    return kip
                except ValueError:
                    pass
                try:
                    # Bound fetch response size (KIP-74)
                    str_kip = str_with_kip[1:].split(")")[0]
                    kip = int(str_kip)
                    return kip
                except ValueError:
                    pass
                try:
                    # KIP-31&
                    str_kip = str_with_kip[1:].split("&")[0]
                    kip = int(str_kip)
                    return kip
                except ValueError:
                    pass
                try:
                    # KIP-31/
                    str_kip = str_with_kip[1:].split("/")[0]
                    kip = int(str_kip)
                    return kip
                except ValueError:
                    pass
                try:
                    # Re: Copycat (KIP-26. PR-99) - plan on moving forward
                    str_kip = str_with_kip[1:].split(".")[0]
                    kip = int(str_kip)
                    return kip
                except ValueError:
                    pass
            elif str_with_kip[0] == ' ':
                try:
                    # KIP 20 Enable
                    str_kip = str_with_kip[1:].split(" ")[0]
                    kip = int(str_kip)
                    return kip
                except ValueError:
                    pass
                try:
                    # Re: [DISCUSS] KIP 88: DescribeGroups Protocol Update
                    str_kip = str_with_kip[1:].split(":")[0]
                    kip = int(str_kip)
                    return kip
                except ValueError:
                    pass
                try:
                    # [jira] [Updated] (KAFKA-5092) KIP 141- ProducerRecordBuilder
                    str_kip = str_with_kip[1:].split("-")[0]
                    kip = int(str_kip)
                    return kip
                except ValueError:
                    pass
            elif str_with_kip[0] == ':':
                try:
                    # Re: [VOTE] KIP:71 Enable log compaction and deletion to co-exist
                    str_kip = str_with_kip[1:].split(" ")[0]
                    kip = int(str_kip)
                    return kip
                except ValueError:
                    pass

            if not kip:
                # logger.debug("Can not extract KIP from %s", subject)
                pass

            return kip

        def lazy_result(votes):
            """ Compute the result of a votation using lazy consensus
                which requires 3 binding +1 votes and no binding vetoes.
            """
            yes = 0
            yes_binding = 0
            veto = 0
            veto_binding = 0

            result = -1

            for (vote, binding) in votes:
                if vote == 1:
                    if binding:
                        yes_binding += 1
                    else:
                        yes += 1
                if vote == -1:
                    if binding:
                        veto_binding += 1
                    else:
                        veto += 1

            if veto_binding == 0 and yes_binding >= 3:
                result = 1

            return result

        def add_kip_time_status_fields(self):
            """ Add kip fields with final status and times """

            total = 0

            for eitem in self.fetch():
                # kip_status: adopted (closed), discussion (open), voting (open),
                #             inactive (open), discarded (closed)
                kip_fields = {
                    "kip_status": None,
                    "kip_discuss_time_days": None,
                    "kip_voting_time_days": None,
                    "kip_is_first_discuss": 0,
                    "kip_is_first_vote": 0,
                    "kip_is_last_discuss": 0,
                    "kip_is_last_vote": 0,
                    "kip_result": None,
                }

                if "kip" not in eitem:
                    # It is not a KIP message
                    continue
                kip = eitem["kip"]
                kip_date = parser.parse(eitem["email_date"])

                if eitem['kip_is_discuss']:
                    kip_fields["kip_discuss_time_days"] = \
                        get_time_diff_days(self.kips_dates[kip]['kip_min_discuss'],
                                           self.kips_dates[kip]['kip_max_discuss'])

                    # Detect first and last discuss messages
                    if kip_date == self.kips_dates[kip]['kip_min_discuss']:
                        kip_fields['kip_is_first_discuss'] = 1
                    elif kip_date == self.kips_dates[kip]['kip_max_discuss']:
                        kip_fields['kip_is_last_discuss'] = 1

                    # Detect discussion status
                    if "kip_min_vote" not in self.kips_dates[kip]:
                        kip_fields['kip_status'] = 'discussion'


                if eitem['kip_is_vote']:
                    kip_fields["kip_voting_time_days"] = \
                        get_time_diff_days(self.kips_dates[kip]['kip_min_vote'],
                                           self.kips_dates[kip]['kip_max_vote'])

                    # Detect first and last discuss messages
                    if kip_date == self.kips_dates[kip]['kip_min_vote']:
                        kip_fields['kip_is_first_vote'] = 1
                    elif kip_date == self.kips_dates[kip]['kip_max_vote']:
                        kip_fields['kip_is_last_vote'] = 1

                    # Detect discussion status
                    kip_fields['kip_status'] = 'voting'

                    # Now check if there is a result from self.kips_scores
                    kip_fields['kip_result'] = lazy_result(self.kips_scores[kip])

                    if kip_fields['kip_result'] == 1:
                        kip_fields['kip_status'] = 'adopted'

                    # And now change the status inactive or discarded if
                    # a condition could be defined

                eitem.update(kip_fields)
                yield eitem
                total += 1

            logger.info("Total eitems with kafka extra kip fields %i", total)


        def add_kip_fields(self):
            """ Add extra fields needed for kip analysis"""

            total = 0

            self.kips_dates = {
                0: {
                    "kip_min_discuss": None,
                    "kip_max_discuss": None,
                    "kip_min_vote": None,
                    "kip_max_vote": None,
                }
            }

            self.kips_scores = {}

            # First iteration
            for eitem in self.fetch():
                kip_fields = {
                    "kip_is_vote": 0,
                    "kip_is_discuss": 0,
                    "kip_vote": 0,
                    "kip_binding": 0,
                    "kip": 0,
                    "kip_type": "general"
                }

                kip = extract_kip(eitem['Subject'])
                if not kip:
                    # It is not a KIP message
                    continue
                if kip not in self.kips_dates:
                    self.kips_dates[kip] = {}
                if kip not in self.kips_scores:
                    self.kips_scores[kip] = []

                kip_date = parser.parse(eitem["email_date"])

                # Analyze the subject to fill the kip fields
                if '[discuss]' in eitem['Subject'].lower():
                    kip_fields['kip_is_discuss'] = 1
                    kip_fields['kip_type'] = "discuss"
                    kip_fields['kip'] = kip
                    # Update kip discuss dates
                    if "kip_min_discuss" not in self.kips_dates[kip]:
                        self.kips_dates[kip].update({
                            "kip_min_discuss": kip_date,
                            "kip_max_discuss": kip_date
                        })
                    else:
                        if self.kips_dates[kip]["kip_min_discuss"] >= kip_date:
                            self.kips_dates[kip]["kip_min_discuss"] = kip_date
                        if self.kips_dates[kip]["kip_max_discuss"] <= kip_date:
                            self.kips_dates[kip]["kip_max_discuss"] = kip_date

                if '[vote]' in eitem['Subject'].lower():
                    kip_fields['kip_is_vote'] = 1
                    kip_fields['kip_type'] = "vote"
                    kip_fields['kip'] = kip
                    if 'body' in eitem:
                        (vote, binding) = extract_vote_and_binding(eitem['body'])
                        self.kips_scores[kip] += [(vote, binding)]
                        kip_fields['kip_vote'] = vote
                        kip_fields['kip_binding'] = binding
                    else:
                        logger.debug("Message %s without body", eitem['Subject'])
                    # Update kip discuss dates
                    if "kip_min_vote" not in self.kips_dates[kip]:
                        self.kips_dates[kip].update({
                            "kip_min_vote": kip_date,
                            "kip_max_vote": kip_date
                        })
                    else:
                        if self.kips_dates[kip]["kip_min_vote"] >= kip_date:
                            self.kips_dates[kip]["kip_min_vote"] = kip_date
                        if self.kips_dates[kip]["kip_max_vote"] <= kip_date:
                            self.kips_dates[kip]["kip_max_vote"] = kip_date


                eitem.update(kip_fields)
                yield eitem
                total += 1

            logger.info("Total eitems with kafka kip fields %i", total)

        logger.debug("Doing kafka_kip study from %s", self.elastic.index_url)

        # First iteration with the basic fields
        eitems = add_kip_fields(self)
        self.elastic.bulk_upload_sync(eitems, self.get_field_unique_id())

        # Second iteration with the final time and status fields
        eitems = add_kip_time_status_fields(self)
        self.elastic.bulk_upload_sync(eitems, self.get_field_unique_id())
