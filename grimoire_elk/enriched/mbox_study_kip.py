# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
#

import logging

from .utils import get_time_diff_days, anonymize_url
from grimoirelab_toolkit.datetime import (datetime_utcnow, str_to_datetime)

logger = logging.getLogger(__name__)

MAX_LINES_FOR_VOTE = 10


def kafka_kip(enrich):

    """ Kafka Improvement Proposals process study """

    def extract_vote_and_binding(body):
        """ Extracts the vote and binding for a KIP process included in message body """

        vote = 0
        binding = 0  # by default the votes are binding for +1
        nlines = 0

        for line in body.split("\n"):
            if nlines > MAX_LINES_FOR_VOTE:
                # The vote must be in the first MAX_LINES_VOTE
                break
            if line.startswith(">"):
                # This line is from a previous email
                continue
            elif "+1" in line and "-1" in line:
                # Report summary probably
                continue
            elif "to -1" in line or "is -1" in line or "= -1" in line or "-1 or" in line:
                continue
            elif line.startswith("+1") or " +1 " in line or line.endswith("+1") \
                    or " +1." in line or " +1," in line:
                vote = 1
                binding = 1  # by default the votes are binding for +1
                if 'non-binding' in line.lower():
                    binding = 0
                elif 'binding' in line.lower():
                    binding = 1
                break
            elif line.startswith("-1") or line.endswith(" -1") or " -1 " in line \
                    or " -1." in line or " -1," in line:
                vote = -1
                if 'non-binding' in line.lower():
                    binding = 0
                elif 'binding' in line.lower():
                    binding = 1
                break
            nlines += 1

        return (vote, binding)

    def extract_kip(subject):
        """ Extracts a KIP number from an email subject """

        kip = None

        if not subject:
            return kip

        if 'KIP' not in subject:
            return kip

        kip_tokens = subject.split('KIP')
        if len(kip_tokens) > 2:
            # [KIP-DISCUSSION] KIP-7 Security
            for token in kip_tokens:
                kip = extract_kip("KIP" + token)
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

    def add_kip_final_status_field(enrich):
        """ Add kip final status field """

        total = 0

        for eitem in enrich.fetch():
            if "kip" not in eitem:
                # It is not a KIP message
                continue

            if eitem['kip'] in enrich.kips_final_status:
                eitem.update({"kip_final_status":
                              enrich.kips_final_status[eitem['kip']]})
            else:
                logger.warning("[mbox] study Kafka KIP no final status: {}".format(eitem['kip']))
                eitem.update({"kip_final_status": None})
            yield eitem
            total += 1

        logger.info("[mbox] study Kafka KIP total eitems with kafka final status kip field {}".format(total))

    def add_kip_time_status_fields(enrich):
        """ Add kip fields with final status and times """

        total = 0
        max_inactive_days = 90  # days

        enrich.kips_final_status = {}  # final status for each kip

        for eitem in enrich.fetch():
            # kip_status: adopted (closed), discussion (open), voting (open),
            #             inactive (open), discarded (closed)
            # kip_start_end: discuss_start, discuss_end, voting_start, voting_end
            kip_fields = {
                "kip_status": None,
                "kip_discuss_time_days": None,
                "kip_discuss_inactive_days": None,
                "kip_voting_time_days": None,
                "kip_voting_inactive_days": None,
                "kip_is_first_discuss": 0,
                "kip_is_first_vote": 0,
                "kip_is_last_discuss": 0,
                "kip_is_last_vote": 0,
                "kip_result": None,
                "kip_start_end": None
            }

            if "kip" not in eitem:
                # It is not a KIP message
                continue
            kip = eitem["kip"]
            kip_date = str_to_datetime(eitem["email_date"])

            if eitem['kip_is_discuss']:
                kip_fields["kip_discuss_time_days"] = \
                    get_time_diff_days(enrich.kips_dates[kip]['kip_min_discuss'],
                                       enrich.kips_dates[kip]['kip_max_discuss'])

                # Detect first and last discuss messages
                if kip_date == enrich.kips_dates[kip]['kip_min_discuss']:
                    kip_fields['kip_is_first_discuss'] = 1
                    kip_fields['kip_start_end'] = 'discuss_start'
                elif kip_date == enrich.kips_dates[kip]['kip_max_discuss']:
                    kip_fields['kip_is_last_discuss'] = 1
                    kip_fields['kip_start_end'] = 'discuss_end'

                # Detect discussion status
                if "kip_min_vote" not in enrich.kips_dates[kip]:
                    kip_fields['kip_status'] = 'discussion'
                max_discuss_date = enrich.kips_dates[kip]['kip_max_discuss']
                kip_fields['kip_discuss_inactive_days'] = get_time_diff_days(max_discuss_date, datetime_utcnow())

            if eitem['kip_is_vote']:
                kip_fields["kip_voting_time_days"] = \
                    get_time_diff_days(enrich.kips_dates[kip]['kip_min_vote'],
                                       enrich.kips_dates[kip]['kip_max_vote'])

                # Detect first and last discuss messages
                if kip_date == enrich.kips_dates[kip]['kip_min_vote']:
                    kip_fields['kip_is_first_vote'] = 1
                    kip_fields['kip_start_end'] = 'voting_start'
                elif kip_date == enrich.kips_dates[kip]['kip_max_vote']:
                    kip_fields['kip_is_last_vote'] = 1
                    kip_fields['kip_start_end'] = 'voting_end'

                # Detect discussion status
                kip_fields['kip_status'] = 'voting'
                max_vote_date = enrich.kips_dates[kip]['kip_max_vote']
                kip_fields['kip_voting_inactive_days'] = get_time_diff_days(max_vote_date, datetime_utcnow())

                # Now check if there is a result from enrich.kips_scores
                kip_fields['kip_result'] = lazy_result(enrich.kips_scores[kip])

                if kip_fields['kip_result'] == 1:
                    kip_fields['kip_status'] = 'adopted'
                elif kip_fields['kip_result'] == -1:
                    kip_fields['kip_status'] = 'discarded'

            # And now change the status inactive
            if kip_fields['kip_status'] not in ['adopted', 'discarded']:
                inactive_days = kip_fields['kip_discuss_inactive_days']

                if inactive_days and inactive_days > max_inactive_days:
                    kip_fields['kip_status'] = 'inactive'

                inactive_days = kip_fields['kip_voting_inactive_days']
                if inactive_days and inactive_days > max_inactive_days:
                    kip_fields['kip_status'] = 'inactive'

            # The final status is in the kip_is_last_discuss or kip_is_last_vote
            # It will be filled in the next enrichment round
            eitem.update(kip_fields)
            if eitem['kip'] not in enrich.kips_final_status:
                enrich.kips_final_status[kip] = None
            if eitem['kip_is_last_discuss'] and not enrich.kips_final_status[kip]:
                enrich.kips_final_status[kip] = kip_fields['kip_status']
            if eitem['kip_is_last_vote']:
                enrich.kips_final_status[kip] = kip_fields['kip_status']

            yield eitem
            total += 1

        logger.info("[mbox] study Kafka KIP total eitems with kafka extra kip fields {}".format(total))

    def add_kip_fields(enrich):
        """ Add extra fields needed for kip analysis"""

        total = 0

        enrich.kips_dates = {
            0: {
                "kip_min_discuss": None,
                "kip_max_discuss": None,
                "kip_min_vote": None,
                "kip_max_vote": None,
            }
        }

        enrich.kips_scores = {}

        # First iteration
        for eitem in enrich.fetch():
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
            if kip not in enrich.kips_dates:
                enrich.kips_dates[kip] = {}
            if kip not in enrich.kips_scores:
                enrich.kips_scores[kip] = []

            kip_date = str_to_datetime(eitem["email_date"])

            # Analyze the subject to fill the kip fields
            if '[discuss]' in eitem['Subject'].lower() or \
               '[kip-discussion]' in eitem['Subject'].lower() or \
               '[discussion]' in eitem['Subject'].lower():
                kip_fields['kip_is_discuss'] = 1
                kip_fields['kip_type'] = "discuss"
                kip_fields['kip'] = kip
                # Update kip discuss dates
                if "kip_min_discuss" not in enrich.kips_dates[kip]:
                    enrich.kips_dates[kip].update({
                        "kip_min_discuss": kip_date,
                        "kip_max_discuss": kip_date
                    })
                else:
                    if enrich.kips_dates[kip]["kip_min_discuss"] >= kip_date:
                        enrich.kips_dates[kip]["kip_min_discuss"] = kip_date
                    if enrich.kips_dates[kip]["kip_max_discuss"] <= kip_date:
                        enrich.kips_dates[kip]["kip_max_discuss"] = kip_date

            if '[vote]' in eitem['Subject'].lower():
                kip_fields['kip_is_vote'] = 1
                kip_fields['kip_type'] = "vote"
                kip_fields['kip'] = kip
                if 'body_extract' in eitem:
                    (vote, binding) = extract_vote_and_binding(eitem['body_extract'])
                    enrich.kips_scores[kip] += [(vote, binding)]
                    kip_fields['kip_vote'] = vote
                    kip_fields['kip_binding'] = binding
                else:
                    logger.debug("[mbox] study Kafka KIP message {} without body".format(eitem['Subject']))
                # Update kip discuss dates
                if "kip_min_vote" not in enrich.kips_dates[kip]:
                    enrich.kips_dates[kip].update({
                        "kip_min_vote": kip_date,
                        "kip_max_vote": kip_date
                    })
                else:
                    if enrich.kips_dates[kip]["kip_min_vote"] >= kip_date:
                        enrich.kips_dates[kip]["kip_min_vote"] = kip_date
                    if enrich.kips_dates[kip]["kip_max_vote"] <= kip_date:
                        enrich.kips_dates[kip]["kip_max_vote"] = kip_date

            eitem.update(kip_fields)
            yield eitem
            total += 1

        logger.info("[mbox] study Kafka KIP total eitems with kafka kip fields {}".format(total))

    logger.debug("[mbox] study Kafka KIP doing from {}".format(anonymize_url(enrich.elastic.index_url)))

    # First iteration with the basic fields
    eitems = add_kip_fields(enrich)
    enrich.elastic.bulk_upload(eitems, enrich.get_field_unique_id())

    # Second iteration with the final time and status fields
    eitems = add_kip_time_status_fields(enrich)
    enrich.elastic.bulk_upload(eitems, enrich.get_field_unique_id())

    # Third iteration to compute the end status field for all KIPs
    eitems = add_kip_final_status_field(enrich)
    enrich.elastic.bulk_upload(eitems, enrich.get_field_unique_id())
