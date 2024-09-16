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

from datetime import datetime
import logging
import time

from sortinghat.cli.client import (SortingHatClientError,
                                   SortingHatSchema)
from sgqlc.operation import Operation


logger = logging.getLogger(__name__)

SLEEP_TIME = 5

DEFAULT_FROM_DATE = datetime(1900, 1, 1)
DEFAULT_TO_DATE = datetime(2100, 1, 1)

MULTI_ORG_NAMES = '_multi_org_names'

PAGE = 1
PAGE_SIZE = 100


class SortingHat(object):

    # Mutations
    @classmethod
    def add_enrollment(cls, db, uuid, organization, from_date=DEFAULT_FROM_DATE, to_date=DEFAULT_TO_DATE):
        args = {
            "from_date": from_date,
            "group": organization,
            "to_date": to_date,
            "uuid": uuid
        }
        try:
            op = Operation(SortingHatSchema.SortingHatMutation)
            add_org = op.enroll(**args)
            add_org.individual().enrollments().group().name()
            result = db.execute(op)
            org_name = result['data']['enroll']['individual']['enrollments'][0]['group']['name']
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error add enrollment {} {} {} {}: {}".
                         format(uuid, organization, from_date, to_date, e.errors[0]['message']))
            org_name = None

        return org_name

    @classmethod
    def add_id(cls, db, source, email=None, name=None, username=None):
        uuid = None
        args = {
            "email": email,
            "name": name,
            "source": source,
            "username": username
        }
        args_without_empty = {k: v for k, v in args.items() if v}
        try:
            op = Operation(SortingHatSchema.SortingHatMutation)
            add = op.add_identity(**args_without_empty)
            add.uuid()
            result = db.execute(op)
            uuid = result['data']['addIdentity']['uuid']

            logger.debug("[sortinghat] New identity {} {},{},{} ".format(
                uuid, args['username'], args['name'], args['email']))
        except SortingHatClientError as ex:
            msg = ex.errors[0]['message']
            if 'already exists' in msg:
                logger.debug("[sortinghat] {}".format(msg))
                uuid = msg.split("'")[1]
            else:
                raise SortingHatClientError(msg)
        return uuid

    @classmethod
    def add_identity(cls, db, identity, backend):
        """ Load and identity list from backend in Sorting Hat """
        uuid = None

        try:
            uuid = cls.add_id(db, backend, email=identity['email'],
                              name=identity['name'], username=identity['username'])
        except UnicodeEncodeError:
            logger.warning("[sortinghat] UnicodeEncodeError. Ignoring it. {} {} {}".format(
                           identity['email'], identity['name'], identity['username']))
        except SortingHatClientError as e:
            logger.warning("[sortinghat] Client error adding identity: {}. Ignoring it. {} {} {}".format(
                           e, identity['email'], identity['name'], identity['username']))
        except Exception:
            logger.warning("[sortinghat] Unknown exception adding identity. Ignoring it. {} {} {}".format(
                           identity['email'], identity['name'], identity['username']))

        if 'company' in identity and identity['company'] is not None:
            org_name = cls.add_organization(db, identity['company'])
            logger.debug("[sortinghat] New organization added {}".format(org_name))

            org_name = cls.add_enrollment(db, uuid, identity['company'])
            logger.debug("[sortinghat] New enrollment added {}".format(org_name))

        return uuid

    @classmethod
    def add_identities(cls, db, identities, backend):
        """ Load identities list from backend in Sorting Hat """

        logger.debug("[sortinghat] Adding identities")

        op = Operation(SortingHatSchema.SortingHatMutation)
        for i, identity in enumerate(identities):
            args = {
                "source": backend,
                "email": identity['email'],
                "name": identity['name'],
                "username": identity['username']
            }
            args_without_empty = {k: v for k, v in args.items() if v}
            if args_without_empty:
                op.add_identity(**args_without_empty, __alias__=f'identity_{i}')
        try:
            db.execute(op)
        except SortingHatClientError as ex:
            for error in ex.errors:
                msg = error['message']
                status = error.get('status', None)
                if 'already exists' in msg:
                    logger.debug("[sortinghat] {}".format(msg))
                elif status and status == 502:
                    raise SortingHatClientError(ex)
                else:
                    logger.warning("[sortinghat] {}".format(msg))

    @classmethod
    def add_organization(cls, db, organization):
        try:
            op = Operation(SortingHatSchema.SortingHatMutation)
            add_org = op.add_organization(name=organization)
            add_org.organization().name()
            result = db.execute(op)
            org_name = result['data']['addOrganization']['organization']['name']
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error add organization {}: {}".format(organization, e.errors[0]['message']))
            org_name = None

        return org_name

    @classmethod
    def do_affiliate(cls, db):
        entities = SortingHat.unique_identities(db)
        if not entities:
            return
        mks = [e['mk'] for e in entities]
        args = {
            "uuids": mks
        }
        try:
            op = Operation(SortingHatSchema.SortingHatMutation)
            op.affiliate(**args).job_id
            result = db.execute(op)
            job_id = result['data']['affiliate']['jobId']
            logger.info("[sortinghat] Affiliate job id: {}".format(job_id))
            wait = cls.check_job(db, job_id)
            while wait:
                time.sleep(SLEEP_TIME)
                wait = cls.check_job(db, job_id)
            logger.info("[sortinghat] Affiliate finished job id: {}".format(job_id))
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error affiliate job id: {}\n{}".format(job_id, e.errors[0]))

        return

    @classmethod
    def do_autogender(cls):
        return None

    @classmethod
    def do_unify(cls, db, kwargs):
        entities = SortingHat.unique_identities(db)
        if not entities:
            return
        mks = [e['mk'] for e in entities]
        try:
            op = Operation(SortingHatSchema.SortingHatMutation)
            args = {
                'criteria': [kwargs['matching']],
                'source_uuids': mks
            }
            op.unify(**args).job_id
            result = db.execute(op)
            job_id = result['data']['unify']['jobId']
            logger.info("[sortinghat] Unify job id: {}".format(job_id))
            wait = cls.check_job(db, job_id)
            while wait:
                time.sleep(SLEEP_TIME)
                wait = cls.check_job(db, job_id)
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error unify criteria: {}\n{}".format(args['criteria'], e.errors[0]))

        return

    @classmethod
    def remove_identity(cls, sh_db, ident_id):
        """Delete an identity or unique identity from SortingHat.

        :param sh_db: SortingHat database
        :param ident_id: identity identifier
        """
        success = False
        try:
            op = Operation(SortingHatSchema.SortingHatMutation)
            delete = op.delete_identity(uuid=ident_id)
            delete.uuid()
            result = sh_db.execute(op)
            removed_uuid = result['data']['deleteIdentity']['uuid']
            logger.debug("[sortinghat] Identity {} deleted".format(removed_uuid))
            success = True
        except Exception as e:
            logger.error("[sortinghat] Error remove identity {}: {}".format(ident_id, e))

        return success

    @classmethod
    def update_profile(cls, db, uuid, data):
        args = {
            'uuid': uuid,
            'data': data
        }
        try:
            op = Operation(SortingHatSchema.SortingHatMutation)
            update = op.update_profile(**args)
            update.uuid()
            result = db.execute(op)
            uuid = result['data']['updateProfile']['uuid']
            logger.debug("[sortinghat] updated: {}".format(uuid))
        except SortingHatClientError as ex:
            msg = ex.errors[0]['message']
            logger.error("[sortinghat] updating {}: {}".format(args, msg))

    # Queries
    @classmethod
    def check_job(cls, db, id):
        args = {
            "job_id": id
        }
        wait = True

        op = Operation(SortingHatSchema.Query)
        jobid = op.job(**args)
        jobid.status()
        jobid.errors()
        result = db.execute(op)
        job = result['data']['job']
        if job['status'] == 'finished':
            wait = False
        elif job['status'] == 'failed':
            error_msg = {
                "msg": "job failed",
                "errors": job['errors']
            }
            raise SortingHatClientError(**error_msg)

        return wait

    @classmethod
    def is_bot(cls, db, uuid):
        bot = None
        args = {
            'filters': {
                'uuid': uuid
            }
        }
        try:
            op = Operation(SortingHatSchema.Query)
            op.individuals(**args)
            individual = op.individuals().entities()
            profile = individual.profile()
            profile.is_bot()
            result = db.execute(op)
            bot = result['data']['individuals']['entities'][0]['profile']['isBot']
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error is bot {}: {}".format(uuid, e.errors[0]['message']))
        return bot

    @classmethod
    def get_enrollments(cls, db, uuid):
        args = {
            'page': PAGE,
            'page_size': PAGE_SIZE,
            'filters': {
                'uuid': uuid
            }
        }
        try:
            op = Operation(SortingHatSchema.Query)
            op.individuals(**args)
            individual = op.individuals().entities()
            enrollments = individual.enrollments()
            enrollments.group().name()
            enrollments.group().type()
            enrollments.group().parent_org().name()
            enrollments.start()
            enrollments.end()
            result = db.execute(op)
            enroll = result['data']['individuals']['entities'][0]['enrollments']
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error get enrollments {}: {}".format(uuid, e.errors[0]['message']))
            enroll = None

        return enroll

    @classmethod
    def get_entity(cls, db, id):
        entity = None
        args = {
            'filters': {
                'uuid': id
            }
        }
        try:
            op = Operation(SortingHatSchema.Query)
            op.individuals(**args)
            individual = op.individuals().entities()
            individual.mk()
            identities = individual.identities()
            identities.uuid()
            identities.name()
            identities.email()
            identities.username()
            profile = individual.profile()
            profile.name()
            profile.email()
            profile.gender()
            profile.gender_acc()
            profile.is_bot()
            enrollments = individual.enrollments()
            enrollments.group().parent_org().name()
            enrollments.group().name()
            enrollments.group().type()
            enrollments.start()
            enrollments.end()
            result = db.execute(op)
            if result['data']['individuals']['entities']:
                entity = result['data']['individuals']['entities'][0]
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error get entities {}: {}".format(id, e.errors[0]['message']))
            raise SortingHatClientError(e)
        return entity

    @classmethod
    def get_unique_identity(cls, db, uuid):
        args = {
            'filters': {
                'uuid': uuid
            }
        }
        try:
            op = Operation(SortingHatSchema.Query)
            op.individuals(**args)
            individual = op.individuals().entities()
            individual.mk()
            profile = individual.profile()
            profile.name()
            profile.email()
            profile.gender()
            profile.gender_acc()
            result = db.execute(op)
            p = result['data']['individuals']['entities'][0]['profile']
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error get unique identity {}: {}".format(uuid, e.errors[0]['message']))
            p = None

        return p

    @classmethod
    def get_uuid_from_id(cls, db, sh_id):
        uuid = None
        args = {
            "filters": {
                "uuid": sh_id
            }
        }
        try:
            op = Operation(SortingHatSchema.Query)
            op.individuals(**args)
            individual = op.individuals().entities()
            individual.mk()
            result = db.execute(op)
            entities = result['data']['individuals']['entities']
            mks = [e['mk'] for e in entities]
            if mks:
                uuid = mks[0]
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error get uuid from id {}: {}".format(sh_id, e.errors[0]['message']))

        return uuid

    @classmethod
    def get_uuids_from_profile_name(cls, db, profile_name):
        """ Get the uuid for a profile name """

        def fetch_mk(entities, name):
            mks = []
            for e in entities:
                mk = e['mk']
                if e['profile']['name'] == name:
                    mks.append(mk)

            return mks

        args = {
            'page': 1,
            'page_size': 10,
            'filters': {
                'term': profile_name
            }
        }
        try:
            op = Operation(SortingHatSchema.Query)
            op.individuals(**args)
            individual = op.individuals().entities()
            individual.mk()
            individual.profile().name()
            page_info = op.individuals().page_info()
            page_info.has_next()
            result = db.execute(op)
            entities = result['data']['individuals']['entities']
            mks = fetch_mk(entities, profile_name)
            has_next = result['data']['individuals']['pageInfo']['hasNext']
            while has_next:
                page = args['page']
                args['page'] = page + 1
                op = Operation(SortingHatSchema.Query)
                op.individuals(**args)
                individual = op.individuals().entities()
                individual.mk()
                individual.profile().name()
                page_info = op.individuals().page_info()
                page_info.has_next()
                result = db.execute(op)
                entities = result['data']['individuals']['entities']
                mks.extend(fetch_mk(entities, profile_name))
                has_next = result['data']['individuals']['pageInfo']['hasNext']
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error get uuid from profile name {}: {}".
                         format(profile_name, e.errors[0]['message']))

        return mks

    @classmethod
    def unique_identities(cls, sh_db):
        """List the unique identities available in SortingHat.

        :param sh_db: SortingHat database
        """
        args = {
            'page': PAGE,
            'page_size': PAGE_SIZE
        }
        try:
            op = Operation(SortingHatSchema.Query)
            op.individuals(**args)
            individual = op.individuals().entities()
            individual.mk()
            individual.identities().source()
            individual.identities().uuid()
            page_info = op.individuals().page_info()
            page_info.has_next()
            result = sh_db.execute(op)
            entities = result['data']['individuals']['entities']
            has_next = result['data']['individuals']['pageInfo']['hasNext']
            while has_next:
                page = args['page']
                args['page'] = page + 1
                op = Operation(SortingHatSchema.Query)
                op.individuals(**args)
                individual = op.individuals().entities()
                individual.mk()
                individual.identities().source()
                individual.identities().uuid()
                page_info = op.individuals().page_info()
                page_info.has_next()
                result = sh_db.execute(op)
                entities += result['data']['individuals']['entities']
                has_next = result['data']['individuals']['pageInfo']['hasNext']
            return entities
        except SortingHatClientError as e:
            logger.debug("[sortinghat] Error list unique identities: {}".format(e))

    @classmethod
    def search_last_modified_identities(cls, db, after):
        args = {
            'page': PAGE,
            'page_size': PAGE_SIZE,
            'filters': {
                'lastUpdated': '>' + after.strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')
            }
        }
        try:
            has_next = True
            while has_next:
                op = Operation(SortingHatSchema.Query)
                op.individuals(**args)
                individual = op.individuals().entities()
                individual.mk()
                identities = individual.identities()
                identities.uuid()
                identities.name()
                identities.email()
                identities.username()
                profile = individual.profile()
                profile.name()
                profile.email()
                profile.gender()
                profile.gender_acc()
                profile.is_bot()
                enrollments = individual.enrollments()
                enrollments.group().parent_org().name()
                enrollments.group().name()
                enrollments.group().type()
                enrollments.start()
                enrollments.end()
                page_info = op.individuals().page_info()
                page_info.has_next()
                result = db.execute(op)
                args['page'] = args['page'] + 1
                has_next = result['data']['individuals']['pageInfo']['hasNext']
                entities = result['data']['individuals']['entities']
                yield entities
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error searching identities after {}"
                         ": {}".format(after, e.errors[0]['message']))
