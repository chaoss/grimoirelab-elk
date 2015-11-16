#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# SortingHat class helper
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

from datetime import datetime
import logging

from grimoire.elk.database import Database

class SortingHat(object):

    def __init__(self, sortinghat_db, gerrit_grimoirelib_db):
        # sortinghat_db = "acs_sortinghat_mediawiki_5879"
        # sortinghat_db = "amartin_sortinghat_openstack_sh"
        # gerrit_grimoirelib_db = "amartin_bicho_gerrit_openstack_sh"

        self.sortinghat_db = sortinghat_db
        self.gerrit_grimoirelib_db = gerrit_grimoirelib_db

        self.domain2org = {}
        self.uuid2orgs = {}
        self.email2uuid = {}
        self.uuid2bot = {}
        self.username2bot = {}
        self.emailNOuuid = []


    def sortinghat_to_es(self):
        """ Load all identities data in SH in memory """

        logging.info("Loading Sorting Hat identities")

        db = Database (user = "root", passwd = "",
                       host = "localhost", port = 3306,
                       scrdb = None, shdb = self.sortinghat_db, prjdb = None)

        # Create the domain to orgs mapping
        sql = """
            SELECT domain, name
            FROM domains_organizations do
            JOIN organizations o ON o.id = do.organization_id;
        """
        domain2org_raw = db.execute(sql)
        for item in domain2org_raw:
            domain = item[0]
            org = item[1]
            self.domain2org[domain] = org

        # Create the uuids to orgs dict
        sql = """
            SELECT uuid, name, start, end
            FROM enrollments e
            JOIN organizations o ON e.organization_id = o.id
        """

        uuid2orgs_raw = db.execute(sql)

        for enrollment in uuid2orgs_raw:
            uuid = enrollment[0]
            org_name = enrollment[1]
            start = enrollment[2]
            end = enrollment[3]
            if uuid not in self.uuid2orgs: 
                self.uuid2orgs[uuid] = []
            self.uuid2orgs[uuid].append({"name":org_name,
                                         "start":start,
                                         "end":end})

        # First using the email in profile table from sorting hat
        sql = """
            SELECT p.uuid, email, is_bot, username
            FROM profiles p
            WHERE email is not NULL
            """
        profiles = db.execute(sql)

        for profile in profiles:
            uuid = profile[0]
            email = profile[1]
            is_bot = profile[2]
            username = profile[3]

            self.email2uuid[email] = uuid
            self.uuid2bot[uuid] = is_bot
            self.username2bot[username] = is_bot

        # Now using directly the grimoirelib gerrit identities
        sql = """
            SELECT uuid, email
            FROM %s.people p
            JOIN %s.people_uidentities pup ON p.id = pup.people_id
            """ % (self.gerrit_grimoirelib_db, self.gerrit_grimoirelib_db)

        profiles = db.execute(sql)

        for profile in profiles:
            uuid = profile[0]
            email = profile[1]

            self.email2uuid[email] = uuid

    def get_uuid(self, email):
        """ Get in the most efficient way the uuid (people unique identifier)
            for an email """

        uuid = None
        try:
            uuid = self.email2uuid[email]
        except:
            self.emailNOuuid.add(email)
            pass

        return uuid


    def get_org(self, uuid, action_date_str):
        """ Get in the most efficient way the organization for
            uuid in the date when an action was done  """

        org_found = "Unknown"
        action_date = datetime.strptime(action_date_str, "%Y-%m-%dT%H:%M:%S")

        try:
            orgs = self.uuid2orgs[uuid]
            # Take the org active in action_date
            for org in orgs:
                if org['start'] < action_date and org['end'] >= action_date:
                    org_found = org['name']
                    break
        except:
            # logging.info("Can't find org for " + email)
            pass

        return org_found

    def get_org_by_email(self, email, action_date_str):
        """ Get in the most efficient way the organization for
            email in the date when an action was done  """

        org_found = self.get_org(self.get_uuid(email), action_date_str)

        if org_found == "Unknown":
            # Try to get the org from the email domain
            try:
                domain = email.split('@')[1]
                org_found = self.domain2org[domain]
            except:
                pass # domain not found

        return org_found


    def get_isbot(self, uuid):
        """ Get if an uuid is a bot  """

        bot = 0 # Default uuid is not a bot
        try:
            bot = self.uuid2bot[uuid]
        except:
            # logging.info("Can't find org for " + email)
            pass

        return bot

    def get_isbot_by_username(self, username):
        """ Get if a username is a bot  """

        # TODO: Using username as key is fragile. It is not unique.
        # For example, jenkins could be the name of a non bot user

        bot = 0 # Default username is not a bot
        try:
            bot = self.username2bot[username]
        except:
            # logging.info("Can't find org for " + email)
            pass

        return bot



