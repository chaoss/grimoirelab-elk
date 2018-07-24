# -*- coding: utf-8 -*-
#
# GrimoireLib projects support
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

import logging

from .database import Database


logger = logging.getLogger(__name__)


class GrimoireLibProjects(object):

    def __init__(self, projects_db, repository):
        self.projects_db = projects_db
        self.repository = repository

    def get_projects(self):
        """ Get the projects list from database """

        repos_list = []

        gerrit_projects_db = self.projects_db

        db = Database(user="root", passwd="", host="localhost", port=3306,
                      scrdb=None, shdb=gerrit_projects_db, prjdb=None)

        sql = """
            SELECT DISTINCT(repository_name)
            FROM project_repositories
            WHERE data_source='scr'
        """

        repos_list_raw = db.execute(sql)

        # Convert from review.openstack.org_openstack/rpm-packaging-tools to
        # openstack_rpm-packaging-tools
        for repo in repos_list_raw:
            # repo_name = repo[0].replace("review.openstack.org_","")
            repo_name = repo[0].replace(self.repository + "_", "")
            repos_list.append(repo_name)

        return repos_list
