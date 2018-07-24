# -*- coding: utf-8 -*-
#
# Basic class for getting data from MySQL databases
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
import pymysql

logger = logging.getLogger(__name__)


# https://github.com/jgbarah/Grimoire-demo/blob/master/grimoire-ng-data.py#L338
class Database:
    """To work with a database (likely including several schemas).
    """

    def __init__(self, user, passwd, host, port, scrdb, shdb, prjdb):
        self.user = user
        self.passwd = passwd
        self.host = host
        self.port = port
        self.scrdb = scrdb
        self.shdb = shdb
        self.prjdb = prjdb
        self.db, self.cursor = self._connect()

    def _connect(self):
        """Connect to the MySQL database.
        """

        try:
            db = pymysql.connect(user=self.user, passwd=self.passwd,
                                 host=self.host, port=self.port,
                                 db=self.shdb, use_unicode=True)
            return db, db.cursor()
        except Exception:
            logger.error("Database connection error")
            raise

    def execute(self, query):
        """Execute an SQL query with the corresponding database.
        The query can be "templated" with {scm_db} and {sh_db}.
        """

        # sql = query.format(scm_db = self.scmdb,
        #                   sh_db = self.shdb,
        #                   prj_db = self.prjdb)

        results = int(self.cursor.execute(query))
        if results > 0:
            result1 = self.cursor.fetchall()
            return result1
        else:
            return []
