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
#     Valerio Cosentino <valcos@bitergia.com>
#


class BaseError(Exception):
    """Base class for GrimoireELK exceptions.

    Derived classes can overwrite the error message declaring ``message``
    property.
    """
    message = 'GrimoireELK base error'

    def __init__(self, **kwargs):
        super().__init__()
        self.msg = self.message % kwargs

    def __str__(self):
        return self.msg


class ELKError(BaseError):
    """Generic error for elk error"""

    message = "%(cause)s"


class ElasticError(BaseError):
    """Generic error for elastic"""

    message = "%(cause)s"
