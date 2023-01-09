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
#   Jose Javier Merchante Picazo <jjmerchante@cauldron.io>
#

import hashlib


class Identities:
    """Class for managing identities.

    This class will be subclassed by backends,
    which will provide specific implementations.
    """

    @staticmethod
    def _hash(name):
        sha1 = hashlib.sha1(name.encode('UTF-8', errors="surrogateescape"))
        return sha1.hexdigest()

    @classmethod
    def anonymize_item(cls, item):
        """Remove or hash the fields that contain personal information"""

        pass
