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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Valerio Cosentino <valcos@bitergia.com>
#

import logging
import sys
import unittest
from grimoire_elk.elastic_items import ElasticItems


if '..' not in sys.path:
    sys.path.insert(0, '..')


class TestElasticItems(unittest.TestCase):
    """Unit tests for ElasticItems class"""

    def test_set_filter_raw(self):
        """Test whether the filter raw is properly set"""

        ei = ElasticItems(None)

        filter_raws = [
            "data.product:Firefox, for Android,data.component:Logins, Passwords and Form Fill",
            "data.product:Add-on SDK",
            "data.product:Add-on SDK,    data.component:Documentation",
            "data.product:Add-on SDK, data.component:General",
            "data.product:addons.mozilla.org Graveyard,       data.component:API",
            "data.product:addons.mozilla.org Graveyard,   data.component:Add-on Builder",
            "data.product:Firefox for Android,data.component:Build Config & IDE Support",
            "data.product:Firefox for Android,data.component:Logins, Passwords and Form Fill",
            "data.product:Mozilla Localizations,data.component:nb-NO / Norwegian Bokm\u00e5l",
            "data.product:addons.mozilla.org Graveyard,data.component:Add-on Validation"
        ]

        expected = [
            [
                {
                    "name": "data.product",
                    "value": "Firefox, for Android"
                },
                {
                    "name": "data.component",
                    "value": "Logins, Passwords and Form Fill"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Add-on SDK"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Add-on SDK"
                },
                {
                    "name": "data.component",
                    "value": "Documentation"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Add-on SDK"
                },
                {
                    "name": "data.component",
                    "value": "General"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "addons.mozilla.org Graveyard"
                },
                {
                    "name": "data.component",
                    "value": "API"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "addons.mozilla.org Graveyard"
                },
                {
                    "name": "data.component",
                    "value": "Add-on Builder"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Firefox for Android"
                },
                {
                    "name": "data.component",
                    "value": "Build Config & IDE Support"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Firefox for Android"
                },
                {
                    "name": "data.component",
                    "value": "Logins, Passwords and Form Fill"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Mozilla Localizations"
                },
                {
                    "name": "data.component",
                    "value": "nb-NO / Norwegian Bokm\u00e5l"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "addons.mozilla.org Graveyard"
                },
                {
                    "name": "data.component",
                    "value": "Add-on Validation"
                }
            ]
        ]

        for i, filter_raw in enumerate(filter_raws):
            ei.set_filter_raw(filter_raw)

            self.assertDictEqual(ei.filter_raw_dict[0], expected[i][0])

            if len(ei.filter_raw_dict) > 1:
                self.assertDictEqual(ei.filter_raw_dict[1], expected[i][1])

    def test_set_filter_raw_should(self):
        """Test whether the filter raw should is properly set"""

        ei = ElasticItems(None)

        filter_raws = [
            "data.product:Firefox, for Android,data.component:Logins, Passwords and Form Fill",
            "data.product:Add-on SDK",
            "data.product:Add-on SDK,    data.component:Documentation",
            "data.product:Add-on SDK, data.component:General",
            "data.product:addons.mozilla.org Graveyard,       data.component:API",
            "data.product:addons.mozilla.org Graveyard,   data.component:Add-on Builder",
            "data.product:Firefox for Android,data.component:Build Config & IDE Support",
            "data.product:Firefox for Android,data.component:Logins, Passwords and Form Fill",
            "data.product:Mozilla Localizations,data.component:nb-NO / Norwegian Bokm\u00e5l",
            "data.product:addons.mozilla.org Graveyard,data.component:Add-on Validation"
        ]

        expected = [
            [
                {
                    "name": "data.product",
                    "value": "Firefox, for Android"
                },
                {
                    "name": "data.component",
                    "value": "Logins, Passwords and Form Fill"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Add-on SDK"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Add-on SDK"
                },
                {
                    "name": "data.component",
                    "value": "Documentation"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Add-on SDK"
                },
                {
                    "name": "data.component",
                    "value": "General"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "addons.mozilla.org Graveyard"
                },
                {
                    "name": "data.component",
                    "value": "API"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "addons.mozilla.org Graveyard"
                },
                {
                    "name": "data.component",
                    "value": "Add-on Builder"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Firefox for Android"
                },
                {
                    "name": "data.component",
                    "value": "Build Config & IDE Support"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Firefox for Android"
                },
                {
                    "name": "data.component",
                    "value": "Logins, Passwords and Form Fill"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "Mozilla Localizations"
                },
                {
                    "name": "data.component",
                    "value": "nb-NO / Norwegian Bokm\u00e5l"
                }
            ],
            [
                {
                    "name": "data.product",
                    "value": "addons.mozilla.org Graveyard"
                },
                {
                    "name": "data.component",
                    "value": "Add-on Validation"
                }
            ]
        ]

        for i, filter_raw in enumerate(filter_raws):
            ei.set_filter_raw_should(filter_raw)

            self.assertDictEqual(ei.filter_raw_should_dict[0], expected[i][0])

            if len(ei.filter_raw_should_dict) > 1:
                self.assertDictEqual(ei.filter_raw_should_dict[1], expected[i][1])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    unittest.main()
