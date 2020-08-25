#!/usr/bin/env python3
#
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2020 Bitergia
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
#   Ria Gupta <ria18405@iiitd.ac.in>
#


import pandas as pd
import json


def formatexcel(spreadsheetname):
    scms_df = pd.read_csv(spreadsheetname)
    formatted_df = scms_df[['id', 'grimoire_creation_date', 'context', 'body', 'channel', 'Category', 'Weight']]
    formatted_df['scms_tags'] = ''
    for j in range(len(scms_df)):
        scms_tags = []
        for i in range(1, 6):
            x = scms_df["Tag {0}".format(i)][j]
            if isinstance(x, str):
                scms_tags.append(scms_df["Tag {0}".format(i)][j])
        formatted_df['scms_tags'][j] = scms_tags
    formatted_df.to_csv("formatted.csv", index=False)
    Excel2JSON("formatted.csv")


def Excel2JSON(tagged_file):
    """
    Input: Tagged CSV file containing Weight, Category, scms_tag

    Output: formatted json string (extra_data.json)

    """
    tagged_file_df = pd.read_csv(tagged_file)
    json_str = '['
    for i in range(len(tagged_file_df)):
        d = json.dumps(tagged_file_df['scms_tags'][i])
        d = d[1:-1].replace("\'", '"')

        json_str += '{{"conditions":[{{"field":"id","value":"{0}"}}],' \
                    '"set_extra_fields":[{{"field":"scms_tags","value":{1}}}]}},'.format(tagged_file_df['id'][i], d)
        json_str += '{{"conditions":[{{"field":"id","value":"{0}"}}],' \
                    '"set_extra_fields":[{{"field":"Weight","value":{1}}}]}},'.format(tagged_file_df['id'][i],
                                                                                      int(tagged_file_df['Weight'][i]))
        json_str += '{{"conditions":[{{"field":"id","value":"{0}"}}],' \
                    '"set_extra_fields":[{{"field":"Category","value":"{1}"}}]}},'.format(tagged_file_df['id'][i],
                                                                                          tagged_file_df['Category'][i])

    json_str = json_str[:-1] + ']'
    json_list = json.loads(json_str)

    json_file = open("extra_data.json", 'w')
    json_file.write(json.dumps(json_list, indent=4))


def main():
    formatexcel(spreadsheetname="Testing.csv")


if __name__ == "__main__":
    main()
