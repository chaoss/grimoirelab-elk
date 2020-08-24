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


from elasticsearch import Elasticsearch
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def ES2Excel():

    es = Elasticsearch()

    res = es.search(index="all_scms", body={"query": {"match_all": {}}}, size=9000)
    dict_data = []

    for hit in res['hits']['hits']:
        dict_data.append(hit['_source'])

    arr_id = []
    arr_grimoire_creation_date = []
    arr_context = []
    arr_body = []
    arr_channel = []

    for data in dict_data:
        if "https://coveralls.io" not in data["body"] \
                and "> has quit" not in data["body"] \
                and "> has joined #" not in data["body"] \
                and "> has left #" not in data["body"]:
            arr_id.append(str(data["id"]))
            arr_grimoire_creation_date.append(data["grimoire_creation_date"])
            arr_context.append(data["context"])
            arr_body.append(data["body"])
            arr_channel.append(data["data_source"])

    df = pd.DataFrame(
        {
            'id': arr_id,
            'grimoire_creation_date': arr_grimoire_creation_date,
            'context': arr_context,
            'body': arr_body,
            'channel': arr_channel
        }
    )
    convert_csv(df)
    convert_xlsx(df)
    Excel2GSheets(spreadsheetname='Testing')


def convert_csv(df):
    df.to_csv(csv_file_name, index=None)


def convert_xlsx(df):
    df.to_excel(xls_file_name, index=None)


def Excel2GSheets(spreadsheetname):
    """
    This method uploads the csv data (csv_file_name) to a Google Sheets with the name spreadsheetname

    Input: Name of the Google spreadsheet
    Output: GSheets

    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name("SCMS-creds.json", scope)

    client = gspread.authorize(creds)

    spreadsheet = client.open(spreadsheetname)
    content = open(csv_file_name, 'r').read().encode("utf-8")
    client.import_csv(spreadsheet.id, content)


def main():
    global xls_file_name, csv_file_name
    filename = "all_scms_enriched"
    csv_file_name = filename + ".csv"
    xls_file_name = filename + ".xlsx"
    ES2Excel()


if __name__ == "__main__":
    main()
