#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# This script manage the information about Mordred projects
#
# Copyright (C) 2016 Bitergia
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
#   Quan Zhou <quan@bitergia.com>
#


import argparse
import configparser
import datetime
import json
import os
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


VERSION = "version 0.2"


def write_file(filename, data, type):
    with open(filename, 'w+') as f:
        if type == "json":
            json.dump(data, f)
        else:
            for log in data:
                f.write(data[log])

def read_arguments():
    desc="Checks data freshness in Elasticsearch databases"
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=desc)

    parser.add_argument("-v", "--version",
                        action="version",
                        version=VERSION,
                        help="show program's version number and exit")
    parser.add_argument("-s", "--send",
                        choices=['true', 'false'],
                        help="Sends the information to the mail. Disabled by default.")
    parser.add_argument("-f", "--file",
                        action="store",
                        dest="file",
                        help="Generates a file with the content of the report.")
    parser.add_argument("config_file",
                        action="store",
                        help="config file")
    parser.add_argument("log_file",
                        action="store",
                        help="exit log file")

    args = parser.parse_args()

    return args

def get_query(conf, index, project):
    query ="curl -k -u "+conf['user']+":"+conf['password']+" -XGET 'https://"+conf['es']+"/"+index+"/_search?q=*' -d "
    query += """ '{
      "filter" : {
        "match_all" : { }
      },
      "sort": [
        {
          "metadata__timestamp": {
            "order": "desc"
          }
        }
      ],
      "size": 1
    }' 2>/dev/null"""

    return query

def get_timestamp(conf, project):
    dict_indexes = {}

    for index in conf['backends']:
        index = index.split('\n')[0]

        query = get_query(conf, index, project)

        try:
            content = json.loads(os.popen(query).read())
            timestamp = content['hits']['hits'][0]['_source']['metadata__timestamp'].split('.')[0]
            dict_indexes[index] = timestamp
        except KeyError:
            dict_indexes[index] = "null"

    return dict_indexes

def get_data_smells(datas):
    dict_smells = {}

    iso_format = "%Y-%m-%dT%H:%M:%S"

    today = datetime.datetime.utcnow().isoformat()
    today = datetime.datetime.strptime(today.split('.')[0], iso_format)

    # Get difference between today and the day of database
    for backend in datas:
        date = datas[backend]
        if date != "null":
            date_smell = datetime.datetime.strptime(date, iso_format)
            smell = str(today - date_smell)
            dict_smells[backend] = smell

    return dict_smells

def get_logs(datas, project, thresholds):
    today = datetime.datetime.utcnow().isoformat()
    text = "\n** "+project+" **\n"

    for backend in datas:
        if backend in thresholds:
            text += today+"\t"+backend+": "+datas[backend]+"\t(threshold="+thresholds[backend]+")\n"
        else:
            text += today+"\t"+backend+": "+datas[backend]+"\t(threshold="+thresholds['default']+")\n"

    return text

def compose_text(data, conf, thresholds):
    text = "\nES datafreshness\n"

    for project in data:
        text += "\n** "+project+" **\n"
        # dict with max days peer backend. If len(orgs) == 0 then everything ok
        all_ok = True
        for backend in data[project]:
            try:
                int_time = int(data[project][backend].split(" days")[0])
            except ValueError:
                int_time = 0

            threshold = thresholds['default']
            if backend in thresholds:

                threshold = thresholds[backend]

            if int_time > int(threshold):
                all_ok = False
                text += backend+"\t"+str(int_time)+" days (threshold="+threshold+")\n"

        if all_ok:
            text += "everything ok :)\n"

    return text

def send_email(msg_from, msg_to, text):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "ES datafreshness"
    msg['From'] = msg_from
    msg['To'] = msg_to

    msg_from=""

    text = 'The owl detected expired SQL data ..\n\n' + str(text)
    body = MIMEText(text, 'plain')

    msg.attach(body)

    # Send the email via our own SMTP server.
    s = smtplib.SMTP('localhost')
    s.sendmail(msg_from, msg_to, msg.as_string())
    s.quit()

def get_conf(conf_name):
    Config = configparser.ConfigParser()
    Config.read(conf_name)

    conf = {}
    for project in Config.sections():
        if project != "notification":
            conf[project] = {}
            indexes = Config[project]['indexes'].replace(", ", ",")
            conf[project]['backends'] = indexes.split(',')
            conf[project]['es'] = Config[project]['es']
            conf[project]['user'] = Config[project]['user']
            conf[project]['password'] = Config[project]['password']
            if Config.has_option(project, 'threshold'):
                thresholds = Config[project]['threshold'].replace(", ", ",")
                conf[project]['threshold'] = thresholds.split(',')

    email_to = Config['notification']['email_to']
    email_from = Config['notification']['email_from']
    threshold = Config['notification']['default_threshold']

    return conf, email_to, email_from, threshold

def main():
    args = read_arguments()

    log_name = args.log_file
    conf_name = args.config_file

    conf, email_to, email_from, threshold = get_conf(conf_name)

    smells = {}
    logs = {}
    thresholds = {}
    thresholds['default'] = threshold
    for project in conf:
        time = get_timestamp(conf[project], project)

        smell = get_data_smells(time)
        smells[project] = smell

        if 'threshold' in conf[project]:
            for t in conf[project]['threshold']:
                t = t.split(':')
                thresholds[t[0]] = t[1]
        log = get_logs(smell, project, thresholds)

        logs[project] = log

    write_file(log_name, logs, "logs")

    if args.send and args.send == 'true':
        text = compose_text(smells, conf, thresholds)
        send_email(email_from, email_to, text)

    if args.file:
        write_file(args.file, smells, "json")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        s = "Error: %s\n" % str(e)
        sys.stderr.write(s)
        sys.exit(1)
