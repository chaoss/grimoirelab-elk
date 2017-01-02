#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Twitter JSON files importer to Elastic Search
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
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import argparse
import json
import logging
import os

from dateutil import parser

from grimoire_elk.elk.elastic import ElasticSearch

def get_params():
    parser = argparse.ArgumentParser(usage="usage: twitter2es [options]",
                                     description="Import tweets in ElasticSearch")
    parser.add_argument("-d", "--json-dir", required=True, help="Directory with the tweets JSON files")
    parser.add_argument("-e", "--elastic-url", required=True, help="ElasticSearch URL")
    parser.add_argument("-i", "--index", required=True, help="ElasticSearch index in which to import the tweets")
    parser.add_argument('-g', '--debug', dest='debug', action='store_true')
    args = parser.parse_args()

    return args

def logstash_fields(tweet):
    """ Return the logstash generated fields for this tweet"""
    fields = {
        "@version": "1",
        "@timestamp": parser.parse(tweet['created_at']).isoformat()
    }

    return fields

def fetch_tweets(json_dir):
    for filename in os.listdir(json_dir):
        with open(os.path.join(json_dir, filename)) as json_file:
            # We have 100 tweets approach per file
            tweets = json.load(json_file)['statuses']
            for tweet in tweets:
                tweet.update(logstash_fields(tweet))
                yield tweet

if __name__ == '__main__':

    args = get_params()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    logging.info("Importing tweets from %s to %s/%s", args.json_dir, args.elastic_url, args.index)

    elastic = ElasticSearch(args.elastic_url, args.index)

    total = 0

    first_date = None
    last_date = None

    ids = []
    tweets = []

    for tweet in fetch_tweets(args.json_dir):
        # Check first and last dates
        tweet_date = parser.parse(tweet['created_at'])
        if not first_date or tweet_date <= first_date:
            first_date = tweet_date
        if not last_date or tweet_date >= last_date:
            last_date = tweet_date
        total += 1
        tweets.append(tweet)
        ids.append(tweet["id_str"])

    logging.info("%s -> %s", first_date, last_date)
    logging.info("Total tweets to be imported: %i", len(ids))
    logging.info("Total unique tweets to be imported: %i", len(set(ids)))

    # Upload data to ES. The id is: "id_str" and the type "items"
    total = elastic.bulk_upload(tweets, "id_str")

    logging.info("Total tweets imported: %i", total)
