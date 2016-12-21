#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
#   Recovers tweets from Twitter Search API and stores them
#   in disk using Twitter JSON response format. Follows
#   Twitter recommended best practices to recover tweets
#   based on their ids: https://dev.twitter.com/rest/public/timelines
#   Search API doc: https://dev.twitter.com/rest/reference/get/search/tweets
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
#     Alberto Pérez García-Plaza <alpgarcia@bitergia.com>
#


import argparse
import datetime
import logging
import requests
import sys
import time

from requests_oauthlib import OAuth1


TWITTER_SEARCH_DESC_MSG = \
    """Download tweets from Twitter Search API. If neither since_id
    nor max_id are specified, all available tweets for the given
    query will be retrieved."""

URL_CREDS = 'https://api.twitter.com/1.1/account/verify_credentials.json'
URL_SEARCH_TWEETS = 'https://api.twitter.com/1.1/search/tweets.json'

# Logging formats
LOG_FORMAT = "[%(asctime)s] - %(message)s"
DEBUG_LOG_FORMAT = "[%(asctime)s - %(name)s - %(levelname)s] - %(message)s"

SINCE_ID_PARAM = 'since_id'
MAX_ID_PARAM = 'max_id'

def main():

    args = parse_args()

    configure_logging(args.debug)

    # Get OAuth
    logging.info('Verifying credentials')
    logging.debug('API_KEY: %s', args.api_key)
    logging.debug('API_SECRET: %s', args.api_secret)
    logging.debug('ACCESS_TOKEN: %s', args.access_token)
    logging.debug('ACCESS_TOKEN_SECRET: %s', args.token_secret)

    auth = OAuth1(args.api_key, args.api_secret, args.access_token,
                  args.token_secret)
    requests.get(URL_CREDS, auth=auth)

    # Search params
    params = {'q': args.query,
              'count': args.count}

    if args.since_id is not None:
        params[SINCE_ID_PARAM] = args.since_id

    if args.max_id is not None:
        params[MAX_ID_PARAM] = args.max_id

    logging.info('Querying twitter for: %s', args.query)

    i = 0
    total_tweets = 0
    newest_id = 0
    while True:

        # Get Tweets
        logging.info('SINCE_ID: %s ',
            params[SINCE_ID_PARAM] if SINCE_ID_PARAM in params else '')
        logging.info('MAX_ID:   %s',
            params[MAX_ID_PARAM] if MAX_ID_PARAM in params else '')

        r = requests.get(URL_SEARCH_TWEETS, auth=auth, params=params)

        if r.status_code != 200:
            logging.debug('Reponse %s', r.status_code)
            logging.debug(r.headers)
            logging.debug(r.content)

            waiting_time = int(r.headers['x-rate-limit-reset']) - datetime.datetime.now().timestamp() + 1
            if waiting_time < 0:
                waiting_time = 0
            logging.info('Rate limit reached: %s seconds to reset...zZz',
                         waiting_time)
            time.sleep(waiting_time)
            continue

        n_tweets = len(r.json()['statuses'])
        total_tweets = total_tweets + n_tweets
        logging.info('Retrieved %s - Total %s', n_tweets, total_tweets)

        if n_tweets == 0:
            break

        # Write JSON to disk
        fo = open('%s/%s_%s.json' % (args.path, args.file_prefix, i), "wb")
        logging.info('Writing output to: %s', fo.name)
        fo.write(r.content)
        fo.close()

        # Get the minimum ID recovered
        # As Twitter returns most recent first, we have to
        # paginate results backwards in time, from newer to older
        # See: https://dev.twitter.com/rest/public/timelines
        if MAX_ID_PARAM in params:
            min_id = params[MAX_ID_PARAM]
        else:
            min_id =  sys.maxsize

        for tweet in r.json()['statuses']:
            current_id = tweet['id']
            if current_id < min_id:
                min_id = current_id
            if current_id > newest_id:
                newest_id = current_id

        # max_id is inclusive, so we have to substract 1 to avoid retrieving
        # the same tweet twice
        params[MAX_ID_PARAM] = min_id - 1

        i = i + 1

    logging.info('Tweets retrieved: %s.', total_tweets)
    if total_tweets > 0:
        logging.info('This is the end. Newest id: %s', newest_id)



def parse_args():
    """Parse arguments from the command line"""

    parser = argparse.ArgumentParser(description=TWITTER_SEARCH_DESC_MSG)

    parser.add_argument('-k', '--api-key', dest='api_key', required=True,
                        help='twitter API key')
    parser.add_argument('-s', '--api-secret', dest='api_secret', required=True,
                        help='twitter API secret')
    parser.add_argument('-a', '--access-token', dest='access_token',
                        required=True, help='twitter access token')
    parser.add_argument('-t', '--token-secret', dest='token_secret',
                        required=True, help='twitter access token secret')

    parser.add_argument('-q', '--query', dest='query',
                        required=True,
                        help='A UTF-8, URL-encoded search query of 1,000'
                        ' characters maximum, including operators. Queries'
                        ' may additionally be limited by complexity.')

    parser.add_argument('-i', '--since-id', dest='since_id', type=int,
                        help='since id. Use it to recover newer tweets. ')
    parser.add_argument('-m', '--max-id', dest='max_id', type=int,
                        help='max id. Use it to recover older tweets')

    parser.add_argument('-f', '--file-prefix', dest='file_prefix',
                        required=True,
                        help='file name prefix for tweet files')

    parser.add_argument('-p', '--path', dest='path', default='tweets',
                        help='file path for tweet files')
    parser.add_argument('-c', '--count', dest='count', default=100,
                        help='twitter API secret')

    parser.add_argument('-g', '--debug', dest='debug',
                        action='store_true')

    return parser.parse_args()


def configure_logging(debug=False):
    """Configure logging
    The function configures log messages. By default, log messages
    are sent to stderr. Set the parameter `debug` to activate the
    debug mode.
    :param debug: set the debug mode
    """
    if not debug:
        logging.basicConfig(level=logging.INFO,
                            format=LOG_FORMAT)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('urrlib3').setLevel(logging.WARNING)
    else:
        logging.basicConfig(level=logging.DEBUG,
                            format=DEBUG_LOG_FORMAT)



if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        s = "\n\nReceived Ctrl-C or other break signal. Exiting.\n"
        sys.stdout.write(s)
        sys.exit(0)
    except RuntimeError as e:
        s = "Error: %s\n" % str(e)
        sys.stderr.write(s)
        sys.exit(1)
