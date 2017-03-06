#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
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
import hashlib
import json
import logging

import requests

from grimoire_elk.elk.elastic import ElasticSearch

from perceval.backends.core.gerrit import Gerrit

def get_params():
    args_parser = argparse.ArgumentParser(usage="usage: track_items [options]",
                                     description="Track items from different data sources.")
    args_parser.add_argument("-e", "--elastic-url", required=True,
                             help="ElasticSearch URL with raw indexes wich includes the items to track")
    args_parser.add_argument("-f", "--file", required=True, help="File with the items to track")
    args_parser.add_argument('-g', '--debug', dest='debug', action='store_true')
    return args_parser.parse_args()

def fetch_track_items(items_file_path, data_source):
    """ The file format is:

    # Upstream contributions, bitergia will crawl this and extract the relevant information
    # system is one of Gerrit, Bugzilla, Launchpad (insert more)
    ---
    -
      url: https://review.openstack.org/169836
      system: Gerrit
    """

    track_uris = []
    with open(items_file_path) as f:
        for line in f:
            if 'url: ' in line:
                ds = next(f).split('system: ')[1].strip('\n')
                if ds == data_source:
                    track_uris.append(line.split('url: ')[1].strip('\n'))
    return track_uris

# TODO: find the best way to reuse uuid from perceval
def uuid(*args):
    """Generate a UUID based on the given parameters.
    The UUID will be the SHA1 of the concatenation of the values
    from the list. The separator bewteedn these values is ':'.
    Each value must be a non-empty string, otherwise, the function
    will raise an exception.
    :param *args: list of arguments used to generate the UUID
    :returns: a universal unique identifier
    :raises ValueError: when anyone of the values is not a string,
        is empty or `None`.
    """
    def check_value(v):
        if not isinstance(v, str):
            raise ValueError("%s value is not a string instance" % str(v))
        elif not v:
            raise ValueError("value cannot be None or empty")
        else:
            return v

    s = ':'.join(map(check_value, args))

    sha1 = hashlib.sha1(s.encode('utf-8'))
    uuid_sha1 = sha1.hexdigest()

    return uuid_sha1

    s = ':'.join(map(check_value, args))

    sha1 = hashlib.sha1(s.encode('utf-8'))
    uuid_sha1 = sha1.hexdigest()

    return uuid_sha1

if __name__ == '__main__':

    args = get_params()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    logging.info("Importing track items from %s ", args.file)

    es_index = "gerrit_openstack"
    elastic = ElasticSearch(args.elastic_url, es_index)

    total = 0

    item_uris = fetch_track_items(args.file, "Gerrit")
    item_uuids = []
    commits_sha = []

    for item_uri in item_uris:
        # Get the uuid for this item_uri
        gerrit_number = item_uri.rsplit("/", 1)[1]
        origin = item_uri.rsplit("/", 1)[0]
        item_uuids.append(uuid(origin, gerrit_number))

    # Now we need for each gerrit review to find all related commits
    # TODO: testing with gerrit uuids already downloaded
    item_uuids_fake = [
        "f5f081e6d77cca2171cc08af6bbc8607974fda44",
        "fa2b18fc9a7a3f0e979122f324436b898f98bfaf",
        "b2d835892375a8fbce63c91534bcbaa2e41314bb",
        "82e51e6e6492955a36e9fe70c1c995c40040dedc",
        "53ec066614e15e5286a2b7b910824c7bc8b59d27",
        "b167be227a8c149f4eefed4c959908fa404e557c",
        "0e32134839a806d1d61468b02b073cc46a4ee787",
        "fc40b795bb4dcea0d3317ad6af31ca735cf9084d",
        "f37b8491a569127d0111d92c6b2774877f16767a",
        "26cc781cb1b6f9acaaa975f51b93a085a49bf695"
    ]

    logging.info("Total track items to be imported: %i", len(item_uris))

    # Now we need to enrich all gerrit and commits raw items and publish
    # them to the OPNFV ES enriched indexes for gerrit and git

    # total = elastic.bulk_upload(tweets, "id_str")
    r = requests.post(args.elastic_url + "/" + es_index + "/_mget",
                      data = json.dumps({"ids": item_uuids_fake}))
    r.raise_for_status()
    logging.info("Total track items to be imported: %i", len(item_uuids_fake))
    logging.info("Total track items found upstream: %i", len(r.json()["docs"]))

    for review in r.json()["docs"]:
        for patch in review['_source']['data']['patchSets']:
            commits_sha.append(patch['revision'])

    logging.info("Total commits to track %i", len(commits_sha))

    logging.info("Total track items imported: %i", total)
