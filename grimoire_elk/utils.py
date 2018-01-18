#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Grimoire general utils
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

import argparse
import logging
import requests
import sys

from dateutil import parser

from .ocean.elastic import ElasticOcean

# Connectors for Ocean
from .ocean.askbot import AskbotOcean
from .ocean.bugzilla import BugzillaOcean
from .ocean.bugzillarest import BugzillaRESTOcean
from .ocean.confluence import ConfluenceOcean
from .ocean.crates import CratesOcean
from .ocean.discourse import DiscourseOcean
from .ocean.dockerhub import DockerHubOcean
from .ocean.functest import FunctestOcean
from .ocean.gerrit import GerritOcean
from .ocean.git import GitOcean
from .ocean.github import GitHubOcean
from .ocean.jenkins import JenkinsOcean
from .ocean.jira import JiraOcean
from .ocean.kitsune import KitsuneOcean
from .ocean.mbox import MBoxOcean
from .ocean.mediawiki import MediaWikiOcean
from .ocean.meetup import MeetupOcean
from .ocean.mozillaclub import MozillaClubOcean
from .ocean.nntp import NNTPOcean
from .ocean.phabricator import PhabricatorOcean
from .ocean.redmine import RedmineOcean
from .ocean.remo2 import ReMoOcean
from .ocean.rss import RSSOcean
from .ocean.slack import SlackOcean
from .ocean.stackexchange import StackExchangeOcean
from .ocean.supybot import SupybotOcean
from .ocean.telegram import TelegramOcean
from .ocean.twitter import TwitterOcean

# Connectors for EnrichOcean
from .elk.askbot import AskbotEnrich
from .elk.bugzilla import BugzillaEnrich
from .elk.bugzillarest import BugzillaRESTEnrich
from .elk.confluence import ConfluenceEnrich
from .elk.crates import CratesEnrich
from .elk.dockerhub import DockerHubEnrich
from .elk.discourse import DiscourseEnrich
from .elk.functest import FunctestEnrich
from .elk.git import GitEnrich
from .elk.github import GitHubEnrich
from .elk.gerrit import GerritEnrich
from .elk.gmane import GmaneEnrich
from .elk.hyperkitty import HyperKittyEnrich
from .elk.jenkins import JenkinsEnrich
from .elk.jira import JiraEnrich
from .elk.kitsune import KitsuneEnrich
from .elk.mbox import MBoxEnrich
from .elk.mediawiki import MediaWikiEnrich
from .elk.meetup import MeetupEnrich
from .elk.mozillaclub import MozillaClubEnrich
from .elk.nntp import NNTPEnrich
from .elk.phabricator import PhabricatorEnrich
from .elk.redmine import RedmineEnrich
# from .elk.remo import ReMoEnrich
from .elk.remo2 import ReMoEnrich
from .elk.rss import RSSEnrich
from .elk.pipermail import PipermailEnrich
from .elk.slack import SlackEnrich
from .elk.stackexchange import StackExchangeEnrich
from .elk.supybot import SupybotEnrich
from .elk.telegram import TelegramEnrich
from .elk.twitter import TwitterEnrich

# Connectors for Perceval
from perceval.backends.core.askbot import Askbot, AskbotCommand
from perceval.backends.core.bugzilla import Bugzilla, BugzillaCommand
from perceval.backends.core.bugzillarest import BugzillaREST, BugzillaRESTCommand
from perceval.backends.core.discourse import Discourse, DiscourseCommand
from perceval.backends.core.confluence import Confluence, ConfluenceCommand
from perceval.backends.core.dockerhub import DockerHub, DockerHubCommand
from perceval.backends.core.gerrit import Gerrit, GerritCommand
from perceval.backends.core.git import Git, GitCommand
from perceval.backends.core.github import GitHub, GitHubCommand
from perceval.backends.core.gmane import Gmane, GmaneCommand
from perceval.backends.core.jenkins import Jenkins, JenkinsCommand
from perceval.backends.core.jira import Jira, JiraCommand
from perceval.backends.core.hyperkitty import HyperKitty, HyperKittyCommand
from perceval.backends.core.mbox import MBox, MBoxCommand
from perceval.backends.core.mediawiki import MediaWiki, MediaWikiCommand
from perceval.backends.core.meetup import Meetup, MeetupCommand
from perceval.backends.core.nntp import NNTP, NNTPCommand
from perceval.backends.core.phabricator import Phabricator, PhabricatorCommand
from perceval.backends.core.pipermail import Pipermail, PipermailCommand
from perceval.backends.core.redmine import Redmine, RedmineCommand
from perceval.backends.core.rss import RSS, RSSCommand
from perceval.backends.core.slack import Slack, SlackCommand
from perceval.backends.core.stackexchange import StackExchange, StackExchangeCommand
from perceval.backends.core.supybot import Supybot, SupybotCommand
from perceval.backends.core.telegram import Telegram, TelegramCommand
from perceval.backends.mozilla.crates import Crates, CratesCommand
from perceval.backends.mozilla.kitsune import Kitsune, KitsuneCommand
from perceval.backends.mozilla.mozillaclub import MozillaClub, MozillaClubCommand
from perceval.backends.mozilla.remo import ReMo, ReMoCommand
from perceval.backends.opnfv.functest import Functest, FunctestCommand


from .elk.elastic import ElasticSearch
from .elk.elastic import ElasticConnectException


logger = logging.getLogger(__name__)

kibiter_version = None


def get_connector_from_name(name):
    found = None
    connectors = get_connectors()

    for cname in connectors:
        if cname == name:
            found = connectors[cname]

    return found


def get_connector_name(cls):
    found = None
    connectors = get_connectors()

    for cname in connectors:
        for con in connectors[cname]:
            if cls == con:
                if found:
                    # The canonical name is included in the classname
                    if cname in cls.__name__.lower():
                        found = cname
                else:
                    found = cname
    return found


def get_connector_name_from_cls_name(cls_name):
    found = None
    connectors = get_connectors()

    for cname in connectors:
        for con in connectors[cname]:
            if not con:
                continue
            if cls_name == con.__name__:
                if found:
                    # The canonical name is included in the classname
                    if cname in con.__name__.lower():
                        found = cname
                else:
                    found = cname
    return found


def get_connectors():

    return {"askbot": [Askbot, AskbotOcean, AskbotEnrich, AskbotCommand],
            "bugzilla": [Bugzilla, BugzillaOcean, BugzillaEnrich, BugzillaCommand],
            "bugzillarest": [BugzillaREST, BugzillaRESTOcean, BugzillaRESTEnrich, BugzillaRESTCommand],
            "confluence": [Confluence, ConfluenceOcean, ConfluenceEnrich, ConfluenceCommand],
            "crates": [Crates, CratesOcean, CratesEnrich, CratesCommand],
            "discourse": [Discourse, DiscourseOcean, DiscourseEnrich, DiscourseCommand],
            "dockerhub": [DockerHub, DockerHubOcean, DockerHubEnrich, DockerHubCommand],
            "functest": [Functest, FunctestOcean, FunctestEnrich, FunctestCommand],
            "gerrit": [Gerrit, GerritOcean, GerritEnrich, GerritCommand],
            "git": [Git, GitOcean, GitEnrich, GitCommand],
            "github": [GitHub, GitHubOcean, GitHubEnrich, GitHubCommand],
            "gmane": [Gmane, MBoxOcean, GmaneEnrich, GmaneCommand],
            "hyperkitty": [HyperKitty, MBoxOcean, HyperKittyEnrich, HyperKittyCommand],
            "jenkins": [Jenkins, JenkinsOcean, JenkinsEnrich, JenkinsCommand],
            "jira": [Jira, JiraOcean, JiraEnrich, JiraCommand],
            "kitsune": [Kitsune, KitsuneOcean, KitsuneEnrich, KitsuneCommand],
            "mbox": [MBox, MBoxOcean, MBoxEnrich, MBoxCommand],
            "mediawiki": [MediaWiki, MediaWikiOcean, MediaWikiEnrich, MediaWikiCommand],
            "meetup": [Meetup, MeetupOcean, MeetupEnrich, MeetupCommand],
            "mozillaclub": [MozillaClub, MozillaClubOcean, MozillaClubEnrich, MozillaClubCommand],
            "nntp": [NNTP, NNTPOcean, NNTPEnrich, NNTPCommand],
            "phabricator": [Phabricator, PhabricatorOcean, PhabricatorEnrich, PhabricatorCommand],
            "pipermail": [Pipermail, MBoxOcean, PipermailEnrich, PipermailCommand],
            "redmine": [Redmine, RedmineOcean, RedmineEnrich, RedmineCommand],
            "remo": [ReMo, ReMoOcean, ReMoEnrich, ReMoCommand],
            "rss": [RSS, RSSOcean, RSSEnrich, RSSCommand],
            "slack": [Slack, SlackOcean, SlackEnrich, SlackCommand],
            "stackexchange": [StackExchange, StackExchangeOcean,
                              StackExchangeEnrich, StackExchangeCommand],
            "supybot": [Supybot, SupybotOcean, SupybotEnrich, SupybotCommand],
            "telegram": [Telegram, TelegramOcean, TelegramEnrich, TelegramCommand],
            "twitter": [None, TwitterOcean, TwitterEnrich, None]
            }  # Will come from Registry


def get_elastic(url, es_index, clean=None, backend=None):

    mapping = None

    if backend:
        backend.set_elastic_url(url)
#        mapping = backend.get_elastic_mappings()
        mapping = backend.mapping
        analyzers = backend.get_elastic_analyzers()
    try:
        insecure = True
        elastic = ElasticSearch(url=url, index=es_index, mappings=mapping,
                                clean=clean, insecure=insecure,
                                analyzers=analyzers)

    except ElasticConnectException:
        logger.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    return elastic


def get_kibiter_version(url):
    """
        Return kibiter major number version

        The url must point to the Elasticsearch used by Kibiter
    """

    config_url = '.kibana/config/_search'
    # Avoid having // in the URL because ES will fail
    if url[-1] != '/':
        url += "/"
    url += config_url
    r = requests.get(url)
    r.raise_for_status()

    if len(r.json()['hits']['hits']) == 0:
        logger.error("Can not get the Kibiter version")
        return None

    version = r.json()['hits']['hits'][0]['_id']
    # 5.4.0-SNAPSHOT
    major_version = version.split(".", 1)[0]
    return major_version


def config_logging(debug):

    if debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        logging.debug("Debug mode activated")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    # Per commit log is too verbose
    logging.getLogger("perceval.backends.core.git").setLevel(logging.WARNING)


ARTHUR_USAGE_MSG = ''
ARTHUR_DESC_MSG = ''
ARTHUR_EPILOG_MSG = ''


def get_params_parser():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(usage=ARTHUR_USAGE_MSG,
                                     description=ARTHUR_DESC_MSG,
                                     epilog=ARTHUR_EPILOG_MSG,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     add_help=False)

    ElasticOcean.add_params(parser)

    parser.add_argument('-h', '--help', action='help',
                        help=argparse.SUPPRESS)
    parser.add_argument('-g', '--debug', dest='debug',
                        action='store_true',
                        help=argparse.SUPPRESS)
    parser.add_argument("--no_incremental", action='store_true',
                        help="don't use last state for data source")
    parser.add_argument("--fetch_cache", action='store_true',
                        help="Use cache for item retrieval")
    parser.add_argument("--enrich", action='store_true',
                        help="Enrich items after retrieving")
    parser.add_argument("--enrich_only", action='store_true',
                        help="Only enrich items (DEPRECATED, use --only-enrich)")
    parser.add_argument("--only-enrich", dest='enrich_only', action='store_true',
                        help="Only enrich items")
    parser.add_argument("--filter-raw", dest='filter_raw',
                        help="Filter raw items. Format: field:value")
    parser.add_argument("--filters-raw-prefix", nargs='*',
                        help="Filter raw items with prefix filter. Format: field:value field:value ...")
    parser.add_argument("--events-enrich", dest='events_enrich', action='store_true',
                        help="Enrich events in items")
    parser.add_argument('--index', help="Ocean index name")
    parser.add_argument('--index-enrich', dest="index_enrich", help="Ocean enriched index name")
    parser.add_argument('--db-user', help="User for db connection (default to root)",
                        default="root")
    parser.add_argument('--db-password', help="Password for db connection (default empty)",
                        default="")
    parser.add_argument('--db-host', help="Host for db connection (default to mariadb)",
                        default="mariadb")
    parser.add_argument('--db-projects-map', help="Projects Mapping DB")
    parser.add_argument('--json-projects-map', help="Projects Mapping JSON file")
    parser.add_argument('--project', help="Project for the repository (origin)")
    parser.add_argument('--refresh-projects', action='store_true', help="Refresh projects in enriched items")
    parser.add_argument('--db-sortinghat', help="SortingHat DB")
    parser.add_argument('--only-identities', action='store_true', help="Only add identities to SortingHat DB")
    parser.add_argument('--refresh-identities', action='store_true', help="Refresh identities in enriched items")
    parser.add_argument('--author_id', nargs='*', help="Field author_ids to be refreshed")
    parser.add_argument('--author_uuid', nargs='*', help="Field author_uuids to be refreshed")
    parser.add_argument('--github-token', help="If provided, github usernames will be retrieved in git enrich.")
    parser.add_argument('--jenkins-rename-file', help="CSV mapping file with nodes renamed schema.")
    parser.add_argument('--studies', action='store_true', help="Execute studies after enrichment.")
    parser.add_argument('--only-studies', action='store_true', help="Execute only studies.")
    parser.add_argument('--bulk-size', default=1000, type=int,
                        help="Number of items per bulk request to Elasticsearch.")
    parser.add_argument('--scroll-size', default=100, type=int,
                        help="Number of items to get from Elasticsearch when scrolling.")
    parser.add_argument('--arthur', action='store_true', help="Read items from arthur redis queue")
    parser.add_argument('backend', help=argparse.SUPPRESS)
    parser.add_argument('backend_args', nargs=argparse.REMAINDER,
                        help=argparse.SUPPRESS)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    return parser


def get_params():
    ''' Get params definition from ElasticOcean and from all the backends '''

    parser = get_params_parser()
    args = parser.parse_args()

    return args


def get_time_diff_days(start_txt, end_txt):
    ''' Number of days between two days  '''

    if start_txt is None or end_txt is None:
        return None

    start = parser.parse(start_txt)
    end = parser.parse(end_txt)

    seconds_day = float(60 * 60 * 24)
    diff_days = \
        (end - start).total_seconds() / seconds_day
    diff_days = float('%.2f' % diff_days)

    return diff_days
