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
import sys

import requests
from dateutil import parser

from grimoire_elk.elastic import ElasticConnectException
from grimoire_elk.elastic import ElasticSearch
# Connectors for Perceval
from grimoire_elk.raw.hyperkitty import HyperKittyOcean
from perceval.backends.core.askbot import Askbot, AskbotCommand
from perceval.backends.core.bugzilla import Bugzilla, BugzillaCommand
from perceval.backends.core.bugzillarest import BugzillaREST, BugzillaRESTCommand
from perceval.backends.core.confluence import Confluence, ConfluenceCommand
from perceval.backends.core.discourse import Discourse, DiscourseCommand
from perceval.backends.core.dockerhub import DockerHub, DockerHubCommand
from perceval.backends.core.gerrit import Gerrit, GerritCommand
from perceval.backends.core.git import Git, GitCommand
from perceval.backends.core.github import GitHub, GitHubCommand
from perceval.backends.core.gitlab import GitLab, GitLabCommand
from perceval.backends.core.googlehits import GoogleHits, GoogleHitsCommand
from perceval.backends.core.groupsio import Groupsio, GroupsioCommand
from perceval.backends.core.hyperkitty import HyperKitty, HyperKittyCommand
from perceval.backends.core.jenkins import Jenkins, JenkinsCommand
from perceval.backends.core.jira import Jira, JiraCommand
from perceval.backends.core.mattermost import Mattermost, MattermostCommand
from perceval.backends.core.mbox import MBox, MBoxCommand
from perceval.backends.core.mediawiki import MediaWiki, MediaWikiCommand
from perceval.backends.core.meetup import Meetup, MeetupCommand
from perceval.backends.core.nntp import NNTP, NNTPCommand
from perceval.backends.core.phabricator import Phabricator, PhabricatorCommand
from perceval.backends.core.pipermail import Pipermail, PipermailCommand
from perceval.backends.core.twitter import Twitter, TwitterCommand
from perceval.backends.puppet.puppetforge import PuppetForge, PuppetForgeCommand
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
# Connectors for EnrichOcean
from .enriched.askbot import AskbotEnrich
from .enriched.bugzilla import BugzillaEnrich
from .enriched.bugzillarest import BugzillaRESTEnrich
from .enriched.confluence import ConfluenceEnrich
from .enriched.crates import CratesEnrich
from .enriched.discourse import DiscourseEnrich
from .enriched.dockerhub import DockerHubEnrich
from .enriched.functest import FunctestEnrich
from .enriched.gerrit import GerritEnrich
from .enriched.git import GitEnrich
from .enriched.github import GitHubEnrich
from .enriched.gitlab import GitLabEnrich
from .enriched.google_hits import GoogleHitsEnrich
from .enriched.groupsio import GroupsioEnrich
from .enriched.hyperkitty import HyperKittyEnrich
from .enriched.jenkins import JenkinsEnrich
from .enriched.jira import JiraEnrich
from .enriched.kitsune import KitsuneEnrich
from .enriched.mattermost import MattermostEnrich
from .enriched.mbox import MBoxEnrich
from .enriched.mediawiki import MediaWikiEnrich
from .enriched.meetup import MeetupEnrich
from .enriched.mozillaclub import MozillaClubEnrich
from .enriched.nntp import NNTPEnrich
from .enriched.phabricator import PhabricatorEnrich
from .enriched.pipermail import PipermailEnrich
from .enriched.puppetforge import PuppetForgeEnrich
from .enriched.redmine import RedmineEnrich
from .enriched.remo import ReMoEnrich
from .enriched.rss import RSSEnrich
from .enriched.slack import SlackEnrich
from .enriched.stackexchange import StackExchangeEnrich
from .enriched.supybot import SupybotEnrich
from .enriched.telegram import TelegramEnrich
from .enriched.twitter import TwitterEnrich
# Connectors for Ocean
from .raw.askbot import AskbotOcean
from .raw.bugzilla import BugzillaOcean
from .raw.bugzillarest import BugzillaRESTOcean
from .raw.confluence import ConfluenceOcean
from .raw.crates import CratesOcean
from .raw.discourse import DiscourseOcean
from .raw.dockerhub import DockerHubOcean
from .raw.elastic import ElasticOcean
from .raw.functest import FunctestOcean
from .raw.gerrit import GerritOcean
from .raw.git import GitOcean
from .raw.github import GitHubOcean
from .raw.gitlab import GitLabOcean
from .raw.google_hits import GoogleHitsOcean
from .raw.groupsio import GroupsioOcean
from .raw.jenkins import JenkinsOcean
from .raw.jira import JiraOcean
from .raw.kitsune import KitsuneOcean
from .raw.mattermost import MattermostOcean
from .raw.mbox import MBoxOcean
from .raw.mediawiki import MediaWikiOcean
from .raw.meetup import MeetupOcean
from .raw.mozillaclub import MozillaClubOcean
from .raw.nntp import NNTPOcean
from .raw.phabricator import PhabricatorOcean
from .raw.pipermail import PipermailOcean
from .raw.puppetforge import PuppetForgeOcean
from .raw.redmine import RedmineOcean
from .raw.remo import ReMoOcean
from .raw.rss import RSSOcean
from .raw.slack import SlackOcean
from .raw.stackexchange import StackExchangeOcean
from .raw.supybot import SupybotOcean
from .raw.telegram import TelegramOcean
from .raw.twitter import TwitterOcean

logger = logging.getLogger(__name__)

kibiter_version = None


def get_connector_from_name(name):

    # Remove extra data from data source section: remo:activities
    name = name.split(":")[0]
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
            "gitlab": [GitLab, GitLabOcean, GitLabEnrich, GitLabCommand],
            "google_hits": [GoogleHits, GoogleHitsOcean, GoogleHitsEnrich, GoogleHitsCommand],
            "groupsio": [Groupsio, GroupsioOcean, GroupsioEnrich, GroupsioCommand],
            "hyperkitty": [HyperKitty, HyperKittyOcean, HyperKittyEnrich, HyperKittyCommand],
            "jenkins": [Jenkins, JenkinsOcean, JenkinsEnrich, JenkinsCommand],
            "jira": [Jira, JiraOcean, JiraEnrich, JiraCommand],
            "kitsune": [Kitsune, KitsuneOcean, KitsuneEnrich, KitsuneCommand],
            "mattermost": [Mattermost, MattermostOcean, MattermostEnrich, MattermostCommand],
            "mbox": [MBox, MBoxOcean, MBoxEnrich, MBoxCommand],
            "mediawiki": [MediaWiki, MediaWikiOcean, MediaWikiEnrich, MediaWikiCommand],
            "meetup": [Meetup, MeetupOcean, MeetupEnrich, MeetupCommand],
            "mozillaclub": [MozillaClub, MozillaClubOcean, MozillaClubEnrich, MozillaClubCommand],
            "nntp": [NNTP, NNTPOcean, NNTPEnrich, NNTPCommand],
            "phabricator": [Phabricator, PhabricatorOcean, PhabricatorEnrich, PhabricatorCommand],
            "pipermail": [Pipermail, PipermailOcean, PipermailEnrich, PipermailCommand],
            "puppetforge": [PuppetForge, PuppetForgeOcean, PuppetForgeEnrich, PuppetForgeCommand],
            "redmine": [Redmine, RedmineOcean, RedmineEnrich, RedmineCommand],
            "remo": [ReMo, ReMoOcean, ReMoEnrich, ReMoCommand],
            "rss": [RSS, RSSOcean, RSSEnrich, RSSCommand],
            "slack": [Slack, SlackOcean, SlackEnrich, SlackCommand],
            "stackexchange": [StackExchange, StackExchangeOcean,
                              StackExchangeEnrich, StackExchangeCommand],
            "supybot": [Supybot, SupybotOcean, SupybotEnrich, SupybotCommand],
            "telegram": [Telegram, TelegramOcean, TelegramEnrich, TelegramCommand],
            "twitter": [Twitter, TwitterOcean, TwitterEnrich, TwitterCommand]
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
    parser.add_argument('--pair-programming', action='store_true', help="Do pair programming in git enrich")
    parser.add_argument('--studies-list', nargs='*', help="List of studies to be executed")
    parser.add_argument('backend', help=argparse.SUPPRESS)
    parser.add_argument('backend_args', nargs=argparse.REMAINDER,
                        help=argparse.SUPPRESS)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    return parser


def get_params():
    """ Get params definition from ElasticOcean and from all the backends """

    parser = get_params_parser()
    args = parser.parse_args()

    if not args.enrich_only and not args.only_identities and not args.only_studies:
        if not args.index:
            # Check that the raw index name is defined
            print("[error] --index <name> param is required when collecting items from raw")
            sys.exit(1)

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
