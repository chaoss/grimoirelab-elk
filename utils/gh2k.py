#!/usr/bin/env python3
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import argparse
import configparser
import logging
import requests
import subprocess

from datetime import datetime
from dateutil import parser
from os import sys, path
from time import sleep
from requests_oauthlib import OAuth1
from urllib.parse import quote_plus

import smtplib
from email.mime.text import MIMEText

from grimoire_elk.raw.elastic import ElasticOcean
from grimoire_elk.utils import config_logging

GITHUB_URL = "https://github.com/"
GITHUB_API_URL = "https://api.github.com"
NREPOS = 10  # Default number of repos to be analyzed
CAULDRON_DASH_URL = "https://cauldron.io/dashboards"


def get_params_parser():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser()

    ElasticOcean.add_params(parser)

    parser.add_argument('-g', '--debug', dest='debug', action='store_true')
    parser.add_argument('-t', '--token', dest='token', help="GitHub token")
    parser.add_argument('-o', '--org', dest='org', help='GitHub Organization to be analyzed')
    parser.add_argument('-c', '--contact', dest='contact', help='Contact (mail) to notify events.')
    parser.add_argument('--twitter', dest='twitter', help='Twitter account to notify.')
    parser.add_argument('-w', '--web-dir', default='/var/www/cauldron/dashboards', dest='web_dir',
                        help='Redirect HTML project pages for accessing Kibana dashboards.')
    parser.add_argument('-k', '--kibana-url', default='https://dashboard.cauldron.io', dest='kibana_url',
                        help='Kibana URL.')
    parser.add_argument('-u', '--graas-url', default='https://cauldron.io', dest='graas_url',
                        help='GraaS service URL.')
    parser.add_argument('-n', '--nrepos', dest='nrepos', type=int, default=NREPOS,
                        help='Number of GitHub repositories from the Organization to be analyzed (default:10)')

    return parser


def get_params():
    parser = get_params_parser()
    args = parser.parse_args()

    if not args.org or not args.token:
        parser.error("token and org params must be provided.")
        sys.exit(1)

    return args


def get_payload():
    # 100 max in repos
    payload = {'per_page': 100,
               'fork': False,
               'sort': 'updated',  # does not work in repos listing
               'direction': 'desc'}
    return payload


def get_headers(token):
    headers = {'Authorization': 'token ' + token}
    return headers


def get_owner_repos_url(owner, token):
    """ The owner could be a org or a user.
        It waits if need to have rate limit.
        Also it fixes a djando issue changing - with _
    """
    url_org = GITHUB_API_URL + "/orgs/" + owner + "/repos"
    url_user = GITHUB_API_URL + "/users/" + owner + "/repos"

    url_owner = url_org  # Use org by default

    try:
        r = requests.get(url_org,
                         params=get_payload(),
                         headers=get_headers(token))
        r.raise_for_status()

    except requests.exceptions.HTTPError as e:
        if r.status_code == 403:
            rate_limit_reset_ts = datetime.fromtimestamp(int(r.headers['X-RateLimit-Reset']))
            seconds_to_reset = (rate_limit_reset_ts - datetime.utcnow()).seconds + 1
            logging.info("GitHub rate limit exhausted. Waiting %i secs for rate limit reset." % (seconds_to_reset))
            sleep(seconds_to_reset)
        else:
            # owner is not an org, try with a user
            url_owner = url_user
    return url_owner


def get_repositores(owner_url, token, nrepos):
    """ owner could be an org or and user """
    all_repos = []

    url = owner_url

    while True:
        logging.debug("Getting repos from: %s" % (url))
        try:
            r = requests.get(url,
                             params=get_payload(),
                             headers=get_headers(token))

            r.raise_for_status()
            all_repos += r.json()

            logging.debug("Rate limit: %s" % (r.headers['X-RateLimit-Remaining']))

            if 'next' not in r.links:
                break

            url = r.links['next']['url']  # Loving requests :)
        except requests.exceptions.ConnectionError:
            logging.error("Can not connect to GitHub")
            break

    # Remove forks
    nrepos_recent = [repo for repo in all_repos if not repo['fork']]
    # Sort by updated_at and limit to nrepos
    nrepos_sorted = sorted(nrepos_recent, key=lambda repo: parser.parse(repo['updated_at']), reverse=True)
    nrepos_sorted = nrepos_sorted[0:nrepos]
    # First the small repositories to feedback the user quickly
    nrepos_sorted = sorted(nrepos_sorted, key=lambda repo: repo['size'])
    for repo in nrepos_sorted:
        logging.debug("%s %i %s" % (repo['updated_at'], repo['size'], repo['name']))
    return nrepos_sorted


def create_redirect_web_page(web_dir, org_name, kibana_url):
    """ Create HTML pages with the org name that redirect to
        the Kibana dashboard filtered for this org """
    html_redirect = """
    <html>
        <head>
    """
    html_redirect += """<meta http-equiv="refresh" content="0; URL=%s/app/kibana"""\
                     % kibana_url
    html_redirect += """#/dashboard/Overview?_g=(filters:!(('$state':"""
    html_redirect += """(store:globalState),meta:(alias:!n,disabled:!f,index:"""
    html_redirect += """github_git_enrich,key:project,negate:!f,value:%s),"""\
                     % org_name
    html_redirect += """query:(match:(project:(query:%s,type:phrase))))),"""\
                     % org_name
    html_redirect += """refreshInterval:(display:Off,pause:!f,value:0),"""
    html_redirect += """time:(from:now-2y,mode:quick,to:now))" />
        </head>
    </html>
    """
    try:
        with open(path.join(web_dir, org_name), "w") as f:
            f.write(html_redirect)
    except FileNotFoundError as ex:
        logging.error("Wrong web dir for redirect pages: %s" % (web_dir))
        logging.error(ex)


def notify_contact(mail, owner, graas_url, repos, first_repo=False):
    """ Send an email to the contact with the details to access
        the Kibana dashboard """

    footer = """
--
Bitergia Cauldron Team
http://bitergia.com
    """

    twitter_txt = "Check Cauldron.io dashboard for %s at %s/dashboards/%s" % (owner, graas_url, owner)
    twitter_url = "https://twitter.com/intent/tweet?text=" + quote_plus(twitter_txt)
    twitter_url += "&via=bitergia"

    if first_repo:
        logging.info("Sending first email to %s" % (mail))
        subject = "First repository for %s already in the Cauldron" % (owner)
    else:
        logging.info("Sending last email to %s" % (mail))
        subject = "Your Cauldron %s dashboard is ready!" % (owner)

    if first_repo:
        # body = "%s/dashboards/%s\n\n" % (graas_url, owner)
        # body += "First repository analized: %s\n" % (repos[0]['html_url'])
        body = """
First repository has been analyzed and it's already in the Cauldron. Be patient, we have just started, step by step.

We will notify you when everything is ready.

Meanwhile, check latest dashboards in %s

Thanks,
%s
    """ % (graas_url, footer)
    else:
        body = """
Check it at: %s/dashboards/%s

Play with it, and send us feedback:
https://github.com/Bitergia/cauldron.io/issues/new

Share it on Twitter:
%s

Thank you very much,
%s
    """ % (graas_url, owner, twitter_url, footer)

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = 'info@bitergia.com'
    msg['To'] = mail

    try:
        s = smtplib.SMTP('localhost')
        s.send_message(msg)
        s.quit()
    except ConnectionRefusedError:
        logging.error("Can not notify user. Can not connect to email server.")


def publish_twitter(twitter_contact, owner):
    """ Publish in twitter the dashboard """
    dashboard_url = CAULDRON_DASH_URL + "/%s" % (owner)
    tweet = "@%s your http://cauldron.io dashboard for #%s at GitHub is ready: %s. Check it out! #oscon" \
        % (twitter_contact, owner, dashboard_url)
    status = quote_plus(tweet)
    oauth = get_oauth()
    r = requests.post(url="https://api.twitter.com/1.1/statuses/update.json?status=" + status, auth=oauth)


def get_oauth():
    filepath = "twitter.oauth"
    with open(filepath, 'r'):
        pass
    config = configparser.SafeConfigParser()
    config.read(filepath)

    params = ['consumer_key', 'consumer_secret', 'oauth_token', 'oauth_token_secret']

    if 'oauth' not in config.sections():
        raise RuntimeError("Bad oauth file format %s, section missing: %s" % (filepath, 'oauth'))
    oauth_config = dict(config.items('oauth'))
    for param in params:
        if param not in oauth_config:
            raise RuntimeError("Bad oauth file format %s, not found param: %s" % (filepath, param))

    oauth = OAuth1(oauth_config['consumer_key'],
                   client_secret=oauth_config['consumer_secret'],
                   resource_owner_key=oauth_config['oauth_token'],
                   resource_owner_secret=oauth_config['oauth_token_secret'])

    return oauth


if __name__ == '__main__':
    """GitHub to Kibana"""

    task_init = datetime.now()

    args = get_params()

    config_logging(args.debug)

    # All projects share the same index
    git_index = "github_git"
    issues_index = "github_issues"

    # The owner could be a org or an user.
    owner_url = get_owner_repos_url(args.org, args.token)
    owner = args.org

    logging.info("Creating new GitHub dashboard with %i repositores from %s" %
                 (args.nrepos, owner))

    # Generate redirect web page first so dashboard can be used
    # with partial data during data retrieval
    create_redirect_web_page(args.web_dir, owner, args.kibana_url)

    repos = get_repositores(owner_url, args.token, args.nrepos)
    first_repo = True

    for repo in repos:
        project = owner  # project = org in GitHub
        url = GITHUB_URL + owner + "/" + repo['name']
        basic_cmd = "p2o.py -g -e %s --project %s --enrich" % \
            (args.elastic_url, project)
        cmd = basic_cmd + " --index %s git %s" % (git_index, url)
        git_cmd = subprocess.call(cmd, shell=True)
        if git_cmd != 0:
            logging.error("Problems with command: %s" % cmd)
        cmd = basic_cmd + " --index %s github --owner %s --repository %s -t %s --sleep-for-rate" % \
            (issues_index, owner, repo['name'], args.token)
        issues_cmd = subprocess.call(cmd, shell=True)
        if issues_cmd != 0:
            logging.error("Problems with command: %s" % cmd)
        else:
            if first_repo:
                if args.contact:
                    notify_contact(args.contact, owner, args.graas_url, repos, first_repo)
                first_repo = False

    total_time_min = (datetime.now() - task_init).total_seconds() / 60

    logging.info("Finished %s in %.2f min" % (owner, total_time_min))

    # Notify the contact about the new dashboard
    if args.contact:
        notify_contact(args.contact, owner, args.graas_url, repos)
    if args.twitter:
        logging.debug("Twitter user to be notified: %s" % (args.twitter))
        publish_twitter(args.twitter, owner)
