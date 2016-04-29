#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# GitHub to Kibana
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
import configparser
from datetime import datetime
from dateutil import parser
import logging
from os import sys, path
import requests
from requests_oauthlib import OAuth1
import subprocess
from urllib.parse import quote_plus

import smtplib
from email.mime.text import MIMEText

from grimoire.ocean.elastic import ElasticOcean
from grimoire.utils import config_logging

GITHUB_URL = "https://github.com/"
GITHUB_API_URL = "https://api.github.com"
NREPOS = 10 # Default number of repos to be analyzed
CAULDRON_DASH_URL = "https://thelma.bitergia.net/dashboards"

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
    parser.add_argument('-k', '--kibana-url', default='http://cauldron.io:5601', dest='kibana_url',
                        help='Kibana URL.')
    parser.add_argument('-u', '--graas-url', default='http://cauldron.io', dest='graas_url',
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
               'sort': 'updated', # does not work in repos listing
               'direction': 'desc'}
    return payload

def get_headers(token):
    headers = {'Authorization': 'token ' + token}
    return headers

def get_repositores(org, token, nrepos):
    all_repos = []
    url = GITHUB_API_URL+"/orgs/"+org+"/repos"

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
    for repo in nrepos_sorted:
        logging.debug("%s %s" % (repo['updated_at'], repo['name']))

    return nrepos_sorted

def create_redirect_web_page(web_dir, org_name, kibana_url):
    """ Create HTML pages with the org name that redirect to
        the Kibana dashboard filtered for this org """
    html_redirect = """
    <html>
        <head>
            <meta http-equiv="refresh" content="0; URL=%s/app/kibana#/dashboard/GitHubDash?_a=(filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:github_git_enrich,key:project,negate:!f,value:%s),query:(match:(project:(query:%s,type:phrase))))))&_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:now-1y,mode:quick,to:now))" />
        </head>
    </html>
    """ % (kibana_url, org_name, org_name)
    try:
        with open(path.join(web_dir,org_name),"w") as f:
            f.write(html_redirect)
    except FileNotFoundError as ex:
        logging.error("Wrong web dir for redirect pages: %s" % (web_dir))
        logging.error(ex)

def notify_contact(mail, org, graas_url, repos, first_repo=False):
    """ Send an email to the contact with the details to access
        the Kibana dashboard """
    if first_repo:
        logging.info("Sending first email to %s" % (mail))
        subject = "Bitergia dashboard for %s in progress" % (org)
    else:
        logging.info("Sending last email to %s" % (mail))
        subject = "Bitergia dashboard for %s ready" % (org)


    body = "%s/dashboards/%s\n\n" % (graas_url, org)

    if first_repo:
        body += "First repository analized: %s\n" % (repos[0]['html_url'])
    else:
        body += "Repositories analyzed:\n"
        for repo in repos:
            body += "%s\n" % (repo['html_url'])

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


def publish_twitter(twitter_contact, org):
    """ Publish in twitter the dashboard """
    dashboard_url = CAULDRON_DASH_URL + "/%s" % (org)
    status = quote_plus("In @CauldronIO, @%s your new dashboard is ready %s" %(twitter_contact, dashboard_url))
    oauth = get_oauth()
    r = requests.post(url="https://api.twitter.com/1.1/statuses/update.json?status="+status, auth=oauth)
    print (r.json())

def get_oauth():
    filepath = "twitter.oauth"
    with open(filepath, 'r'): pass
    config = configparser.SafeConfigParser()
    config.read(filepath)

    params = ['consumer_key','consumer_secret','oauth_token','oauth_token_secret']

    if 'oauth' not in config.sections():
        raise RuntimeError("Bad oauth file format %s, section missing: %s" % (filepath, 'oauth'))
    oauth_config =  dict(config.items('oauth'))
    for param in params:
        if param not in oauth_config:
            raise RuntimeError("Bad oauth file format %s, not found param: %s" % (filepath, param))

    oauth = OAuth1(oauth_config['consumer_key'],
                   client_secret=oauth_config['consumer_secret'],
                   resource_owner_key=oauth_config['oauth_token'],
                   resource_owner_secret=oauth_config['oauth_token_secret'])

    return oauth

if __name__ == '__main__':

    task_init = datetime.now()

    args = get_params()

    config_logging(args.debug)

    # All projects share the same index
    git_index = "github_git"
    issues_index = "github_issues"

    logging.info("Creating new GitHub dashboard with %i repositores from %s" %
                (args.nrepos, args.org))

    # Generate redirect web page first so dashboard can be used
    # with partial data during data retrieval
    create_redirect_web_page(args.web_dir, args.org, args.kibana_url)

    repos = get_repositores(args.org, args.token, args.nrepos)
    first_repo = True

    for repo in repos:
        project = args.org  # project = org in GitHub
        url = GITHUB_URL+args.org+"/"+repo['name']
        basic_cmd = "./p2o.py -g -e %s --project %s --enrich" % \
            (args.elastic_url, project)
        cmd = basic_cmd + " --index %s git %s" % (git_index, url)
        git_cmd = subprocess.call(cmd, shell=True)
        if git_cmd != 0:
            logging.error("Problems with command: %s" % cmd)
        cmd = basic_cmd + " --index %s github --owner %s --repository %s -t %s --sleep-for-rate" % \
            (issues_index, args.org, repo['name'], args.token)
        issues_cmd = subprocess.call(cmd, shell=True)
        if issues_cmd != 0:
            logging.error("Problems with command: %s" % cmd)
        else:
            if first_repo:
                if args.contact:
                    notify_contact(args.contact, args.org, args.graas_url, repos, first_repo)
                first_repo = False


    total_time_min = (datetime.now()-task_init).total_seconds()/60

    logging.info("Finished %s in %.2f min" % (args.org, total_time_min))

    # Notify the contact about the new dashboard
    if args.contact:
        notify_contact(args.contact, args.org, args.graas_url, repos)
    if args.twitter:
        logging.debug("Twitter user to be notified: %s" % (args.twitter))
        publish_twitter(args.twitter, args.org)
