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
import enum
import os
import datetime
import inspect
import json
import logging
import re
import time

import requests
import urllib3
from git import Repo, RemoteProgress

from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)


BACKOFF_FACTOR = 0.2
MAX_RETRIES = 21
MAX_RETRIES_ON_REDIRECT = 5
MAX_RETRIES_ON_READ = 8
MAX_RETRIES_ON_CONNECT = 21
STATUS_FORCE_LIST = [408, 409, 429, 502, 503, 504]
METADATA_FILTER_RAW = 'metadata__filter_raw'
REPO_LABELS = 'repository_labels'
REPO_BASE_DIR = '/var/tmp/repo'

logger = logging.getLogger(__name__)


def get_repository_filter(perceval_backend, perceval_backend_name, term=False):
    """ Get the filter needed for get the items in a repository """
    from .github import GITHUB

    filter_ = {}

    if not perceval_backend:
        return filter_

    field = 'origin'
    value = anonymize_url(perceval_backend.origin)

    if perceval_backend_name in ["meetup", "nntp", "stackexchange", "jira"]:
        # Until tag is supported in all raw and enriched indexes
        # we should use origin. But stackexchange and meetup won't work with origin
        # because the tag must be included in the filter.
        # For nntp we have a common group server as origin, so we need to use also the tag.
        # And in jira we can filter by product, and the origin is the same jira server.
        field = 'tag'
        value = perceval_backend.tag

    if perceval_backend:
        if not term:
            filter_ = {"name": field,
                       "value": value}
        else:
            filter_ = '''
                {"term":
                    { "%s" : "%s"  }
                }
            ''' % (field, value)
            # Filters are always a dict
            filter_ = json.loads(filter_)

    if value in ['', GITHUB + '/', 'https://meetup.com/']:
        # Support for getting all items from a multiorigin index
        # In GitHub we receive GITHUB + '/', the site url without org and repo
        # In Meetup we receive https://meetup.com/ as the tag
        filter_ = {}

    return filter_


def anonymize_url(url):
    """Remove credentials from the url

    :param url: target url
    """
    anonymized = re.sub('://.*@', '://', url)

    return anonymized


def get_time_diff_days(start, end):
    ''' Number of days between two dates in UTC format  '''

    if start is None or end is None:
        return None

    if type(start) is not datetime.datetime:
        start = str_to_datetime(start).replace(tzinfo=None)
    if type(end) is not datetime.datetime:
        end = str_to_datetime(end).replace(tzinfo=None)

    seconds_day = float(60 * 60 * 24)
    diff_days = (end - start).total_seconds() / seconds_day
    diff_days = float('%.2f' % diff_days)

    return diff_days


def grimoire_con(insecure=True, conn_retries=MAX_RETRIES_ON_CONNECT, total=MAX_RETRIES):
    conn = requests.Session()
    # {backoff factor} * (2 ^ ({number of total retries} - 1))
    # conn_retries = 21  # 209715.2 = 2.4d
    # total covers issues like 'ProtocolError('Connection aborted.')
    # Retry when there are errors in HTTP connections
    retries = urllib3.util.Retry(total=total, connect=conn_retries, read=MAX_RETRIES_ON_READ,
                                 redirect=MAX_RETRIES_ON_REDIRECT, backoff_factor=BACKOFF_FACTOR,
                                 method_whitelist=False, status_forcelist=STATUS_FORCE_LIST)
    adapter = requests.adapters.HTTPAdapter(max_retries=retries)
    conn.mount('http://', adapter)
    conn.mount('https://', adapter)

    if insecure:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        conn.verify = False

    return conn


def get_last_enrich(backend_cmd, enrich_backend, filter_raw=None):
    last_enrich = None

    if backend_cmd:
        backend = backend_cmd.backend
        # Only supported in data retrieved from a perceval backend
        # Always filter by repository to support multi repository indexes
        backend_name = enrich_backend.get_connector_name()

        filter_ = get_repository_filter(backend, backend_name)
        filters_ = [filter_]

        # include the filter_raw text (taken from the projects.json) to the
        # list of filters used to retrieve the last enrich date.
        if filter_raw:
            filter_raw_ = {
                "name": METADATA_FILTER_RAW,
                "value": filter_raw
            }

            filters_.append(filter_raw_)

        # Check if backend supports from_date
        signature = inspect.signature(backend.fetch)

        from_date = None
        if 'from_date' in signature.parameters:
            try:
                # Support perceval pre and post BackendCommand refactoring
                from_date = backend_cmd.from_date
            except AttributeError:
                from_date = backend_cmd.parsed_args.from_date

        offset = None
        if 'offset' in signature.parameters:
            try:
                offset = backend_cmd.offset
            except AttributeError:
                offset = backend_cmd.parsed_args.offset

        if from_date:
            if from_date.replace(tzinfo=None) != str_to_datetime("1970-01-01").replace(tzinfo=None):
                last_enrich = from_date
            # if the index is empty, set the last enrich to None
            elif not enrich_backend.from_date:
                last_enrich = None
            else:
                # if the index is not empty, the last enrich is the minimum between
                # the last filtered item (the last item for a given origin) and the
                # last item in the enriched index. If `last_enrich_filtered` is empty,
                # it means that no items for that origin are in the index, thus the
                # `last_enrich` is set to None
                last_enrich_filtered = enrich_backend.get_last_update_from_es(filters_)
                last_enrich = get_min_last_enrich(enrich_backend.from_date, last_enrich_filtered)

        elif offset is not None:
            if offset != 0:
                last_enrich = offset
            else:
                last_enrich = enrich_backend.get_last_offset_from_es(filters_)

        else:
            if not enrich_backend.from_date:
                last_enrich = None
            else:
                last_enrich_filtered = enrich_backend.get_last_update_from_es(filters_)
                last_enrich = get_min_last_enrich(enrich_backend.from_date, last_enrich_filtered)
    else:
        last_enrich = enrich_backend.get_last_update_from_es()

    return last_enrich


def get_min_last_enrich(last_enrich, last_enrich_filtered):
    if last_enrich_filtered:
        min_enrich = min(last_enrich, last_enrich_filtered.replace(tzinfo=None))
    else:
        min_enrich = None

    return min_enrich


def get_diff_current_date(days=0, hours=0, minutes=0):
    before_date = datetime_utcnow() - datetime.timedelta(days=days, hours=hours, minutes=minutes)

    return before_date


def fix_field_date(date_value):
    """Fix possible errors in the field date"""

    field_date = str_to_datetime(date_value)

    try:
        _ = int(field_date.strftime("%z")[0:3])
    except ValueError:
        field_date = field_date.replace(tzinfo=None)

    return field_date.isoformat()


class CloneProgress(RemoteProgress):

    def update(self, op_code, cur_count, max_count=None, message=''):
        if message:
            print(message)


class GitOperation(enum.Enum):
    clone = 'clone'
    purge = 'purge'


def get_list_of_files(dir_name):
    # create a list of file and sub directories
    # names in the given directory
    list_of_file = os.listdir(dir_name)
    all_files = list()
    # Iterate over all the entries
    for entry in list_of_file:
        # Create full path
        full_path = os.path.join(dir_name, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(full_path) and not full_path.endswith('.git'):
            all_files = all_files + get_list_of_files(full_path)
        else:
            all_files.append(full_path)

    return all_files


def get_git_repo_dir(base_dir, repo_dir):
    return os.path.join(base_dir, repo_dir)


def get_git_repo_name(repo_url):
    return (repo_url.split('/')[-1]).replace('.git', '')


def build_git_clone_command(repo_url, base_dir=REPO_BASE_DIR):
    cmd_git = '/usr/bin/git'
    cmd_ops_git = 'clone'
    return f'{cmd_git} {cmd_ops_git} {repo_url}'


def build_git_purge_repo(base_dir, repo_dir):
    cmd_purge = 'rm -rf'
    return f'{cmd_purge} {os.path.join(base_dir, repo_dir)}'


def exec_git_command(command):
    os.popen(command)


def exec_git_clone_operation(repo_url, path):
    result = Repo.clone_from(url=repo_url, to_path=path,
                             progress=CloneProgress())


def process_git_operation(choice, args):
    if GitOperation.clone.value == choice:
        # command = build_git_clone_command(repo_url=args.get('repo_url'))
        # exec_git_command(command)
        exec_git_clone_operation(repo_url=args.get('repo_url'),
                                 path=args.get('path'))
    elif GitOperation.purge.value == choice:
        command = build_git_purge_repo(base_dir=args.get('base_dir'),
                                       repo_dir=args.get('repo_dir'))
        exec_git_command(command)
    else:
        pass


def build_cloc_command(path):
    cmd_loc = 'cloc'
    return f'{cmd_loc} {path}'


def exec_cloc_command(command):
    return os.popen(command)


def get_lines_of_code(repo_url):
    lines_of_code = -1
    repo_dir = get_git_repo_name(repo_url)
    repo_dir_path = get_git_repo_dir(REPO_BASE_DIR, repo_dir)

    def read_os_stream(stream):
        return stream.read()

    def extract_lines_of_code(value):
        if 'SUM:' in value:
            return int((value.split('\n')[-3]).split(' ')[-1])
        return 0

    start = time.time()
    logger.debug('Get lines of code process started')
    logger.debug(f"Runtime of the program start {start}")
    try:
        logger.debug('Purge the repository directory if exists')
        if os.path.exists(repo_dir_path):
            logger.debug('Purge the repository started')
            process_git_operation(choice=GitOperation.purge.value,
                                  args={'base_dir': REPO_BASE_DIR,
                                        'repo_dir': repo_dir})
            logger.debug('Purge the repository started')

        logger.debug('Pull repository in repository base location')
        process_git_operation(choice=GitOperation.clone.value,
                              args={'repo_url': repo_url,
                                    'path': repo_dir_path})

        logger.debug('The lines of code calculation started')
        command = build_cloc_command(repo_dir_path)
        os_stream = exec_cloc_command(command)
        lines_of_code += extract_lines_of_code(read_os_stream(os_stream))
        logger.debug('The lines of code calculation finished')
    except Exception as e:
        logger.debug('Oops error in execution')
        logger.debug(e)
    else:
        logger.debug('Purge the repository directory if exists')
        process_git_operation(choice=GitOperation.purge.value,
                              args={'base_dir': REPO_BASE_DIR,
                                    'repo_dir': repo_dir})
        end = time.time()
        logger.debug(f"Runtime of the program end {end}")
        logger.debug(f"Runtime of the program is {end - start}")

        # Return lines of code value otherwise -1 for error
        return lines_of_code
