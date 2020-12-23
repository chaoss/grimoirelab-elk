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
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#   Florent Kaisser <florent.pro@kaisser.name>
#   Miguel Ángel Fernández <mafesan@bitergia.com>
#

import logging
import re
import time

import requests

from dateutil.relativedelta import relativedelta
from datetime import datetime

from grimoire_elk.elastic import ElasticSearch
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)

from elasticsearch import Elasticsearch as ES, RequestsHttpConnection

from .utils import get_time_diff_days

from .enrich import Enrich, metadata, anonymize_url
from ..elastic_mapping import Mapping as BaseMapping

from .github_study_evolution import (get_unique_repository_with_project_name,
                                     get_issues_dates,
                                     get_issues_not_closed_by_label,
                                     get_issues_open_at_by_label,
                                     get_issues_not_closed_other_label,
                                     get_issues_open_at_other_label)


GITHUB = 'https://github.com/'

logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        geopoints type is not created in dynamic mapping

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
               "merge_author_geolocation": {
                   "type": "geo_point"
               },
               "assignee_geolocation": {
                   "type": "geo_point"
               },
               "state": {
                   "type": "keyword"
               },
               "user_geolocation": {
                   "type": "geo_point"
               },
               "title_analyzed": {
                 "type": "text",
                 "index": true
               }
            }
        }
        """

        return {"items": mapping}


class GitHubEnrich(Enrich):

    mapping = Mapping

    issue_roles = ['assignee_data', 'user_data']
    pr_roles = ['merged_by_data', 'user_data']
    roles = ['assignee_data', 'merged_by_data', 'user_data']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []
        self.studies.append(self.enrich_onion)
        self.studies.append(self.enrich_pull_requests)
        self.studies.append(self.enrich_geolocation)
        self.studies.append(self.enrich_extra_data)
        self.studies.append(self.enrich_backlog_analysis)

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_author(self):
        return "user_data"

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "grimoire_creation_date"

    def get_identities(self, item):
        """Return the identities from an item"""

        category = item['category']
        item = item['data']

        if category == "issue":
            identity_types = ['user', 'assignee']
        elif category == "pull_request":
            identity_types = ['user', 'merged_by']
        else:
            identity_types = []

        for identity in identity_types:
            identity_attr = identity + "_data"
            if item[identity] and identity_attr in item:
                # In user_data we have the full user data
                user = self.get_sh_identity(item[identity_attr])
                if user:
                    yield user

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if isinstance(item, dict) and 'data' in item:
            user = item['data'][identity_field]

        if not user:
            return identity

        identity['username'] = user['login']
        identity['email'] = None
        identity['name'] = None
        if 'email' in user:
            identity['email'] = user['email']
        if 'name' in user:
            identity['name'] = user['name']
        return identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        return repo

    def get_time_to_first_attention(self, item):
        """Get the first date at which a comment or reaction was made to the issue by someone
        other than the user who created the issue
        """
        comment_dates = [str_to_datetime(comment['created_at']) for comment in item['comments_data']
                         if item['user']['login'] != comment['user']['login']]
        reaction_dates = [str_to_datetime(reaction['created_at']) for reaction in item['reactions_data']
                          if item['user']['login'] != reaction['user']['login']]
        reaction_dates.extend(comment_dates)
        if reaction_dates:
            return min(reaction_dates)
        return None

    def get_time_to_merge_request_response(self, item):
        """Get the first date at which a review was made on the PR by someone
        other than the user who created the PR
        """
        review_dates = []
        for comment in item['review_comments_data']:
            # skip comments of ghost users
            if not comment['user']:
                continue

            # skip comments of the pull request creator
            if item['user']['login'] == comment['user']['login']:
                continue

            review_dates.append(str_to_datetime(comment['created_at']))

        if review_dates:
            return min(review_dates)

        return None

    def get_latest_comment_date(self, item):
        """Get the date of the latest comment on the issue/pr"""

        comment_dates = [str_to_datetime(comment['created_at']) for comment in item['comments_data']]
        if comment_dates:
            return max(comment_dates)
        return None

    def get_num_commenters(self, item):
        """Get the number of unique people who commented on the issue/pr"""

        commenters = [comment['user']['login'] for comment in item['comments_data']]
        return len(set(commenters))

    @metadata
    def get_rich_item(self, item):

        rich_item = {}
        if item['category'] == 'issue':
            rich_item = self.__get_rich_issue(item)
        elif item['category'] == 'pull_request':
            rich_item = self.__get_rich_pull(item)
        elif item['category'] == 'repository':
            rich_item = self.__get_rich_repo(item)
        else:
            logger.error("[github] rich item not defined for GitHub category {}".format(
                         item['category']))

        self.add_repository_labels(rich_item)
        self.add_metadata_filter_raw(rich_item)
        return rich_item

    def enrich_onion(self, ocean_backend, enrich_backend,
                     no_incremental=False,
                     in_index_iss='github_issues_onion-src',
                     in_index_prs='github_prs_onion-src',
                     out_index_iss='github_issues_onion-enriched',
                     out_index_prs='github_prs_onion-enriched',
                     data_source_iss='github-issues',
                     data_source_prs='github-prs',
                     contribs_field='uuid',
                     timeframe_field='grimoire_creation_date',
                     sort_on_field='metadata__timestamp',
                     seconds=Enrich.ONION_INTERVAL):

        super().enrich_onion(enrich_backend=enrich_backend,
                             in_index=in_index_iss,
                             out_index=out_index_iss,
                             data_source=data_source_iss,
                             contribs_field=contribs_field,
                             timeframe_field=timeframe_field,
                             sort_on_field=sort_on_field,
                             no_incremental=no_incremental,
                             seconds=seconds)

        super().enrich_onion(enrich_backend=enrich_backend,
                             in_index=in_index_prs,
                             out_index=out_index_prs,
                             data_source=data_source_prs,
                             contribs_field=contribs_field,
                             timeframe_field=timeframe_field,
                             sort_on_field=sort_on_field,
                             no_incremental=no_incremental,
                             seconds=seconds)

    def enrich_pull_requests(self, ocean_backend, enrich_backend,
                             raw_issues_index="github_issues_raw"):
        """
        The purpose of this Study is to add additional fields to the pull_requests only index.
        Basically to calculate some of the metrics from Code Development under GMD metrics:
        https://github.com/chaoss/wg-gmd/blob/master/2_Growth-Maturity-Decline.md#code-development

        When data from the pull requests category is fetched using perceval,
        some additional fields such as "number_of_comments" that are made on the PR
        cannot be calculated as the data related to comments is not fetched.
        When data from the issues category is fetched, then every item is considered as an issue
        and PR specific data such as "review_comments" are not fetched.

        Items (pull requests) from the raw issues index are queried and data from those items
        are used to add fields in the corresponding pull request in the pull requests only index.
        The ids are matched in both the indices.

        :param ocean_backend: backend from which to read the raw items
        :param enrich_backend:  backend from which to read the enriched items
        :param raw_issues_index: the raw issues index from which the data for PRs is to be extracted
        :return: None
        """

        HEADER_JSON = {"Content-Type": "application/json"}

        # issues raw index from which the data will be extracted
        github_issues_raw_index = ocean_backend.elastic_url + "/" + raw_issues_index
        issues_index_search_url = github_issues_raw_index + "/_search"

        # pull_requests index search url in which the data is to be updated
        enrich_index_search_url = self.elastic.index_url + "/_search"

        logger.info("[github] Doing enrich_pull_request study for index {}".format(
                    anonymize_url(self.elastic.index_url)))
        time.sleep(1)  # HACK: Wait until git enrich index has been written

        def make_request(url, error_msg, data=None, req_type="GET"):
            """
            Make a request to the given url. The request can be of type GET or a POST.
            If the request raises an error, display that error using the custom error msg.

            :param url: URL to make the GET request to
            :param error_msg: custom error message for logging purposes
            :param data: data to be sent with the POST request
                         optional if type="GET" else compulsory
            :param req_type: the type of request to be made: GET or POST
                         default: GET
            :return r: requests object
            """

            r = None
            if req_type == "GET":
                r = self.requests.get(url, headers=HEADER_JSON,
                                      verify=False)
            elif req_type == "POST" and data is not None:
                r = self.requests.post(url, data=data, headers=HEADER_JSON,
                                       verify=False)
            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                logger.error(error_msg)
                logger.error(ex)
                return

            return r

        # Check if the github issues raw index exists, if not raise an error and abort
        error_msg = "Invalid index provided for enrich_pull_requests study. Aborting."
        make_request(issues_index_search_url, error_msg)

        # get the number of pull requests in the pull_requests index
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-count.html
        # Example:
        # epoch      timestamp count
        # 1533454641 13:07:21  276
        count_url = enrich_backend.elastic_url + "/_cat/count/" + enrich_backend.elastic.index + "?v"
        error_msg = "Cannot fetch number of items in {} Aborting.".format(enrich_backend.elastic.index)
        r = make_request(count_url, error_msg)
        num_pull_requests = int(r.text.split()[-1])

        # get all the ids that are in the enriched pull requests index which will be used later
        # to pull requests data from the issue having the same id in the raw_issues_index
        pull_requests_ids = []
        size = 10000  # Default number of items that can be queried from elasticsearch at a time
        i = 0  # counter
        while num_pull_requests > 0:
            fetch_id_in_repo_query = """
            {
                "_source": ["id_in_repo"],
                "from": %s,
                "size": %s
            }
            """ % (i, size)

            error_msg = "Error extracting id_in_repo from {}. Aborting.".format(self.elastic.index_url)
            r = make_request(enrich_index_search_url, error_msg, fetch_id_in_repo_query, "POST")
            id_in_repo_json = r.json()["hits"]["hits"]
            pull_requests_ids.extend([item["_source"]["id_in_repo"] for item in id_in_repo_json])
            i += size
            num_pull_requests -= size

        # get pull requests data from the github_issues_raw and pull_requests only
        # index using specific id for each of the item
        query = """
        {
            "query": {
                "bool": {
                    "must": [{
                                "match": {
                                    %s: %s
                                }
                            }]
                        }
                    }
            }
        """
        num_enriched = 0  # counter to count the number of PRs enriched
        pull_requests = []

        for pr_id in pull_requests_ids:
            # retrieve the data from the issues index
            issue_query = query % ('"data.number"', pr_id)
            error_msg = "Id {} doesnot exists in {}. Aborting.".format(pr_id, github_issues_raw_index)
            r = make_request(issues_index_search_url, error_msg, issue_query, "POST")
            issue = r.json()["hits"]["hits"][0]["_source"]["data"]

            # retrieve the data from the pull_requests index
            pr_query = query % ('"id_in_repo"', pr_id)
            error_msg = "Id {} doesnot exists in {}. Aborting.".format(pr_id, self.elastic.index_url)
            r = make_request(enrich_index_search_url, error_msg, pr_query, "POST")
            pull_request_data = r.json()["hits"]["hits"][0]
            pull_request = pull_request_data['_source']
            pull_request["_item_id"] = pull_request_data['_id']

            # Add the necessary fields
            reaction_time = get_time_diff_days(str_to_datetime(issue['created_at']),
                                               self.get_time_to_first_attention(issue))
            if not reaction_time:
                reaction_time = 0
            if pull_request["time_to_merge_request_response"]:
                reaction_time = min(pull_request["time_to_merge_request_response"], reaction_time)
            pull_request["time_to_merge_request_response"] = reaction_time

            pull_request['num_comments'] = issue['comments']

            # should latest reviews be considered as well?
            pull_request['pr_comment_duration'] = get_time_diff_days(str_to_datetime(issue['created_at']),
                                                                     self.get_latest_comment_date(issue))
            pull_request['pr_comment_diversity'] = self.get_num_commenters(issue)

            pull_requests.append(pull_request)
            if len(pull_requests) >= self.elastic.max_items_bulk:
                self.elastic.bulk_upload(pull_requests, "_item_id")
                pull_requests = []

            num_enriched += 1
            logger.info("[github] pull_requests processed {}/{}".format(
                        num_enriched, len(pull_requests_ids)))

        self.elastic.bulk_upload(pull_requests, "_item_id")

    def __get_rich_pull(self, item):
        rich_pr = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_pr)
        # The real data
        pull_request = item['data']

        rich_pr['time_to_close_days'] = \
            get_time_diff_days(pull_request['created_at'], pull_request['closed_at'])

        if pull_request['state'] != 'closed':
            rich_pr['time_open_days'] = \
                get_time_diff_days(pull_request['created_at'], datetime_utcnow().replace(tzinfo=None))
        else:
            rich_pr['time_open_days'] = rich_pr['time_to_close_days']

        rich_pr['user_login'] = pull_request['user']['login']

        user = pull_request.get('user_data', None)
        if user is not None and user:
            rich_pr['user_name'] = user['name']
            rich_pr['author_name'] = user['name']
            rich_pr["user_domain"] = self.get_email_domain(user['email']) if user['email'] else None
            rich_pr['user_org'] = user['company']
            rich_pr['user_location'] = user['location']
            rich_pr['user_geolocation'] = None
        else:
            rich_pr['user_name'] = None
            rich_pr["user_domain"] = None
            rich_pr['user_org'] = None
            rich_pr['user_location'] = None
            rich_pr['user_geolocation'] = None
            rich_pr['author_name'] = None

        merged_by = pull_request.get('merged_by_data', None)
        if merged_by and merged_by is not None:
            rich_pr['merge_author_login'] = merged_by['login']
            rich_pr['merge_author_name'] = merged_by['name']
            rich_pr["merge_author_domain"] = self.get_email_domain(merged_by['email']) if merged_by['email'] else None
            rich_pr['merge_author_org'] = merged_by['company']
            rich_pr['merge_author_location'] = merged_by['location']
            rich_pr['merge_author_geolocation'] = None
        else:
            rich_pr['merge_author_name'] = None
            rich_pr['merge_author_login'] = None
            rich_pr["merge_author_domain"] = None
            rich_pr['merge_author_org'] = None
            rich_pr['merge_author_location'] = None
            rich_pr['merge_author_geolocation'] = None

        rich_pr['id'] = pull_request['id']
        rich_pr['id_in_repo'] = pull_request['html_url'].split("/")[-1]
        rich_pr['repository'] = self.get_project_repository(rich_pr)
        rich_pr['title'] = pull_request['title']
        rich_pr['title_analyzed'] = pull_request['title']
        rich_pr['state'] = pull_request['state']
        rich_pr['created_at'] = pull_request['created_at']
        rich_pr['updated_at'] = pull_request['updated_at']
        rich_pr['merged'] = pull_request['merged']
        rich_pr['merged_at'] = pull_request['merged_at']
        rich_pr['closed_at'] = pull_request['closed_at']
        rich_pr['url'] = pull_request['html_url']
        rich_pr['additions'] = pull_request['additions']
        rich_pr['deletions'] = pull_request['deletions']
        rich_pr['changed_files'] = pull_request['changed_files']
        # Adding this field for consistency with the rest of github-related enrichers
        rich_pr['issue_url'] = pull_request['html_url']
        labels = []
        [labels.append(label['name']) for label in pull_request['labels'] if 'labels' in pull_request]
        rich_pr['labels'] = labels

        rich_pr['pull_request'] = True
        rich_pr['item_type'] = 'pull request'

        rich_pr['github_repo'] = rich_pr['repository'].replace(GITHUB, '')
        rich_pr['github_repo'] = re.sub('.git$', '', rich_pr['github_repo'])
        rich_pr["url_id"] = rich_pr['github_repo'] + "/pull/" + rich_pr['id_in_repo']

        # GMD code development metrics
        rich_pr['forks'] = pull_request['base']['repo']['forks_count']
        rich_pr['code_merge_duration'] = get_time_diff_days(pull_request['created_at'],
                                                            pull_request['merged_at'])
        rich_pr['num_review_comments'] = pull_request['review_comments']

        rich_pr['time_to_merge_request_response'] = None
        if pull_request['review_comments'] != 0:
            min_review_date = self.get_time_to_merge_request_response(pull_request)
            rich_pr['time_to_merge_request_response'] = \
                get_time_diff_days(str_to_datetime(pull_request['created_at']), min_review_date)

        if self.prjs_map:
            rich_pr.update(self.get_item_project(rich_pr))

        if 'project' in item:
            rich_pr['project'] = item['project']

        rich_pr.update(self.get_grimoire_fields(pull_request['created_at'], "pull_request"))

        item[self.get_field_date()] = rich_pr[self.get_field_date()]
        rich_pr.update(self.get_item_sh(item, self.pr_roles))

        return rich_pr

    def __get_rich_issue(self, item):
        rich_issue = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_issue)
        # The real data
        issue = item['data']

        rich_issue['time_to_close_days'] = \
            get_time_diff_days(issue['created_at'], issue['closed_at'])

        if issue['state'] != 'closed':
            rich_issue['time_open_days'] = \
                get_time_diff_days(issue['created_at'], datetime_utcnow().replace(tzinfo=None))
        else:
            rich_issue['time_open_days'] = rich_issue['time_to_close_days']

        rich_issue['user_login'] = issue['user']['login']

        user = issue.get('user_data', None)
        if user is not None and user:
            rich_issue['user_name'] = user['name']
            rich_issue['author_name'] = user['name']
            rich_issue["user_domain"] = self.get_email_domain(user['email']) if user['email'] else None
            rich_issue['user_org'] = user['company']
            rich_issue['user_location'] = user['location']
            rich_issue['user_geolocation'] = None
        else:
            rich_issue['user_name'] = None
            rich_issue["user_domain"] = None
            rich_issue['user_org'] = None
            rich_issue['user_location'] = None
            rich_issue['user_geolocation'] = None
            rich_issue['author_name'] = None

        assignee = issue.get('assignee_data', None)
        if assignee and assignee is not None:
            assignee = issue['assignee_data']
            rich_issue['assignee_login'] = assignee['login']
            rich_issue['assignee_name'] = assignee['name']
            rich_issue["assignee_domain"] = self.get_email_domain(assignee['email']) if assignee['email'] else None
            rich_issue['assignee_org'] = assignee['company']
            rich_issue['assignee_location'] = assignee['location']
            rich_issue['assignee_geolocation'] = None
        else:
            rich_issue['assignee_name'] = None
            rich_issue['assignee_login'] = None
            rich_issue["assignee_domain"] = None
            rich_issue['assignee_org'] = None
            rich_issue['assignee_location'] = None
            rich_issue['assignee_geolocation'] = None

        rich_issue['id'] = issue['id']
        rich_issue['id_in_repo'] = issue['html_url'].split("/")[-1]
        rich_issue['repository'] = self.get_project_repository(rich_issue)
        rich_issue['title'] = issue['title']
        rich_issue['title_analyzed'] = issue['title']
        rich_issue['state'] = issue['state']
        rich_issue['created_at'] = issue['created_at']
        rich_issue['updated_at'] = issue['updated_at']
        rich_issue['closed_at'] = issue['closed_at']
        rich_issue['url'] = issue['html_url']
        # Adding this field for consistency with the rest of github-related enrichers
        rich_issue['issue_url'] = issue['html_url']
        labels = []
        [labels.append(label['name']) for label in issue['labels'] if 'labels' in issue]
        rich_issue['labels'] = labels

        rich_issue['pull_request'] = True
        rich_issue['item_type'] = 'pull request'
        if 'head' not in issue.keys() and 'pull_request' not in issue.keys():
            rich_issue['pull_request'] = False
            rich_issue['item_type'] = 'issue'

        rich_issue['github_repo'] = rich_issue['repository'].replace(GITHUB, '')
        rich_issue['github_repo'] = re.sub('.git$', '', rich_issue['github_repo'])
        rich_issue["url_id"] = rich_issue['github_repo'] + "/issues/" + rich_issue['id_in_repo']

        if self.prjs_map:
            rich_issue.update(self.get_item_project(rich_issue))

        if 'project' in item:
            rich_issue['project'] = item['project']

        rich_issue['time_to_first_attention'] = None
        if issue['comments'] + issue['reactions']['total_count'] != 0:
            rich_issue['time_to_first_attention'] = \
                get_time_diff_days(str_to_datetime(issue['created_at']),
                                   self.get_time_to_first_attention(issue))

        rich_issue.update(self.get_grimoire_fields(issue['created_at'], "issue"))

        item[self.get_field_date()] = rich_issue[self.get_field_date()]
        rich_issue.update(self.get_item_sh(item, self.issue_roles))

        return rich_issue

    def __get_rich_repo(self, item):
        rich_repo = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, rich_repo)

        repo = item['data']

        rich_repo['forks_count'] = repo['forks_count']
        rich_repo['subscribers_count'] = repo['subscribers_count']
        rich_repo['stargazers_count'] = repo['stargazers_count']
        rich_repo['fetched_on'] = repo['fetched_on']
        rich_repo['url'] = repo['html_url']

        if self.prjs_map:
            rich_repo.update(self.get_item_project(rich_repo))

        rich_repo.update(self.get_grimoire_fields(item['metadata__updated_on'], "repository"))

        return rich_repo

    def __create_backlog_item(self, repository_url, repository_name, project, date, org_name, interval, label, map_label, issues):

        average_opened_time = 0
        if (len(issues) > 0):
            average_opened_time = sum(issues) / len(issues)

        evolution_item = {
            "uuid": "{}_{}_{}".format(date, repository_name, label),
            "opened": len(issues),
            "average_opened_time": average_opened_time,
            "origin": repository_url,
            "labels": map_label[label] if (label in map_label) else map_label[""],
            "project": project,
            "interval_days": interval,
            "study_creation_date": date,
            "metadata__enriched_on": date,
            "organization": org_name
        }

        evolution_item.update(self.get_grimoire_fields(date, "stats"))

        return evolution_item

    def __get_opened_issues(self, es_in, in_index, repository_url, date, interval, other, label, reduced_labels):
        next_date = (str_to_datetime(date).replace(tzinfo=None)
                     + relativedelta(days=interval)
                     ).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        if(other):
            issues = es_in.search(
                index=in_index,
                body=get_issues_not_closed_other_label(repository_url, next_date, reduced_labels)
            )['hits']['hits']

            issues = issues + es_in.search(
                index=in_index,
                body=get_issues_open_at_other_label(repository_url, next_date, reduced_labels)
            )['hits']['hits']
        else:
            issues = es_in.search(
                index=in_index,
                body=get_issues_not_closed_by_label(repository_url, next_date, label)
            )['hits']['hits']

            issues = issues + es_in.search(
                index=in_index,
                body=get_issues_open_at_by_label(repository_url, next_date, label)
            )['hits']['hits']

        return list(map(lambda i: get_time_diff_days(
                        str_to_datetime(i['_source']['created_at']),
                        str_to_datetime(next_date)
                        ), issues)
                    )

    def enrich_backlog_analysis(self, ocean_backend, enrich_backend, no_incremental=False,
                                out_index="github_enrich_backlog",
                                date_field="grimoire_creation_date",
                                interval_days=1, reduced_labels=["bug"],
                                map_label=["others", "bugs"]):
        """
        The purpose of this study is to add additional index to compute the
        chronological evolution of opened issues and average opened time issues.

        For each repository and label, we start the study on repository
        creation date until today with a day interval (default). For each date
        we retrieve the number of open issues at this date by difference between
        number of opened issues and number of closed issues. In addition, we
        compute the average opened time for all issues open at this date.

        To differentiate by label, we compute evolution for bugs and all others
        labels (like "enhancement","good first issue" ... ), we call this
        "reduced labels". We need to use theses reduced labels because the
        complexity to compute evolution for each combination of labels would be
        too big. In addition, we can rename "bug" label to "bugs" with map_label.

        Entry example in setup.cfg :

        [github]
        raw_index = github_issues_raw
        enriched_index = github_issues_enriched
        ...
        studies = [enrich_backlog_analysis]

        [enrich_backlog_analysis]
        out_index = github_enrich_backlog
        interval_days = 7
        reduced_labels = [bug,enhancement]
        map_label = [others, bugs, enhancements]

        """

        logger.info("[github] Start enrich_backlog_analysis study")

        # combine two lists to create the dict to map labels
        map_label = dict(zip([""] + reduced_labels, map_label))

        # connect to ES
        es_in = ES([enrich_backend.elastic_url], retry_on_timeout=True, timeout=100,
                   verify_certs=self.elastic.requests.verify, connection_class=RequestsHttpConnection)
        in_index = enrich_backend.elastic.index

        # get all repositories
        unique_repos = es_in.search(
            index=in_index,
            body=get_unique_repository_with_project_name())
        repositories = [repo['key'] for repo in unique_repos['aggregations']['unique_repos'].get('buckets', [])]

        logger.debug("[enrich-backlog-analysis] {} repositories to process".format(len(repositories)))

        # create the index
        es_out = ElasticSearch(enrich_backend.elastic.url, out_index, mappings=Mapping)
        es_out.add_alias("backlog_study")

        # analysis for each repositories
        num_items = 0
        ins_items = 0
        for repository in repositories:
            repository_url = repository["origin"]
            project = repository["project"]
            org_name = repository["organization"]
            repository_name = repository_url.split("/")[-1]

            logger.debug("[enrich-backlog-analysis] Start analysis for {}".format(repository_url))

            # get each day since repository creation
            dates = es_in.search(
                index=in_index,
                body=get_issues_dates(interval_days, repository_url)
            )['aggregations']['created_per_interval'].get("buckets", [])

            # for each selected label + others labels
            for label, other in [("", True)] + [(label, False) for label in reduced_labels]:
                # compute metrics for each day (ES request for each day)
                evolution_items = []
                for date in map(lambda i: i['key_as_string'], dates):
                    evolution_item = self.__create_backlog_item(
                        repository_url, repository_name, project, date, org_name, interval_days, label, map_label,
                        self.__get_opened_issues(es_in, in_index, repository_url, date, interval_days,
                                                 other, label, reduced_labels)
                    )
                    evolution_items.append(evolution_item)

                # complete until today (no ES request needed, just extrapol)
                today = datetime.now().replace(hour=0, minute=0, second=0, tzinfo=None)
                last_item = evolution_item
                last_date = str_to_datetime(
                    evolution_item['study_creation_date']).replace(tzinfo=None) \
                    + relativedelta(days=interval_days)
                average_opened_time = evolution_item['average_opened_time'] \
                    + float(interval_days)
                while last_date < today:
                    date = last_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    evolution_item = {}
                    evolution_item.update(last_item)
                    evolution_item.update({
                        "average_opened_time": average_opened_time,
                        "study_creation_date": date,
                        "uuid": "{}_{}_{}".format(date, repository_name, label),
                    })
                    evolution_item.update(self.get_grimoire_fields(date, "stats"))
                    evolution_items.append(evolution_item)
                    last_date = last_date + relativedelta(days=interval_days)
                    average_opened_time = average_opened_time + float(interval_days)

                # upload items to ES
                if len(evolution_items) > 0:
                    num_items += len(evolution_items)
                    ins_items += es_out.bulk_upload(evolution_items, self.get_field_unique_id())

                if num_items != ins_items:
                    missing = num_items - ins_items
                    logger.error(
                        ("[enrich-backlog-analysis] %s/%s missing items",
                            "for Graal Backlog Analysis Study"),
                        str(missing),
                        str(num_items)
                    )
                else:
                    logger.debug(
                        ("[enrich-backlog-analysis] %s items inserted",
                            "for Graal Backlog Analysis Study"),
                        str(num_items)
                    )

        logger.info("[github] End enrich_backlog_analysis study")
