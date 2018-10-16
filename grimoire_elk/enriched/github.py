# -*- coding: utf-8 -*-
#
# Github to Elastic class helper
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

import json
import logging
import re
import time

import requests
from datetime import datetime

from grimoirelab_toolkit.datetime import str_to_datetime

from .utils import get_time_diff_days

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping


GEOLOCATION_INDEX = '/github/'
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
               "user_geolocation": {
                   "type": "geo_point"
               },
               "title_analyzed": {
                 "type": "text"
               }
            }
        }
        """

        return {"items": mapping}


class GitHubEnrich(Enrich):

    mapping = Mapping

    issue_roles = ['assignee_data', 'user_data']
    pr_roles = ['merged_by_data', 'user_data']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []
        self.studies.append(self.enrich_onion)
        self.studies.append(self.enrich_pull_requests)

        self.users = {}  # cache users
        self.location = {}  # cache users location
        self.location_not_found = []  # location not found in map api

    def set_elastic(self, elastic):
        self.elastic = elastic
        # Recover cache data from Elastic
        self.geolocations = self.geo_locationsfrom__es()

    def get_field_author(self):
        return "user_data"

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "grimoire_creation_date"

    def get_fields_uuid(self):
        return ["assignee_uuid", "user_uuid", "merge_author_uuid"]

    def get_identities(self, item):
        """Return the identities from an item"""

        category = item['category']
        item = item['data']

        identity_types = ['user', 'assignee']
        if category == "pull_request":
            identity_types = ['user', 'merged_by']

        for identity in identity_types:
            if item[identity]:
                # In user_data we have the full user data
                user = self.get_sh_identity(item[identity + "_data"])
                if user:
                    yield user

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
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

    def get_geo_point(self, location):
        geo_point = geo_code = None

        if location is None:
            return geo_point

        if location in self.geolocations:
            geo_location = self.geolocations[location]
            geo_point = {"lat": geo_location['lat'],
                         "lon": geo_location['lon']
                         }

        elif location in self.location_not_found:
            # Don't call the API.
            pass

        else:
            url = 'https://maps.googleapis.com/maps/api/geocode/json'
            params = {'sensor': 'false', 'address': location}
            r = self.requests.get(url, params=params)

            try:
                logger.debug("Using Maps API to find %s" % (location))
                r_json = r.json()
                geo_code = r_json['results'][0]['geometry']['location']
            except Exception:
                if location not in self.location_not_found:
                    logger.debug("Can't find geocode for " + location)
                    self.location_not_found.append(location)

            if geo_code:
                geo_point = {
                    "lat": geo_code['lat'],
                    "lon": geo_code['lng']
                }
                self.geolocations[location] = geo_point

        return geo_point

    def get_github_cache(self, kind, key_):
        """ Get cache data for items of _type using key_ as the cache dict key """

        cache = {}
        res_size = 100  # best size?
        from_ = 0

        index_github = "github/" + kind

        url = self.elastic.url + "/" + index_github
        url += "/_search" + "?" + "size=%i" % res_size
        r = self.requests.get(url)
        type_items = r.json()

        if 'hits' not in type_items:
            logger.info("No github %s data in ES" % (kind))

        else:
            while len(type_items['hits']['hits']) > 0:
                for hit in type_items['hits']['hits']:
                    item = hit['_source']
                    cache[item[key_]] = item
                from_ += res_size
                r = self.requests.get(url + "&from=%i" % from_)
                type_items = r.json()
                if 'hits' not in type_items:
                    break

        return cache

    def geo_locationsfrom__es(self):
        return self.get_github_cache("geolocations", "location")

    def geo_locations_to_es(self):
        max_items = self.elastic.max_items_bulk
        current = 0
        total = 0
        bulk_json = ""

        url = self.elastic.url + GEOLOCATION_INDEX + "geolocations/_bulk"

        logger.debug("Adding geoloc to %s (in %i packs)" % (url, max_items))

        for loc in self.geolocations:
            if current >= max_items:
                total += self.elastic.safe_put_bulk(url, bulk_json)
                bulk_json = ""
                current = 0

            geopoint = self.geolocations[loc]
            location = geopoint.copy()
            location["location"] = loc
            # First upload the raw issue data to ES
            data_json = json.dumps(location)
            # Don't include in URL non ascii codes
            safe_loc = str(loc.encode('ascii', 'ignore'), 'ascii')
            geo_id = str("%s-%s-%s" % (location["lat"], location["lon"],
                                       safe_loc))
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % (geo_id)
            bulk_json += data_json + "\n"  # Bulk document
            current += 1

        if current > 0:
            total += self.elastic.safe_put_bulk(url, bulk_json)

        logger.debug("Adding geoloc to ES Done")

        return total

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
        review_dates = [str_to_datetime(review['created_at']) for review in item['review_comments_data']
                        if item['user']['login'] != review['user']['login']]
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
        else:
            logger.error("rich item not defined for GitHub category %s", item['category'])

        return rich_item

    def enrich_items(self, items):
        total = super(GitHubEnrich, self).enrich_items(items)

        logger.debug("Updating GitHub users geolocations in Elastic")
        self.geo_locations_to_es()  # Update geolocations in Elastic

        return total

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

    def enrich_pull_requests(self, ocean_backend, enrich_backend, raw_issues_index="github_issues_raw"):
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

        logger.info("Doing enrich_pull_request study for index {}".format(self.elastic.index_url))
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
            logger.info("pull_requests processed %i/%i", num_enriched, len(pull_requests_ids))

        self.elastic.bulk_upload(pull_requests, "_item_id")

    def __get_rich_pull(self, item):
        rich_pr = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_pr[f] = item[f]
            else:
                rich_pr[f] = None
        # The real data
        pull_request = item['data']

        rich_pr['time_to_close_days'] = \
            get_time_diff_days(pull_request['created_at'], pull_request['closed_at'])

        if pull_request['state'] != 'closed':
            rich_pr['time_open_days'] = \
                get_time_diff_days(pull_request['created_at'], datetime.utcnow())
        else:
            rich_pr['time_open_days'] = rich_pr['time_to_close_days']

        rich_pr['user_login'] = pull_request['user']['login']
        user = pull_request['user_data']

        if user is not None and user:
            rich_pr['user_name'] = user['name']
            rich_pr['author_name'] = user['name']
            if user['email']:
                rich_pr["user_domain"] = self.get_email_domain(user['email'])
            rich_pr['user_org'] = user['company']
            rich_pr['user_location'] = user['location']
            rich_pr['user_geolocation'] = self.get_geo_point(user['location'])
        else:
            rich_pr['user_name'] = None
            rich_pr["user_domain"] = None
            rich_pr['user_org'] = None
            rich_pr['user_location'] = None
            rich_pr['user_geolocation'] = None
            rich_pr['author_name'] = None

        merged_by = None

        if pull_request['merged_by'] is not None:
            merged_by = pull_request['merged_by_data']
            rich_pr['merge_author_login'] = pull_request['merged_by']['login']
            rich_pr['merge_author_name'] = merged_by['name']
            if merged_by['email']:
                rich_pr["merge_author_domain"] = self.get_email_domain(merged_by['email'])
            rich_pr['merge_author_org'] = merged_by['company']
            rich_pr['merge_author_location'] = merged_by['location']
            rich_pr['merge_author_geolocation'] = \
                self.get_geo_point(merged_by['location'])
        else:
            rich_pr['merge_author_name'] = None
            rich_pr['merge_author_login'] = None
            rich_pr["merge_author_domain"] = None
            rich_pr['merge_author_org'] = None
            rich_pr['merge_author_location'] = None
            rich_pr['merge_author_geolocation'] = None

        rich_pr['id'] = pull_request['id']
        rich_pr['id_in_repo'] = pull_request['html_url'].split("/")[-1]
        rich_pr['repository'] = pull_request['html_url'].rsplit("/", 2)[0]
        rich_pr['title'] = pull_request['title']
        rich_pr['title_analyzed'] = pull_request['title']
        rich_pr['state'] = pull_request['state']
        rich_pr['created_at'] = pull_request['created_at']
        rich_pr['updated_at'] = pull_request['updated_at']
        rich_pr['merged'] = pull_request['merged']
        rich_pr['merged_at'] = pull_request['merged_at']
        rich_pr['closed_at'] = pull_request['closed_at']
        rich_pr['url'] = pull_request['html_url']
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

        if self.sortinghat:
            item[self.get_field_date()] = rich_pr[self.get_field_date()]
            rich_pr.update(self.get_item_sh(item, self.pr_roles))

        return rich_pr

    def __get_rich_issue(self, item):
        rich_issue = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_issue[f] = item[f]
            else:
                rich_issue[f] = None
        # The real data
        issue = item['data']

        rich_issue['time_to_close_days'] = \
            get_time_diff_days(issue['created_at'], issue['closed_at'])

        if issue['state'] != 'closed':
            rich_issue['time_open_days'] = \
                get_time_diff_days(issue['created_at'], datetime.utcnow())
        else:
            rich_issue['time_open_days'] = rich_issue['time_to_close_days']

        rich_issue['user_login'] = issue['user']['login']
        user = issue['user_data']

        if user is not None and user:
            rich_issue['user_name'] = user['name']
            rich_issue['author_name'] = user['name']
            if user['email']:
                rich_issue["user_domain"] = self.get_email_domain(user['email'])
            rich_issue['user_org'] = user['company']
            rich_issue['user_location'] = user['location']
            rich_issue['user_geolocation'] = self.get_geo_point(user['location'])
        else:
            rich_issue['user_name'] = None
            rich_issue["user_domain"] = None
            rich_issue['user_org'] = None
            rich_issue['user_location'] = None
            rich_issue['user_geolocation'] = None
            rich_issue['author_name'] = None

        assignee = None

        if issue['assignee'] is not None:
            assignee = issue['assignee_data']
            rich_issue['assignee_login'] = issue['assignee']['login']
            rich_issue['assignee_name'] = assignee['name']
            if assignee['email']:
                rich_issue["assignee_domain"] = self.get_email_domain(assignee['email'])
            rich_issue['assignee_org'] = assignee['company']
            rich_issue['assignee_location'] = assignee['location']
            rich_issue['assignee_geolocation'] = \
                self.get_geo_point(assignee['location'])
        else:
            rich_issue['assignee_name'] = None
            rich_issue['assignee_login'] = None
            rich_issue["assignee_domain"] = None
            rich_issue['assignee_org'] = None
            rich_issue['assignee_location'] = None
            rich_issue['assignee_geolocation'] = None

        rich_issue['id'] = issue['id']
        rich_issue['id_in_repo'] = issue['html_url'].split("/")[-1]
        rich_issue['repository'] = issue['html_url'].rsplit("/", 2)[0]
        rich_issue['title'] = issue['title']
        rich_issue['title_analyzed'] = issue['title']
        rich_issue['state'] = issue['state']
        rich_issue['created_at'] = issue['created_at']
        rich_issue['updated_at'] = issue['updated_at']
        rich_issue['closed_at'] = issue['closed_at']
        rich_issue['url'] = issue['html_url']
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

        if self.sortinghat:
            item[self.get_field_date()] = rich_issue[self.get_field_date()]
            rich_issue.update(self.get_item_sh(item, self.issue_roles))

        return rich_issue


class GitHubUser(object):
    """ Helper class to manage data from a Github user """

    users = {}  # cache with users from github

    def __init__(self, user):

        self.login = user['login']
        self.email = user['email']
        if 'company' in user:
            self.company = user['company']
        self.orgs = user['orgs']
        self.org = self._getOrg()
        self.name = user['name']
        self.location = user['location']

    def _getOrg(self):
        company = None

        if self.company:
            company = self.company

        if company is None:
            company = ''
            # Return the list of orgs
            for org in self.orgs:
                company += org['login'] + ";;"
            company = company[:-2]

        return company
