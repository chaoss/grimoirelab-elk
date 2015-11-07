#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Bugzilla tickets for Elastic Search
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
# TODO: Just a playing script yet.
#     - Use the _bulk API from ES to improve indexing

import argparse
import json
import logging
import requests
from dateutil import parser
from urllib.parse import urlparse
from xml.etree import ElementTree
from bs4 import BeautifulSoup, Comment as BFComment


class BugzillaChangesHTMLParser():
    """
    Parses HTML to get 5 different fields from a table
    """

    field_map = {}
    status_map = {}
    resolution_map = {}

    def __init__(self, html, idBug):
        self.html = html
        self.idBug = idBug
        self.field_map = {'Status': u'status', 'Resolution': u'resolution'}

    def sanityze_change(self, field, old_value, new_value):
        field = self.field_map.get(field, field)
        old_value = old_value.strip()
        new_value = new_value.strip()
        if field == 'status':
            old_value = self.status_map.get(old_value, old_value)
            new_value = self.status_map.get(new_value, new_value)
        elif field == 'resolution':
            old_value = self.resolution_map.get(old_value, old_value)
            new_value = self.resolution_map.get(new_value, new_value)

        return field, old_value, new_value

    def remove_comments(self, soup):
        cmts = soup.findAll(text=lambda text: isinstance(text, BFComment))
        [comment.extract() for comment in cmts]

    def _to_datetime_with_secs(self, str_date):
        """
        Returns datetime object from string
        """
        return parser.parse(str_date).replace(tzinfo=None)

    def parse_changes(self):
        soup = BeautifulSoup(self.html)
        self.remove_comments(soup)
        remove_tags = ['a', 'span', 'i']
        changes = []
        tables = soup.findAll('table')

        # We look for the first table with 5 cols
        table = None
        for table in tables:
            if len(table.tr.findAll('th', recursive=False)) == 5:
                try:
                    for i in table.findAll(remove_tags):
                        i.replaceWith(i.text)
                except:
                    logging.error("error removing HTML tags")
                break

        if table is None:
            return changes

        rows = list(table.findAll('tr'))
        for row in rows[1:]:
            cols = list(row.findAll('td'))
            if len(cols) == 5:
                changed_by = cols[0].contents[0].strip()
                changed_by = unicode(changed_by.replace('&#64;', '@'))
                date = self._to_datetime_with_secs(cols[1].contents[0].strip())
                date_str = date.isoformat()
                # when the field contains an Attachment, the list has more
                # than a field. For example:
                #
                # [u'\n', u'Attachment #12723', u'\n              Flag\n     ']
                #
                if len(cols[2].contents) > 1:
                    aux_c = unicode(" ".join(cols[2].contents))
                    field = unicode(aux_c.replace("\n", "").strip())
                else:
                    field = unicode(cols[2].contents[0].
                                    replace("\n", "").strip())
                removed = unicode(cols[3].contents[0].strip())
                added = unicode(cols[4].contents[0].strip())
            else:
                # same as above with the Attachment example
                if len(cols[0].contents) > 1:
                    aux_c = unicode(" ".join(cols[0].contents))
                    field = aux_c.replace("\n", "").strip()
                else:
                    field = cols[0].contents[0].strip()
                removed = cols[1].contents[0].strip()
                added = cols[2].contents[0].strip()

            field, removed, added = self.sanityze_change(field, removed, added)
            change = {"changed_by": changed_by,
                      "field": field,
                      "removed": removed,
                      "added": added,
                      "date": date_str
                      }
            changes.append(change)

        return changes


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user",
                        help="Bugzilla user")
    parser.add_argument("--password",
                        help="Bugzilla user password")
    parser.add_argument("-d", "--delay", default="1",
                        help="delay between requests in seconds (1s default)")
    parser.add_argument("-u", "--url", required=True,
                        help="Bugzilla url")
    parser.add_argument("-e", "--elasticsearch_host",  default="127.0.0.1",
                        help="Host with elasticsearch" +
                        "(default: 127.0.0.1)")
    parser.add_argument("--elasticsearch_port",  default="9200",
                        help="elasticsearch port " +
                        "(default: 9200)")
    parser.add_argument("--delete",  action='store_true',
                        help="delete repository data in ES")

    args = parser.parse_args()
    return args


def get_bugzilla_version():

    global bugzilla_version

    if bugzilla_version:
        return bugzilla_version

    info_url = get_domain(args.url) + "show_bug.cgi?id=&ctype=xml"

    r = requests.get(info_url)

    tree = ElementTree.fromstring(r.content)

    bugzilla_version = tree.attrib['version']


def get_time_to_last_update_days(created_at_txt, updated_at_txt):
    """ Number of days between creation and last update """

    # updated_at - created_at
    updated_at = parser.parse(updated_at_txt)
    created_at = parser.parse(created_at_txt)

    seconds_day = float(60*60*24)
    update_time = \
        (updated_at-created_at).total_seconds() / seconds_day
    update_time = float('%.2f' % update_time)

    return update_time


def get_elastic_index_raw():

    return elasticsearch_url + "/" + elasticsearch_index_raw


def get_elastic_index():

    return elasticsearch_url + "/"+elasticsearch_index


def init_es():

    _types = ['issues']

    # Remove and create indexes. Create mappings.
    url_raw = get_elastic_index_raw()
    url = get_elastic_index()

    requests.delete(url_raw)
    requests.delete(url)

    requests.post(url_raw)
    requests.post(url)


def get_last_update_from_es(_type):

    return None

    last_update = None

    url = get_elastic_index_raw()
    url += "/" + _type + "/_search"

    data_json = """
    {
        "aggs": {
            "1": {
              "max": {
                "field": "updated_at"
              }
            }
        }
    }
    """

    res = requests.post(url, data=data_json)
    res_json = res.json()

    if 'aggregations' in res_json:
        if "value_as_string" in res_json["aggregations"]["1"]:
            last_update = res_json["aggregations"]["1"]["value_as_string"]

    return last_update


def changesHTML2ES(changes_html, issue_id):
    """ Store in ES the HTML for each issue changes """

    elasticsearch_type = "changes_raw"

    html = {"html": changes_html}
    data_json = json.dumps(html)

    url = get_elastic_index()
    url += "/"+elasticsearch_type
    url += "/"+str(issue_id)
    requests.put(url, data=data_json)


def issuesXML2ES(issues_xml):
    """ Store in ES the XML for each issue """

    # TODO: Use _bulk API

    elasticsearch_type = "issues_raw"

    for bug in issues_xml:
        _id = bug.findall('bug_id')[0].text
        # TODO.: detect XML enconding and use it
        xml = {"xml": ElementTree.tostring(bug, encoding="us-ascii")}
        data_json = json.dumps(xml)
        url = get_elastic_index()
        url += "/"+elasticsearch_type
        url += "/"+str(_id)
        requests.put(url, data=data_json)


def issues2ES(issues):

    # TODO: use bulk API

    elasticsearch_type = "issues"

    for issue in issues:
        data_json = json.dumps(issue)
        url = get_elastic_index()
        url += "/"+elasticsearch_type
        url += "/"+str(issue["id"])
        requests.put(url, data=data_json)


def get_domain(url):
    result = urlparse.urlparse(url)

    if url.find("show_bug.cgi") > 0:
        pos = result.path.find('show_bug.cgi')
    elif url.find("buglist.cgi") > 0:
        pos = result.path.find('buglist.cgi')

    newpath = result.path[0:pos]
    domain = urlparse.urljoin(result.scheme + '://' + result.netloc + '/',
                              newpath)
    return domain


def get_issues(url):

    def fix_review_dates(issue):
        """ Convert dates so ES detect them """

        for date_field in ['created_on', 'updated_on']:
            if date_field in issue.keys():
                date_ts = parser.parse(issue[date_field])
                issue[date_field] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')

    def get_issues_list_url(base_url, version):
        if '?' in base_url:
            url = base_url + '&'
        else:
            url = base_url + '?'

        if ((version == "3.2.3") or (version == "3.2.2")):
            url = url + "order=Last+Changed&ctype=csv"
        else:
            url = url + "order=changeddate&ctype=csv"
        return url

    def retrieve_issues_ids(url):
        logging.info("Getting issues list ...")

        # return ['963423','954188']

        url = get_issues_list_url(url, bugzilla_version)

        r = requests.get(url)

        csv = r.content.split('\n')[1:]
        ids = []
        for line in csv:
            # 0: bug_id, 7: changeddate
            values = line.split(',')
            issue_id = values[0]
            # change_ts = values[7].strip('"')

            ids.append(issue_id)

        return ids

    def get_issues_info_url(base_url, ids):
        url = base_url + "show_bug.cgi?"

        for issue_id in ids:
            url += "id=" + issue_id + "&"

        url += "ctype=xml"
        url += "&excludefield=attachmentdata"
        return url

    def add_attributes(issue, field, tag):
        """ Specific logic for using data in XML attributes """

        if field.tag == "reporter" or field.tag == "assigned_to":
            if 'name' in field.attrib:
                issue[tag + "_name"] = field.attrib['name']

    def get_changes(base_url, issue_id):
        activity_url = base_url + "show_activity.cgi?id=" + issue_id
        logging.info("Getting changes for issue %s from %s" %
                     (issue_id, activity_url))

        data = requests.get(activity_url).content

        changesHTML2ES(data, issue_id)

        parser = BugzillaChangesHTMLParser(data, issue_id)
        changes = parser.parse_changes()

        return changes

    def get_issue_proccesed(bug_xml_tree):
        """ Return a dict with selected fields """

        issue_processed = {}

        fields = ['reporter', 'assigned_to', 'bug_status', 'resolution']
        fields += ['creation_ts', 'delta_ts', 'product', 'component']
        fields += ['bug_id', 'short_desc', 'priority']
        fields += ['version']

        fields_rename = {"delta_ts": "updated_on",
                         "creation_ts": "created_on",
                         "bug_id": "id",
                         "reporter": "submitted_by"}

        # Extra fields: enriched issue
        issue_processed['number_of_comments'] = 0
        issue_processed['time_to_last_update_days'] = None
        issue_processed['url'] = None

        for field in bug_xml_tree:
            if field.tag in fields:
                tag = field.tag
                if tag in fields_rename:
                    tag = fields_rename[tag]
                issue_processed[tag] = field.text

                add_attributes(issue_processed, field, tag)

            if field.tag == "long_desc":
                issue_processed['number_of_comments'] += 1

        issue_processed['time_to_last_update_days'] = \
            get_time_to_last_update_days(issue_processed['created_on'],
                                         issue_processed['updated_on'])
        issue_processed['url'] = get_domain(url) + "show_bug.cgi?id=" + \
            issue_processed['id']

        fix_review_dates(issue_processed)

        # Time to gather changes for this issue
        issue_processed['changes'] = \
            get_changes(get_domain(url), issue_processed['id'])

        return issue_processed

    def retrieve_issues(ids, base_url):

        issues_processed = []  # Issues JSON ready to inserted in ES

        # We want to use pop() to get the oldest first so we must reverse the
        # order
        ids.reverse()
        while(ids):
            query_issues = []
            issues = []
            while (len(query_issues) < issues_per_query and ids):
                query_issues.append(ids.pop())

            # Retrieving main bug information
            url = get_issues_info_url(base_url, query_issues)
            logging.info("Getting %i issues data" % (issues_per_query))
            issues_raw = requests.get(url)
            logging.info("Processing issues data")

            tree = ElementTree.fromstring(issues_raw.content)

            issuesXML2ES(tree)

            for bug in tree:
                issues.append(get_issue_proccesed(bug))

            issues2ES(issues)

            issues_processed += issues

        return issues_processed

    _type = "issues"

    logging.info("Getting issues from Bugzilla")

    # TODO: not all issues processed. Need to iterate.
    ids = retrieve_issues_ids(url)

    logging.info("Total issues to be gathered %i" % len(ids))

    base_url = get_domain(url)
    issues_processed = retrieve_issues(ids, base_url)

    logging.info("Total issues gathered %i" % len(issues_processed))


def get_bugzilla_index(url):
    """ Return bugzilla ES index name from url """

    _index = get_domain(url)[:-1].split('://')[1]

    if 'product' in url:
        _index += "-" + url.split('product=')[1]

    return _index.lower()  # ES index names must be lower case

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)

    args = parse_args()

    bugzilla_version = None
    get_bugzilla_version()

    users = {}

    elasticsearch_url = "http://"
    elasticsearch_url += args.elasticsearch_host + ":" + \
        args.elasticsearch_port
    elasticsearch_index_bugzilla = "bugzilla"
    elasticsearch_index = elasticsearch_index_bugzilla + \
        "_%s" % (get_bugzilla_index(args.url))
    elasticsearch_index_raw = elasticsearch_index+"_raw"

    init_es()  # until we have incremental support, always from scratch

    issues_per_query = 200  # number of tickets per query

    issues_prs_count = get_issues(args.url)

    logging.info("Total Issues Pull Requests " + str(issues_prs_count))
