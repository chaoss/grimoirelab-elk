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
# Bugzilla backend for Perseval

import argparse
import json
import logging
import requests
from dateutil import parser
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin
from xml.etree import ElementTree
from bs4 import BeautifulSoup, Comment as BFComment


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
    parser.add_argument("-e", "--elastic_host",  default="127.0.0.1",
                        help="Host with elastic search" +
                        "(default: 127.0.0.1)")
    parser.add_argument("--elastic_port",  default="9200",
                        help="elastic search port " +
                        "(default: 9200)")
    parser.add_argument("--delete",  action='store_true',
                        help="delete repository data in ES")
    parser.add_argument("--detail",  default="change",
                        help="list, issue or change (default) detail")
    parser.add_argument("--nissues",  default="200",
                        help="Number of XML issues to get per query")


    args = parser.parse_args()
    return args

##
# Utils methods to be included in Perceval
##

def get_eta(last_update_date, prj_first_date, prj_last_date):
    """ Get the time needed to analyze a day in the project and multiply
    for the number of days in the project """

    if last_update_date == prj_first_date:
        logging.error("Data error: first date and last update the same.")
        return None

    proj_total_days = (prj_last_date - prj_first_date).total_seconds() / (60*60*24)

    app_spend_time_sec = (datetime.now() - app_init).total_seconds()
    prj_seconds_done = (last_update_date-prj_first_date).total_seconds()
    prj_days_done = prj_seconds_done / (60*60*24)

    # Number of seconds needed to analyze a project day
    app_sec_per_proj_day = app_spend_time_sec/prj_days_done

    prj_pending_days = proj_total_days - prj_days_done
    pending_eta_min = (app_sec_per_proj_day * prj_pending_days)/60

    return pending_eta_min

def get_time_diff_days(start_txt, end_txt):
    """ Number of days between two days  """

    start = parser.parse(end_txt)
    end = parser.parse(start_txt)

    seconds_day = float(60*60*24)
    diff_days = \
        (end-start).total_seconds() / seconds_day
    diff_days = float('%.2f' % diff_days)

    return diff_days


class ElasticSearch(object):

    def __init__(self, host, port, index, mappings, clean = False):
        """ clean: remove already exiting index """

        self.url = "http://" + host + ":" + port
        self.index = index
        self.index_raw = index+"_raw"
        self.index_url = self.url+"/"+self.index
        self.index_raw_url = self.url+"/"+self.index_raw

        if requests.get(self.index_url).status_code != 200:
            # Index does no exists
            requests.post(self.index_url)
            logging.info("Created index " + self.index_url)
        else:
            if clean:
                requests.delete(self.index_url)
                requests.post(self.index_url)
                logging.info("Deleted and created index " + self.index_url)
        if mappings:
            self.create_mapping(mappings)


    def create_mapping(self, mappings):

        for mapping in mappings:
            _type = mapping['type']
            url = self.index_url
            url_type = url + "/" + _type

            url_map = url_type+"/_mapping"
            r = requests.put(url_map, data=mapping["value"])

            if r.status_code != 200:
                logging.error("Error creating ES mappings %s" % (r.text))

    def get_last_date(self, _type, field):

        last_date = None

        url = self.index_url
        url += "/" + _type + "/_search"

        data_json = """
        {
            "aggs": {
                "1": {
                  "max": {
                    "field": "%s"
                  }
                }
            }
        }
        """ % (field)

        res = requests.post(url, data=data_json)
        res_json = res.json()

        if 'aggregations' in res_json:
            if "value_as_string" in res_json["aggregations"]["1"]:
                last_date = res_json["aggregations"]["1"]["value_as_string"]
                last_date = parser.parse(last_date).replace(tzinfo=None)
                last_date = last_date.isoformat(" ")

        return last_date


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
                changed_by = changed_by.replace('&#64;', '@')
                date = self._to_datetime_with_secs(cols[1].contents[0].strip())
                date_str = date.isoformat()
                # when the field contains an Attachment, the list has more
                # than a field. For example:
                #
                # [u'\n', u'Attachment #12723', u'\n              Flag\n     ']
                #
                if len(cols[2].contents) > 1:
                    aux_c = " ".join(cols[2].contents)
                    field = aux_c.replace("\n", "").strip()
                else:
                    field = cols[2].contents[0].replace("\n", "").strip()
                removed = cols[3].contents[0].strip()
                added = cols[4].contents[0].strip()
            else:
                # same as above with the Attachment example
                if len(cols[0].contents) > 1:
                    aux_c = " ".join(cols[0].contents)
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

class Bugzilla(object):

    def __init__(self, url, nissues):

        """
            :url: repository url, incuding bugzilla URL and opt product param
            :nissues: number of issues to get per query
        """

        self.url = url
        self.bugzilla_version = self._get_version()
        self.nissues = nissues
        self.elastic = None
        self.elastic_mappings = {}


    def set_elastic(self, es):

        self.elastic = es

    def get_elastic_mappings(self):
        """ Specific mappings needed for ES """

        mapping = """
        {
            "properties": {
               "product": {
                  "type": "string",
                  "index":"not_analyzed"
               },
               "component": {
                  "type": "string",
                  "index":"not_analyzed"
               },
               "assigned_to": {
                  "type": "string",
                  "index":"not_analyzed"
               }
            }
        }
        """

        self.elastic_mappings['issues_list'] = mapping


    def _get_version(self):

        info_url = self._get_domain() + "show_bug.cgi?id=&ctype=xml"

        r = requests.get(info_url)

        tree = ElementTree.fromstring(r.content)

        self.bugzilla_version = tree.attrib['version']


    def _get_last_update_from_es(self, _type):

        if _type == "issues_list":
            field = "changeddate"
        else:
            field = "updated_on"

        last_update = self.elastic.get_last_date(_type, field)

        return last_update


    def _cache_get_changes(self, issue_id):
        """ Get from ES the HTML with the issue changes """

        elastic_type = "changes"

        url = self.elastic.index_raw_url
        url += "/"+elastic_type
        url += "/"+str(issue_id)

        r = requests.get(url)

        if r.status_code == 404:  # Not found
            changes = None
        else:
            changes = r.json()['_source']['html']

        return changes


    def _issues_list_raw_to_es(self, list_csv, last_date):
        """ Store in ES the CSV with the issues listing """

        elastic_type = "issues_list"

        csv = {"csv": list_csv}
        data_json = json.dumps(csv)

        url = self.elastic.index_raw_url
        url += "/"+elastic_type
        url += "/"+str(last_date)
        requests.put(url, data=data_json)


    def _changes_raw_to_es(self, changes_html, issue_id):
        """ Store in ES the HTML for each issue changes """

        elastic_type = "changes"

        html = {"html": changes_html}
        data_json = json.dumps(html)

        url = self.elastic.index_raw_url
        url += "/"+elastic_type
        url += "/"+str(issue_id)
        requests.put(url, data=data_json)


    def _issues_raw_to_es(self, issues_xml):
        """ Store in ES the XML for each issue """

        # TODO: Use _bulk API

        elastic_type = "issues"

        for bug in issues_xml:
            _id = bug.findall('bug_id')[0].text
            # TODO.: detect XML enconding and use it
            # xml = {"xml": ElementTree.tostring(bug, encoding="us-ascii")}
            xml_string = ElementTree.tostring(bug, encoding="utf-8")
            # xml_string is of type b'' byte stream in Python3
            xml_string = xml_string.decode('utf-8')
            xml = {"xml": xml_string}
            data_json = json.dumps(xml)
            url = self.elastic.index_raw_url
            url += "/"+elastic_type
            url += "/"+str(_id)
            requests.put(url, data=data_json)


    def _issues_to_es(self, issues):

        # TODO: use bulk API

        elastic_type = "issues"

        for issue in issues:
            data_json = json.dumps(issue)
            url = self.elastic.index_url
            url += "/"+elastic_type
            url += "/"+str(issue["id"])
            requests.put(url, data=data_json)


    def _issues_list_to_es(self, csv):

        def get_issue_from_list_line(line):

            fields = ["bug_id", "product", "component", "assigned_to",
                      "bug_status"]
            fields += ["resolution", "short_desc", "changeddate"]

            line = line.replace(',","', '","')  # if a field ends with ," remove the ,

            data_raw = line.split(',"')
            data = {}  # fields values

            try:
                i = 0
                for item in data_raw:
                    if item[-1:] == '"':  # remove last item if "
                        item = item[:-1]
                    data[fields[i]] = item
                    if fields[i] in ['changeddate']:
                        data[fields[i]] = parser.parse(item).isoformat()

                    i += 1
            except:
                logging.error("Error parsing CSV line")
                logging.error(line)
                logging.error(data_raw)

            return data


        # TODO: use bulk API
        elastic_type = "issues_list"

        for line in csv:
            issue_fron_list = get_issue_from_list_line(line)
            data_json = json.dumps(issue_fron_list)
            url = self.elastic.index_url
            url += "/"+elastic_type
            url += "/"+str(issue_fron_list["bug_id"])
            requests.put(url, data=data_json)


    def _get_domain(self):
        """ TODO: Old code to be removed once refactored """

        result = urlparse(self.url)

        if self.url.find("show_bug.cgi") > 0:
            pos = result.path.find('show_bug.cgi')
        elif self.url.find("buglist.cgi") > 0:
            pos = result.path.find('buglist.cgi')

        newpath = result.path[0:pos]
        domain = urljoin(result.scheme + '://' + result.netloc + '/', newpath)
        return domain


    def fetch(self):

        def fix_review_dates(issue):
            """ Convert dates so ES detect them """

            for date_field in ['created_on', 'updated_on']:
                if date_field in issue.keys():
                    date_ts = parser.parse(issue[date_field])
                    issue[date_field] = date_ts.strftime('%Y-%m-%dT%H:%M:%S')

        def get_issues_list_url(base_url, version, from_date_str=None):
            # from_date should be increased in 1s to not include last issue

            if from_date_str is not None:
                try:
                    from_date = parser.parse(from_date_str) + timedelta(0, 1)
                    from_date_str = from_date.isoformat(" ")
                except:
                    logging.error("Error in list from date: %s" % (from_date_str))
                    raise

            if '?' in base_url:
                url = base_url + '&'
            else:
                url = base_url + '?'

            if (version == "3.2.3") or (version == "3.2.2"):
                url = url + "order=Last+Changed&ctype=csv"
                if from_date_str:
                    """
                    Firefox ITS (3.2.3) replaces %20 with %2520 that causes
                    Bicho to crash
                    """
                    day = from_date_str[:from_date_str.index(' ')]
                else:
                    day = '1970-01-01'
                url = url + "&chfieldfrom=" + day
            else:
                url = url + "order=changeddate&ctype=csv"
                if from_date_str:
                    day = from_date_str.replace(' ', '%20')
                else:
                    day = '1970-01-01'
                url = url + "&chfieldfrom=" + day

            return url

        def _retrieve_issues_ids(url, from_date):
            logging.info("Getting issues list ...")

            # return ['963423', '954188']

            url = get_issues_list_url(url, self._get_version(), from_date)

            logging.info("List url %s" % (url))

            r = requests.get(url)

            content = str(r.content, 'UTF-8')

            csv = content.split('\n')[1:]

            self._issues_list_to_es(csv)

            ids = []
            for line in csv:
                # 0: bug_id, 7: changeddate
                values = line.split(',')
                issue_id = values[0]
                change_ts = values[len(values)-1].strip('"')
                ids.append([issue_id, change_ts])

            if len(ids) > 0:
                last_date = ids[len(ids)-1][1]
                self._issues_list_raw_to_es(csv, last_date)

            return ids

        def get_issues_info_url(base_url, ids):
            url = base_url + "show_bug.cgi?"

            for issue in ids:
                issue_id = issue[0]
                url += "id=" + issue_id + "&"

            url += "ctype=xml"
            url += "&excludefield=attachmentdata"
            return url

        def add_attributes(issue, field, tag):
            """ Specific logic for using data in XML attributes """

            if field.tag == "reporter" or field.tag == "assigned_to":
                if 'name' in field.attrib:
                    issue[tag + "_name"] = field.attrib['name']

        def get_changes(issue_id):
            base_url = self._get_domain()

            # Try to get changes from cache
            changes_html = self._cache_get_changes(issue_id)
            if changes_html:
                pass
                # logging.info("Cache changes for %s found" % issue_id)
            else:
                activity_url = base_url + "show_activity.cgi?id=" + issue_id
                logging.info("Getting changes for issue %s from %s" %
                             (issue_id, activity_url))

                changes_html = requests.get(activity_url).content
                changes_html = changes_html.decode('utf-8')

                self._changes_raw_to_es(changes_html, issue_id)

            parser = BugzillaChangesHTMLParser(changes_html, issue_id)
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
                get_time_diff_days(issue_processed['created_on'],
                                             issue_processed['updated_on'])
            issue_processed['url'] = self._get_domain(self.url) + "show_bug.cgi?id=" + \
                issue_processed['id']

            fix_review_dates(issue_processed)

            # Time to gather changes for this issue
            issue_processed['changes'] = \
                get_changes(issue_processed['id'])

            return issue_processed


        def _retrieve_issues(ids):

            total = len(ids)
            issues_processed = []  # Issues JSON ready to inserted in ES
            base_url = self._get_domain()

            # We want to use pop() to get the oldest first so we must reverse the
            # order
            ids.reverse()
            while ids:
                query_issues = []
                issues = []
                while len(query_issues) < self.nissues and ids:
                    query_issues.append(ids.pop())

                # Retrieving main bug information
                task_init = datetime.now()
                url = get_issues_info_url(base_url, query_issues)
                issues_raw = requests.get(url)

                tree = ElementTree.fromstring(issues_raw.content)

                self._issues_raw_to_es(tree)

                for bug in tree:
                    issues.append(get_issue_proccesed(bug))

                self._issues_to_es(issues)

                issues_processed += issues

                task_time = (datetime.now() - task_init).total_seconds()
                eta_time = task_time/len(issues) * (total-len(issues_processed))
                eta_min = eta_time / 60.0

                logging.info("Completed %i/%i (ETA iteration: %.2f min)" \
                             % (len(issues_processed), total, eta_min))

            return issues_processed

        _type = "issues"

        logging.info("Getting issues from Bugzilla")

        if args.detail == "list":
            # If working in list detail just use data from issues_list
            last_update = self._get_last_update_from_es("issues_list")
        else:
            # Issues and changes are in the same index
            last_update = self._get_last_update_from_es("issues")

        if last_update is not None:
            logging.info("Incremental analysis: %s" % (last_update))

        ids = _retrieve_issues_ids(self.url, last_update)
        if len(ids) > 0:
            prj_first_date = parser.parse(ids[0][1])
        prj_last_date = app_init
        total_issues = 0

        while ids:
            logging.info("Issues to get in this iteration %i" % len(ids))

            if args.detail in ['issue', 'change']:
                issues_processed = _retrieve_issues(ids)
                logging.info("Issues received in this iteration %i" %
                             len(issues_processed))
                total_issues += len(issues_processed)
            else:
                total_issues += len(ids)


            if len(ids) > 0:
                last_update = ids[len(ids)-1][1]

                eta = get_eta(parser.parse(last_update), prj_first_date,
                              prj_last_date)
                if eta: print ("ETA: %.2f min" % eta)

                ids = _retrieve_issues_ids(self.url, last_update)

        logging.info("Total issues gathered %i" % total_issues)


    def get_index(self, url):
        """ Return bugzilla ES index name from url """

        _index = self._get_domain()[:-1].split('://')[1]

        if 'product' in url:
            _index += "-" + url.split('product=')[1]

        # ES index names must be lower case
        return _index.replace("/", "_").lower()

if __name__ == '__main__':
    app_init = datetime.now()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)

    args = parse_args()

    bugzilla = Bugzilla(args.url, args.nissues)

    es_index_bugzilla = "bugzilla_" + bugzilla.get_index(args.url)
    es_mappings = bugzilla.get_elastic_mappings()
    es = ElasticSearch(args.elastic_host,
                       args.elastic_port,
                       es_index_bugzilla, es_mappings, args.delete)
    bugzilla.set_elastic(es)

    bugzilla.fetch()

    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
