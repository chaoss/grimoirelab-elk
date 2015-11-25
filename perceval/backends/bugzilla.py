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

'''Bugzilla backend for Perseval'''

from datetime import datetime, timedelta
import logging
from urllib.parse import urlparse, urljoin
from xml.etree import ElementTree

import requests
from dateutil import parser
from bs4 import BeautifulSoup, Comment as BFComment

from perceval.backend import Backend
from perceval.utils import get_eta


class Bugzilla(Backend):

    name = "bugzilla"  # to be used for human interaction

    @classmethod
    def add_params(cls, cmdline_parser):
        parser = cmdline_parser
        parser.add_argument("--user",
                            help="Bugzilla user")
        parser.add_argument("--password",
                            help="Bugzilla user password")
        parser.add_argument("-d", "--delay", default="1",
                            help="delay between requests in seconds (1s default)")
        parser.add_argument("-u", "--url", required=True,
                            help="Bugzilla url")
        parser.add_argument("--detail",  default="change",
                            help="list, issue or change (default) detail")
        parser.add_argument("--nissues",  default=200, type=int,
                            help="Number of XML issues to get per query")


    def __init__(self, url = None, nissues = None, detail = "list", 
                 use_cache = False, args = None):

        '''
            :args: command line params to be used
            :url: repository url, incuding bugzilla URL and opt product param
            :nissues: number of issues to get per query
            :detail: list, issue or changes details
            :use_cache: use cache
        '''
        if not args:
            self.url = url
            self.bugzilla_version = self._get_version()
            self.nissues = nissues
            self.detail = detail
        else:
            self.url = args.url
            self.nissues = args.nissues
            self.detail = args.detail
            self.use_cache = args.cache

        self.bugzilla_version = self._get_version()
        super(Bugzilla, self).__init__(use_cache)


    def get_id(self):
        ''' Return bugzilla unique identifier '''

        _index = self._get_domain()[:-1].split('://')[1]

        if 'product' in self.url:
            _index += "-" + self.url.split('product=')[1]

        # ES index names must be lower case
        return _index.replace("/", "_").lower()


    def _get_version(self):

        info_url = self._get_domain() + "show_bug.cgi?id=&ctype=xml"

        r = requests.get(info_url)

        tree = ElementTree.fromstring(r.content)

        self.bugzilla_version = tree.attrib['version']

    def get_field_unique_id(self):
        return "bug_id"


    def _issue_to_cache_item(self, csv_line = None,
                             issue_xml = None, changes_html = None):

        if self.detail == "list":
            bug_id = csv_line.split(",")[0]
            item = {"bug_id": bug_id,
                    "csv": csv_line}
        else:
            bug = issue_xml
            bug_id = bug.findall('bug_id')[0].text
            # TODO.: detect XML enconding and use it
            # xml = {"xml": ElementTree.tostring(bug, encoding="us-ascii")}
            xml_string = ElementTree.tostring(bug, encoding="utf-8")
            # xml_string is of type b'' byte stream in Python3
            xml_string = xml_string.decode('utf-8')
            item = {"bug_id": bug_id,
                    "xml": xml_string,
                    "html": changes_html}
        return item


    def _cache_item_to_issue(self, item):

        issue = None

        if self.detail == "list":
            if 'csv' not in item:
                logging.error("Cache is not of list detail")
                raise
            issue = self._get_issue_json(item['csv'], None, None)
        else:
            xml = ElementTree.fromstring(item['xml'])
            html = item['html']
            csv = None
            issue = self._get_issue_json(csv, xml, html)

        return issue


    def _get_issue_json(self,
                        csv_line = None,
                        issue_xml = None,
                        changes_html = None):
        ''' Create a JSON with all data for an issue. Depending on the detail
            the issue will have more information '''

        issue = {}

        def get_issue_from_csv_line(line):

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
                    # We need this date in Elastic format for incremental
                    if fields[i] in ['changeddate']:
                        data[fields[i]+"_date"] = parser.parse(item).isoformat()

                    i += 1
            except:
                logging.error("Error parsing CSV line")
                logging.error(line)
                logging.error(data_raw)

            return data


        def add_attributes(issue, field, tag):
            ''' Specific logic for using data in XML attributes '''

            if field.tag == "reporter" or field.tag == "assigned_to":
                if 'name' in field.attrib:
                    issue[tag + "_name"] = field.attrib['name']


        def get_issue_from_xml(bug_xml_tree):

            # Bug XML is key=value except long_desc items with XML
            # https://bugzilla.redhat.com/show_bug.cgi?id=300&ctype=xml

            issue = {}
            issue['long_desc'] = []

            for field in bug_xml_tree:
                if field.tag == 'long_desc':
                    new_desc = {}
                    for dfield in field:
                        new_desc[dfield.tag] = dfield.text
                    issue[field.tag].append(new_desc)
                else:
                    tag = field.tag
                    issue[tag] = field.text
                    # We need this date in Elastic format for incremental
                    if tag in ['delta_ts']:
                        issue[tag+"_date"] = parser.parse(field.text).isoformat()

                    add_attributes(issue, field, tag)

            return issue


        def get_changes_from_html(issue_id, html):

            parser = BugzillaChangesHTMLParser(changes_html, issue_id)
            changes = parser.parse_changes()

            return changes


        if csv_line:
            issue = get_issue_from_csv_line(csv_line)

            if not self.use_cache:
                item = self._issue_to_cache_item(csv_line)
                self.cache.item_to_cache(item)

        if issue_xml:

            if not self.use_cache:
                item = self._issue_to_cache_item(None, issue_xml, changes_html)
                self.cache.item_to_cache(item)

            # If we have the XML, replace CSV info
            issue = get_issue_from_xml(issue_xml)

            if changes_html:
                issue['changes'] = get_changes_from_html(issue['bug_id'],
                                                         changes_html)

        return issue


    def _get_domain(self):
        ''' TODO: Old code to be removed once refactored '''

        result = urlparse(self.url)

        if self.url.find("show_bug.cgi") > 0:
            pos = result.path.find('show_bug.cgi')
        elif self.url.find("buglist.cgi") > 0:
            pos = result.path.find('buglist.cgi')

        newpath = result.path[0:pos]
        domain = urljoin(result.scheme + '://' + result.netloc + '/', newpath)
        return domain


    def _get_items_ids(self, url, from_date):

        def get_issues_ids_url(base_url, version, from_date_str=None):
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
                    '''
                    Firefox ITS (3.2.3) replaces %20 with %2520 that causes
                    Bicho to crash
                    '''
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

        logging.info("Getting issues list ...")

        # return ['963423', '954188']

        url = get_issues_ids_url(url, self._get_version(), from_date)

        logging.info("List url %s" % (url))

        r = requests.get(url)

        content = str(r.content, 'UTF-8')

        self.csv = content.split('\n')[1:]

        ids = []
        for line in self.csv:
            # 0: bug_id, 7: changeddate
            values = line.split(',')
            issue_id = values[0]
            change_ts = values[-1].strip('"')
            ids.append([issue_id, change_ts])

        return ids

    def _get_changes_html(self, issue_id):
        base_url = self._get_domain()

        activity_url = base_url + "show_activity.cgi?id=" + issue_id
        logging.debug("Getting changes for issue %s from %s" %
                     (issue_id, activity_url))

        changes_html = requests.get(activity_url).content
        changes_html = changes_html.decode('utf-8')

        return changes_html

    def _get_item(self, bug_xml_tree):
        ''' Return a dict with selected fields '''

        # Time to gather changes for this issue
        issue_id = bug_xml_tree.findall('bug_id')[0].text
        changes_html = None
        if self.detail == "change":
            changes_html = self._get_changes_html(issue_id)

        issue_processed = self._get_issue_json(issue_xml = bug_xml_tree,
                                               changes_html = changes_html)

        return issue_processed


    def _get_items(self, ids):

        def get_issues_info_url(base_url, ids):
            url = base_url + "show_bug.cgi?"

            for issue in ids:
                issue_id = issue[0]
                url += "id=" + issue_id + "&"

            url += "ctype=xml"
            url += "&excludefield=attachmentdata"
            return url

        if self.detail == "list":
            issues = []
            for line in self.csv:
                issue = self._get_issue_json(csv_line = line)
                issues.append(issue)
            self.ids = []
            return issues


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

            for bug in tree:
                issues.append(self.get_item(bug))


            # Each time we receive data from bugzilla server dump it
            self._items_to_es(issues)

            issues_processed += issues

            task_time = (datetime.now() - task_init).total_seconds()
            eta_time = task_time/len(issues) * (total-len(issues_processed))
            eta_min = eta_time / 60.0

            logging.info("Completed %i/%i (ETA iteration: %.2f min)" \
                         % (len(issues_processed), total, eta_min))
        return issues_processed



    def __iter__(self):
        self.ids = self._get_items_ids(self.url, self.start)
        if len(self.ids) > 0:
            self.prj_first_date = parser.parse(self.ids[0][1])
        self.prj_last_date = datetime.now()
        self.total_issues = 0

        self.items_pool  = self._get_items(self.ids)

        return self

    def __next__(self):
        if len(self.items_pool) == 0:
            if len(self.ids) > 0 :
                self.items_pool = self._get_items(self.ids)
            else:
                raise StopIteration

        return self.items_pool.pop()



class BugzillaChangesHTMLParser(object):
    '''
    Parses HTML to get 5 different fields from a table
    '''

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
        '''
        Returns datetime object from string
        '''
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

