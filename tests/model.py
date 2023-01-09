# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2023 Bitergia
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
#     Alberto Pérez García-Plaza <alpgarcia@bitergia.com>
#

"""Stuff to deal with index patterns from JSON files genrated by Kidash."""
import csv
import logging

from unittest import TestCase


class Schema(object):
    """Intends to represent index structure in order
    to compare ES mappings against other compatible
    structures whose types does not match one to one,
    like Kibana types.

    schema_name     -- schema identifier
    __properties    -- dictionary of properties. Example:
        {
            'properties':{
                'author_name': {
                    'type': 'text',
                    'analyzed':  true,
                    'agg': true
                }
                ...
            }
        }

    """

    # List of types allowed for properties in this Schema
    __supported_types = ['text', 'keyword', 'number', 'date', 'boolean',
                         '_source', 'geo_point', 'object']
    # ES meta fields are excluded using the following list
    __excluded_props = ['_all', '_field_names', '_id', '_index', '_meta',
                        '_parent', '_score', '_routing', '_source', '_type',
                        '_uid']
    # In mbox enriched indexes is_mbox is defined
    __excluded_props += ['is_gmane_message', 'is_mbox_message']
    # Cache from askbot could not include this field
    __excluded_props += ['is_askbot_comment']
    # Cache from confluence could not include this field
    __excluded_props += ['is_attachment', 'is_comment', 'is_new_page']

    def __init__(self, schema_name):
        self.schema_name = schema_name
        self.properties = {}

    @staticmethod
    def check_type(src_type):
        """Checks if type is valid

        TypeError -- when type is not supported
        """
        if src_type not in Schema.__supported_types:
            raise TypeError("Type not supported:", src_type)

        return src_type

    def add_property(self, pname, ptype, agg=False):
        """Adds a new property to the current schema. If there
        is already a property with same name, that property
        will be updated.

        Each schema property will have 3 fields:
          - name
          - type
          - agg

        Keyword arguments:
        pname       -- property names
        ptype       -- property type
        agg         -- is this property aggregatable in Kibana?
        """
        # Exclude ES internal properties
        if pname not in self.__excluded_props:
            schema_type = self.check_type(ptype)
            self.properties[pname] = {'type': schema_type}

            if agg:
                self.properties[pname]['agg'] = True

    def get_properties(self):
        """Returns a dictionary containing all schema properties.

            Example:
            {
                'properties':{
                    'author_name': {
                        'type': 'text',
                        'analyzed':  true,
                        'agg': true
                    }
                    ...
                }
            }
        """
        return self.properties

    def compare_properties(self, schema):
        """Compares two schemas. The schema used to call the method
        will be the one we compare from, in advance 'source schema'.
        The schema passed as parameter will be the 'target schema'.

        Returns -- dictionary {status, correct, missing, distinct, message}
            being:
                status 'OK' if all properties in source schema exist in target
                    schema with same values. 'KO' in other case.
                correct: list of properties that matches.
                missing: list of properties missing from target schema.
                distinct: list of properties in both schemas but having
                    with different values.
                message: a string with additional information.
        """
        test_case = TestCase('__init__')
        test_case.maxDiff = None

        status = 'OK'
        correct = []
        missing = []
        distinct = []
        msg = ''
        for pname, pvalue in self.get_properties().items():
            if pname not in schema.get_properties():
                missing.append(pname)
                msg = msg + '\n' + '* Missing property: ' + pname
                status = 'KO'
            else:
                try:
                    test_case.assertDictEqual(pvalue,
                                              schema.get_properties()[pname])
                    correct.append(pname)

                except AssertionError as e:
                    distinct.append(pname)
                    msg = "%s\n* Type mismatch: \n\t%s: %s" %\
                        (msg, pname, str(e).replace('\n', '\n\t'))
                    status = 'KO'

        return {'status': status, 'correct': correct, 'missing': missing,
                'distinct': distinct, 'msg': msg}


class ESMapping(Schema):
    """Represents an ES Mapping"""

    __mapping_types = {'long': 'number',
                       'integer': 'number',
                       'float': 'number',
                       'double': 'number'}
    __non_aggregatables = {'text'}

    @classmethod
    def from_csv(cls, index_name, csv_file):
        """Builds a ESMapping object from a CSV schema definition

        CSV Format:
        field_name, es_type

        :param raw_csv: CSV file to parse
        :returns: ESMapping
        """
        es_mapping = cls(schema_name=index_name)

        with open(csv_file) as f:
            reader = csv.DictReader(f, delimiter=',')
            for row in reader:
                # With current CSVs is not possible to know which
                # fields should be aggregatable. If we rely on
                # '__non_aggregatables' list, then text fields would
                # be non-aggregatables by default. Nevertheless, we
                # want them to be aggregatables in most cases. As a
                # quick fix while CSV schema specification improves,
                # we will assume that fields defined as text in CSVs
                # must be aggregatables in ESMapping.
                property_type = row['type']
                if property_type == 'text':
                    agg = True
                else:
                    agg = None
                es_mapping.add_property(pname=row['name'], ptype=property_type,
                                        agg=agg)

        return es_mapping

    @classmethod
    def from_json(cls, index_name, mapping_json):
        """Builds a ESMapping object from an ES JSON mapping

        We don't know the exact name due to the aliases:
            GET git/_mapping

        Could retrieve:
            {
                "git_enriched": {
                   "mappings": {
                       "items": {
                           "dynamic_templates": [
                               {...}
                           ],
                           "properties": {
                               "Author": {
                                   ...

        So we need to take name as input parameter.

        We can even find several indices if an alias groups them:
           GET affiliations/_mapping

           {
                "git_enriched": {
                   ...
                },
                "gerrit_enriched": {
                   ...

        We store all properties together in the same Schema.
        """
        es_mapping = cls(schema_name=index_name)

        # Get a list of all index mappings in JSON
        mapping_list = list(mapping_json.values())

        # Extract properties from all those grouped indices
        for nested_json in mapping_list:
            # nested_json = mapping_list[0]
            items = nested_json['mappings']['items']['properties'].items()
            for prop, value in items:
                # Support for nested properties:
                # "channel_purpose": {
                #   "properties": {
                #     "value": {
                #       "type": "keyword"
                #     },
                #     "creator": {
                #       "type": "keyword"
                #     },
                #     "last_set": {
                #       "type": "long"
                #     }
                #   }
                # },
                if 'properties' in value:
                    for nested_prop, nested_value in value['properties'].items():
                        prop_name = prop + '.' + nested_prop
                        agg = None

                        if 'fielddata' in nested_value:
                            agg = nested_value['fielddata']
                        if 'type' in nested_value:
                            ptype = nested_value['type']
                        else:
                            logging.warning('Not adding to es_mapping checking the nested value: %s',
                                            nested_value)
                            continue
                        es_mapping.add_property(pname=prop_name,
                                                ptype=ptype,
                                                agg=agg)

                # Support for "regular" properties
                # "channel_id": {
                #   "type": "keyword"
                # },
                else:
                    agg = None
                    if 'fielddata' in value:
                        agg = value['fielddata']
                    es_mapping.add_property(pname=prop, ptype=value['type'],
                                            agg=agg)

        return es_mapping

    @staticmethod
    def get_schema_type(src_type):
        """Type conversion from ES mapping types to schema types.
        """
        out_type = src_type
        if src_type in ESMapping.__mapping_types:
            out_type = ESMapping.__mapping_types[src_type]

        return out_type

    def add_property(self, pname, ptype, agg=None):
        """Overwrites parent method for type conversion
        """
        schema_type = self.get_schema_type(src_type=ptype)

        # Those fields not explicitely specified as aggregatable or not will be
        # aggregatables depending on their type
        schema_agg = agg
        if schema_agg is None:
            if schema_type in self.__non_aggregatables:
                schema_agg = False
            else:
                schema_agg = True
        super().add_property(pname, schema_type, schema_agg)
