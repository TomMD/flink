################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from pyflink.table.table_descriptor import OldCsv
from pyflink.table.types import DataTypes
from pyflink.testing.test_case_utils import PyFlinkTestCase


class OldCsvDescriptorTests(PyFlinkTestCase):

    def test_field_delimiter(self):
        csv = OldCsv()

        csv.field_delimiter("|")

        properties = csv.to_properties()
        expected = {'format.field-delimiter': '|',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        assert properties == expected

    def test_line_delimiter(self):
        csv = OldCsv()

        csv.line_delimiter(";")

        expected = {'format.type': 'csv',
                    'format.property-version': '1',
                    'format.line-delimiter': ';'}

        properties = csv.to_properties()
        assert properties == expected

    def test_ignore_parse_errors(self):
        csv = OldCsv()

        csv.ignore_parse_errors()

        properties = csv.to_properties()
        expected = {'format.ignore-parse-errors': 'true',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        assert properties == expected

    def test_quote_character(self):
        csv = OldCsv()

        csv.quote_character("*")

        properties = csv.to_properties()
        expected = {'format.quote-character': '*',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        assert properties == expected

    def test_comment_prefix(self):
        csv = OldCsv()

        csv.comment_prefix("#")

        properties = csv.to_properties()
        expected = {'format.comment-prefix': '#',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        assert properties == expected

    def test_ignore_first_line(self):
        csv = OldCsv()

        csv.ignore_first_line()

        properties = csv.to_properties()
        expected = {'format.ignore-first-line': 'true',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        assert properties == expected

    def test_field(self):
        csv = OldCsv()

        csv.field("a", DataTypes.LONG)
        csv.field("b", DataTypes.STRING)
        csv.field("c", "SQL_TIMESTAMP")

        properties = csv.to_properties()
        expected = {'format.fields.0.name': 'a',
                    'format.fields.0.type': 'BIGINT',
                    'format.fields.1.name': 'b',
                    'format.fields.1.type': 'VARCHAR',
                    'format.fields.2.name': 'c',
                    'format.fields.2.type': 'SQL_TIMESTAMP',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        assert properties == expected
