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
from pyflink.table.types import DataTypes
from pyflink.table.table_descriptor import Schema, Rowtime
from pyflink.testing.test_case_utils import PyFlinkTestCase


class SchemaDescriptorTests(PyFlinkTestCase):

    def test_field(self):
        schema = Schema()

        schema = schema\
            .field("int_field", DataTypes.INT)\
            .field("long_field", DataTypes.LONG)\
            .field("string_field", DataTypes.STRING)\
            .field("timestamp_field", DataTypes.TIMESTAMP)\
            .field("time_field", DataTypes.TIME)\
            .field("date_field", DataTypes.DATE)\
            .field("double_field", DataTypes.DOUBLE)\
            .field("float_field", DataTypes.FLOAT)\
            .field("byte_field", DataTypes.BYTE)\
            .field("short_field", DataTypes.SHORT)\
            .field("boolean_field", DataTypes.BOOLEAN)

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.type': 'BIGINT',
                    'schema.2.name': 'string_field',
                    'schema.2.type': 'VARCHAR',
                    'schema.3.name': 'timestamp_field',
                    'schema.3.type': 'TIMESTAMP',
                    'schema.4.name': 'time_field',
                    'schema.4.type': 'TIME',
                    'schema.5.name': 'date_field',
                    'schema.5.type': 'DATE',
                    'schema.6.name': 'double_field',
                    'schema.6.type': 'DOUBLE',
                    'schema.7.name': 'float_field',
                    'schema.7.type': 'FLOAT',
                    'schema.8.name': 'byte_field',
                    'schema.8.type': 'TINYINT',
                    'schema.9.name': 'short_field',
                    'schema.9.type': 'SMALLINT',
                    'schema.10.name': 'boolean_field',
                    'schema.10.type': 'BOOLEAN'}
        assert properties == expected

    def test_field_in_string(self):
        schema = Schema()

        schema = schema\
            .field("int_field", 'INT')\
            .field("long_field", 'BIGINT')\
            .field("string_field", 'VARCHAR')\
            .field("timestamp_field", 'SQL_TIMESTAMP')\
            .field("time_field", 'SQL_TIME')\
            .field("date_field", 'SQL_DATE')\
            .field("double_field", 'DOUBLE')\
            .field("float_field", 'FLOAT')\
            .field("byte_field", 'TINYINT')\
            .field("short_field", 'SMALLINT')\
            .field("boolean_field", 'BOOLEAN')

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.type': 'BIGINT',
                    'schema.2.name': 'string_field',
                    'schema.2.type': 'VARCHAR',
                    'schema.3.name': 'timestamp_field',
                    'schema.3.type': 'SQL_TIMESTAMP',
                    'schema.4.name': 'time_field',
                    'schema.4.type': 'SQL_TIME',
                    'schema.5.name': 'date_field',
                    'schema.5.type': 'SQL_DATE',
                    'schema.6.name': 'double_field',
                    'schema.6.type': 'DOUBLE',
                    'schema.7.name': 'float_field',
                    'schema.7.type': 'FLOAT',
                    'schema.8.name': 'byte_field',
                    'schema.8.type': 'TINYINT',
                    'schema.9.name': 'short_field',
                    'schema.9.type': 'SMALLINT',
                    'schema.10.name': 'boolean_field',
                    'schema.10.type': 'BOOLEAN'}
        assert properties == expected

    def test_from_origin_field(self):
        schema = Schema()

        schema = schema\
            .field("int_field", DataTypes.INT)\
            .field("long_field", DataTypes.LONG).from_origin_field("origin_field_a")\
            .field("string_field", DataTypes.STRING)

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.type': 'BIGINT',
                    'schema.1.from': 'origin_field_a',
                    'schema.2.name': 'string_field',
                    'schema.2.type': 'VARCHAR'}
        assert properties == expected

    def test_proctime(self):
        schema = Schema()

        schema = schema\
            .field("int_field", DataTypes.INT)\
            .field("ptime", DataTypes.LONG).proctime()\
            .field("string_field", DataTypes.STRING)

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.type': 'INT',
                    'schema.1.name': 'ptime',
                    'schema.1.type': 'BIGINT',
                    'schema.1.proctime': 'true',
                    'schema.2.name': 'string_field',
                    'schema.2.type': 'VARCHAR'}
        assert properties == expected

    def test_rowtime(self):
        schema = Schema()

        schema = schema\
            .field("int_field", DataTypes.INT)\
            .field("long_field", DataTypes.LONG)\
            .field("rtime", DataTypes.LONG)\
            .rowtime(
                Rowtime().timestamps_from_field("long_field").watermarks_periodic_bounded(5000))\
            .field("string_field", DataTypes.STRING)

        properties = schema.to_properties()
        print(properties)
        expected = {'schema.0.name': 'int_field',
                    'schema.0.type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.type': 'BIGINT',
                    'schema.2.name': 'rtime',
                    'schema.2.type': 'BIGINT',
                    'schema.2.rowtime.timestamps.type': 'from-field',
                    'schema.2.rowtime.timestamps.from': 'long_field',
                    'schema.2.rowtime.watermarks.type': 'periodic-bounded',
                    'schema.2.rowtime.watermarks.delay': '5000',
                    'schema.3.name': 'string_field',
                    'schema.3.type': 'VARCHAR'}
        assert properties == expected
