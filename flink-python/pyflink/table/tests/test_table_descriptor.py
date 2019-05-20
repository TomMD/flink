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
import os

from pyflink.table.table_descriptor import OldCsv, Schema, FileSystem
from pyflink.table.table_sink import CsvTableSink
from pyflink.table.types import DataTypes
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, PyFlinkBatchTableTestCase


class AbstractTableDescriptorTests(object):

    def test_with_format(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor.with_format(OldCsv().field("a", "INT"))

        properties = descriptor.to_properties()

        expected = {'format.type': 'csv',
                    'format.property-version': '1',
                    'format.fields.0.name': 'a',
                    'format.fields.0.type': 'INT',
                    'connector.property-version': '1',
                    'connector.type': 'filesystem'}
        assert properties == expected

    def test_with_schema(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor.with_format(OldCsv()).with_schema(Schema().field("a", "INT"))

        properties = descriptor.to_properties()
        expected = {'schema.0.name': 'a',
                    'schema.0.type': 'INT',
                    'format.type': 'csv',
                    'format.property-version': '1',
                    'connector.type': 'filesystem',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_register_table_sink(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello")]
        csv_source = self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        t_env.register_table_source("source", csv_source)
        # connect sink
        sink_path = os.path.join(self.tempdir + '/streaming2.csv')
        if os.path.isfile(sink_path):
            os.remove(sink_path)

        t_env.connect(FileSystem().path(sink_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .field("a", DataTypes.INT)
                          .field("b", DataTypes.STRING)
                          .field("c", DataTypes.STRING))\
             .with_schema(Schema()
                          .field("a", DataTypes.INT)
                          .field("b", DataTypes.STRING)
                          .field("c", DataTypes.STRING))\
             .register_table_sink("sink")
        t_env.scan("source") \
             .select("a + 1, b, c") \
             .insert_into("sink")
        t_env.execute()

        with open(sink_path, 'r') as f:
            lines = f.read()
            assert lines == '2,Hi,Hello\n' + "3,Hello,Hello\n"

    def test_register_table_source(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello")]
        self.prepare_csv_source(source_path, data, field_types, field_names)
        t_env = self.t_env
        sink_path = os.path.join(self.tempdir + '/streaming2.csv')
        if os.path.isfile(sink_path):
            os.remove(sink_path)
        t_env.register_table_sink(
            "sink",
            field_names, field_types, CsvTableSink(sink_path))

        # connect source
        t_env.connect(FileSystem().path(source_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .field("a", DataTypes.INT)
                          .field("b", DataTypes.STRING)
                          .field("c", DataTypes.STRING))\
             .with_schema(Schema()
                          .field("a", DataTypes.INT)
                          .field("b", DataTypes.STRING)
                          .field("c", DataTypes.STRING))\
             .register_table_source("source")
        t_env.scan("source") \
             .select("a + 1, b, c") \
             .insert_into("sink")
        t_env.execute()

        with open(sink_path, 'r') as f:
            lines = f.read()
            assert lines == '2,Hi,Hello\n' + '3,Hello,Hello\n'

    def test_register_table_source_and_sink(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello")]
        self.prepare_csv_source(source_path, data, field_types, field_names)
        sink_path = os.path.join(self.tempdir + '/streaming2.csv')
        if os.path.isfile(sink_path):
            os.remove(sink_path)
        t_env = self.t_env

        t_env.connect(FileSystem().path(source_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .field("a", DataTypes.INT)
                          .field("b", DataTypes.STRING)
                          .field("c", DataTypes.STRING))\
             .with_schema(Schema()
                          .field("a", DataTypes.INT)
                          .field("b", DataTypes.STRING)
                          .field("c", DataTypes.STRING))\
             .register_table_source_and_sink("source")
        t_env.connect(FileSystem().path(sink_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .field("a", DataTypes.INT)
                          .field("b", DataTypes.STRING)
                          .field("c", DataTypes.STRING))\
             .with_schema(Schema()
                          .field("a", DataTypes.INT)
                          .field("b", DataTypes.STRING)
                          .field("c", DataTypes.STRING))\
             .register_table_source_and_sink("sink")
        t_env.scan("source") \
             .select("a + 1, b, c") \
             .insert_into("sink")
        t_env.execute()

        with open(sink_path, 'r') as f:
            lines = f.read()
            assert lines == '2,Hi,Hello\n' + "3,Hello,Hello\n"


class StreamTableDescriptorTests(PyFlinkStreamTableTestCase, AbstractTableDescriptorTests):

    def test_in_append_mode(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor\
            .with_format(OldCsv())\
            .in_append_mode()

        properties = descriptor.to_properties()
        expected = {'update-mode': 'append',
                    'format.type': 'csv',
                    'format.property-version': '1',
                    'connector.property-version': '1',
                    'connector.type': 'filesystem'}
        assert properties == expected

    def test_in_retract_mode(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor \
            .with_format(OldCsv()) \
            .in_retract_mode()

        properties = descriptor.to_properties()
        expected = {'update-mode': 'retract',
                    'format.type': 'csv',
                    'format.property-version': '1',
                    'connector.property-version': '1',
                    'connector.type': 'filesystem'}
        assert properties == expected

    def test_in_upsert_mode(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor \
            .with_format(OldCsv()) \
            .in_upsert_mode()

        properties = descriptor.to_properties()
        expected = {'update-mode': 'upsert',
                    'format.type': 'csv',
                    'format.property-version': '1',
                    'connector.property-version': '1',
                    'connector.type': 'filesystem'}
        assert properties == expected


class BatchTableDescriptorTests(PyFlinkBatchTableTestCase, AbstractTableDescriptorTests):
    pass
