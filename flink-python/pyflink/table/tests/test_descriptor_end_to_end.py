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

from pyflink.table import DataTypes
from pyflink.table.table_descriptor import FileSystem, OldCsv, Schema
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class StreamDescriptorEndToEndTests(PyFlinkStreamTableTestCase):

    def test_end_to_end(self):
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        with open(source_path, 'w') as f:
            lines = 'a,b,c\n' + \
                    '1,hi,hello\n' + \
                    '#comments\n' + \
                    "error line\n" + \
                    '2,"hi,world!",hello\n'
            f.write(lines)
            f.close()
        sink_path = os.path.join(self.tempdir + '/streaming2.csv')
        t_env = self.t_env
        # connect source
        t_env.connect(FileSystem().path(source_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .line_delimiter("\n")
                          .ignore_parse_errors()
                          .quote_character('"')
                          .comment_prefix("#")
                          .ignore_first_line()
                          .field("a", "INT")
                          .field("b", "VARCHAR")
                          .field("c", "VARCHAR"))\
             .with_schema(Schema()
                          .field("a", "INT")
                          .field("b", "VARCHAR")
                          .field("c", "VARCHAR"))\
             .in_append_mode()\
             .register_table_source("source")
        # connect sink
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
            assert lines == '2,hi,hello\n' + '3,hi,world!,hello\n'
