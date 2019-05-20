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
from pyflink.table.table_descriptor import (Rowtime, PreserveWatermarks, AscendingTimestamps,
                                            BoundedOutOfOrderTimestamps)
from pyflink.testing.test_case_utils import PyFlinkTestCase


class RowTimeDescriptorTests(PyFlinkTestCase):

    def test_timestamps_from_field(self):
        rowtime = Rowtime()

        rowtime = rowtime.timestamps_from_field("rtime")

        properties = rowtime.to_properties()
        expect = {'rowtime.timestamps.type': 'from-field', 'rowtime.timestamps.from': 'rtime'}
        assert properties == expect

    def test_timestamps_from_source(self):
        rowtime = Rowtime()

        rowtime = rowtime.timestamps_from_source()

        properties = rowtime.to_properties()
        expect = {'rowtime.timestamps.type': 'from-source'}
        assert properties == expect

    def test_timestamps_from_extractor(self):
        rowtime = Rowtime()

        rowtime = rowtime.timestamps_from_extractor(
            "org.apache.flink.table.descriptors.RowtimeTest$CustomExtractor")

        properties = rowtime.to_properties()
        expect = {'rowtime.timestamps.type': 'custom',
                  'rowtime.timestamps.class':
                  'org.apache.flink.table.descriptors.RowtimeTest$CustomExtractor',
                  'rowtime.timestamps.serialized':
                  'rO0ABXNyAD5vcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmRlc2NyaXB0b3JzLlJvd3RpbWVUZXN0JEN1c3R'
                  'vbUV4dHJhY3RvcoaChjMg55xwAgABTAAFZmllbGR0ABJMamF2YS9sYW5nL1N0cmluZzt4cgA-b3JnLm'
                  'FwYWNoZS5mbGluay50YWJsZS5zb3VyY2VzLnRzZXh0cmFjdG9ycy5UaW1lc3RhbXBFeHRyYWN0b3Jf1'
                  'Y6piFNsGAIAAHhwdAACdHM'}
        assert properties == expect

    def test_watermarks_periodic_ascending(self):
        rowtime = Rowtime()

        rowtime = rowtime.watermarks_periodic_ascending()

        properties = rowtime.to_properties()
        expect = {'rowtime.watermarks.type': 'periodic-ascending'}
        assert properties == expect

    def test_watermarks_periodic_bounded(self):
        rowtime = Rowtime()

        rowtime = rowtime.watermarks_periodic_bounded(1000)

        properties = rowtime.to_properties()
        expect = {'rowtime.watermarks.type': 'periodic-bounded',
                  'rowtime.watermarks.delay': '1000'}
        assert properties == expect

    def test_watermarks_from_source(self):
        rowtime = Rowtime()

        rowtime = rowtime.watermarks_from_source()

        properties = rowtime.to_properties()
        expect = {'rowtime.watermarks.type': 'from-source'}
        assert properties == expect

    def test_watermarks_from_strategy_preserve_watermarks(self):
        rowtime = Rowtime()

        rowtime = rowtime.watermarks_from_strategy(PreserveWatermarks())

        properties = rowtime.to_properties()
        expect = {'rowtime.watermarks.type': 'from-source'}
        assert properties == expect

    def test_watermarks_from_strategy_ascending_timestamps(self):
        rowtime = Rowtime()

        rowtime = rowtime.watermarks_from_strategy(AscendingTimestamps())

        properties = rowtime.to_properties()
        expect = {'rowtime.watermarks.type': 'periodic-ascending'}
        assert properties == expect

    def test_watermarks_from_strategy_bounded_out_of_order_timestamps(self):
        rowtime = Rowtime()

        rowtime = rowtime.watermarks_from_strategy(BoundedOutOfOrderTimestamps(1000))

        properties = rowtime.to_properties()
        expect = {'rowtime.watermarks.type': 'periodic-bounded',
                  'rowtime.watermarks.delay': '1000'}
        assert properties == expect
