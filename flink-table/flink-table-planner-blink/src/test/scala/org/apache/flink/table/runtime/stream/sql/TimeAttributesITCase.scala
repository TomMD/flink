/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.table.runtime.utils.{StreamingTestBase, TestingRetractSink}
import org.apache.flink.types.Row

import org.junit.{Assert, Test}

class TimeAttributesITCase extends StreamingTestBase {

  @Test
  def testAggregationOnTimeAttribute(): Unit = {
    val data: Seq[(Long, Int)] = Seq(
      (1L, 1),
      (2L, 2),
      (3L, 3)
    )
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val t = env.fromCollection(data)
        .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Long, Int)](0))
        .toTable(tEnv, 'rowtime, 'a)
    tEnv.registerTable("T", t)
    val t1 = tEnv.sqlQuery(
      "select max(rowtime), count(a) from T")
    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = Seq("1970-01-01 00:00:00.003,3")
    Assert.assertEquals(expected, sink.getRetractResults)
  }
}
