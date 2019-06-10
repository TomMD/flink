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

package org.apache.flink.table.plan.util

import java.util.{List => JList}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.transformations.{SinkTransformation, StreamTransformation}

import scala.collection.JavaConverters._

/**
  * Util for [[org.apache.flink.table.plan.nodes.calcite.Sink]]s.
  */
object SinkUtil {

  /**
    * Gets the newly added sink transformation. The returned sink transformation will be used
    * to set resources.
    */
  def getNewlyAddedSinkTransformation(env: StreamExecutionEnvironment): StreamTransformation[_] = {
    // TODO: get all transformations via Java refection. We should come up with a
    //  better solution for this.
    val field = classOf[StreamExecutionEnvironment].getDeclaredField("transformations")
    field.setAccessible(true)

    val allTransformations = field.get(env).asInstanceOf[JList[StreamTransformation[_]]].asScala
    var newlyAdded: StreamTransformation[_] = null
    for (transform <- allTransformations) {
      if (transform.isInstanceOf[SinkTransformation[_]]) {
        // find the sink transformation with maximal id
        if (newlyAdded == null || newlyAdded.getId < transform.getId) {
          newlyAdded = transform
        }
      }
    }
    if (newlyAdded != null) {
      newlyAdded
    } else {
      throw new IllegalStateException("Can not find a newly added sink transformation. " +
        "This should never happen.")
    }
  }

}
