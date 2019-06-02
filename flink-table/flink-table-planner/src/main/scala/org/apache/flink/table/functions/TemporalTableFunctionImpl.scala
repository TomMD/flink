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

package org.apache.flink.table.functions

import java.sql.Timestamp

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.operations.QueryOperation

/**
  * Class representing temporal table function over some history table.
  * It takes one single argument, the `timeAttribute`, for which it returns matching version of
  * the `underlyingHistoryTable`, from which this [[TemporalTableFunction]] was created.
  *
  * This function shouldn't be evaluated. Instead calls to it should be rewritten by the optimiser
  * into other operators (like Temporal Table Join).
  */
class TemporalTableFunctionImpl private(
    @transient private val underlyingHistoryTable: QueryOperation,
    @transient private val timeAttribute: Expression,
    @transient private val primaryKey: Expression,
    private val resultType: RowTypeInfo)
  extends TemporalTableFunction {

  def eval(row: Timestamp): Unit = {
    throw new IllegalStateException("This should never be called")
  }

  override def getResultType: RowTypeInfo = {
    resultType
  }

  def getTimeAttribute: Expression = {
    timeAttribute
  }

  def getPrimaryKey: Expression = {
    primaryKey
  }

  private[flink] def getUnderlyingHistoryTable: QueryOperation = {
    if (underlyingHistoryTable == null) {
      throw new IllegalStateException("Accessing table field after planing/serialization")
    }
    underlyingHistoryTable
  }
}

object TemporalTableFunctionImpl {
  private[flink] def create(
      operationTree: QueryOperation,
      timeAttribute: Expression,
      primaryKey: Expression): TemporalTableFunction = {
    new TemporalTableFunctionImpl(
      operationTree,
      timeAttribute,
      primaryKey,
      new RowTypeInfo(
        operationTree.getTableSchema.getFieldTypes,
        operationTree.getTableSchema.getFieldNames))
  }
}
