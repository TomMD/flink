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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import java.util.List;

/**
 * Base class for representing an operation structure behind a user-facing {@link Table} API.
 *
 * <p>It represents an operation that can be a node of a relational query. It has a schema, that
 * can be used to validate a {@link QueryTableOperation} applied on top of this one.
 */
@Internal
public interface QueryTableOperation extends TableOperation {

	/**
	 * Resolved schema of this operation. This is the schema of the data
	 * that will be produced by the {@link QueryTableOperation}.
	 */
	TableSchema getTableSchema();

	List<QueryTableOperation> getChildren();

	<T> T accept(QueryTableOperationVisitor<T> visitor);
}
