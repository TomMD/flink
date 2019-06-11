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

package org.apache.flink.table.sources;

import java.util.List;
import java.util.Map;

/**
 * Adds support for partition pruning to a [[TableSource]].
 * A {@link TableSource} extending this interface is a partition table and able to
 * prune partitions before reading data.
 *
 * <p>Partitions is represented as a {@code Map<String, String>} which maps from partition-key
 * to value. The partition columns and values are NOT of strict order in the Map. The correct
 * order of partition fields should be provided by {@link #getPartitionKeys()}.
 */
public interface PartitionPrunableTableSource {

	/**
	 * Gets all partitions as key-value map belong to this table.
	 */
	List<Map<String, String>> getAllPartitions();

	/**
	 * Get the partition keys of the table. This should be an empty set if the table
	 * is not partitioned.
	 *
	 * @return partition keys of the table
	 */
	List<String> getPartitionKeys();

	/**
	 * Return the flag to indicate whether partition pruning has been tried. Must return true on
	 * the returned instance of {@link #applyPartitionPruning(List)}.
	 */
	boolean isPartitionPruned();

	/**
	 * Applies the remaining partitions to the table source. The {@code remainingPartitions} is
	 * the remaining partitions of {@link #getAllPartitions()} after partition pruning applied.
	 *
	 * <p>After trying to apply partition pruning, we should return a new {@link TableSource}
	 * instance which holds all pruned-partitions. Even if we actually pushed nothing down,
	 * it is recommended that we still return a new {@link TableSource} instance since we will
	 * mark the returned instance as partition pruned and will never try again.
	 *
	 * @param remainingPartitions Remaining partitions after partition pruning applied.
	 * @return A new cloned instance of {@link TableSource}.
	 */
	TableSource applyPartitionPruning(List<Map<String, String>> remainingPartitions);

	/**
	 * Whether drop partition predicates after apply partition pruning.
	 *
	 * @return true only if the result is correct without partition predicate.
	 */
	default boolean supportDropPartitionPredicate() {
		return false;
	}
}
