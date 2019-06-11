/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Thread-safe Utility for tracking partitions.
 */
public class PartitionTable {

	private final Object lock = new Object();

	private final Map<JobID, Set<ResultPartitionID>> trackedPartitionsPerJob = new HashMap<>(8);

	boolean hasTrackedPartitions(JobID jobId) {
		return trackedPartitionsPerJob.containsKey(jobId);
	}

	/**
	 * Tracks the given partition for the given attempt.
	 * These partitions are not considered finished.
	 *
	 * @param partitionId
	 */
	void startTrackingPartition(JobID jobId, ResultPartitionID partitionId) {
		Preconditions.checkNotNull(partitionId);

		synchronized (lock) {
			trackedPartitionsPerJob.compute(jobId, (ignored, partitionIds) -> {
				if (partitionIds == null) {
					partitionIds = new HashSet<>(8);
				}
				partitionIds.add(partitionId);
				return partitionIds;
			});
		}
	}

	/**
	 * Stops the tracking of the given partition for the given attempt.
	 *
	 * @param partitionId
	 */
	void stopTrackingPartition(JobID jobId, ResultPartitionID partitionId) {
		Preconditions.checkNotNull(partitionId);

		synchronized (lock) {
			trackedPartitionsPerJob.computeIfPresent(jobId, (attemptID, partitionIds) -> {
				partitionIds.remove(partitionId);
				return partitionIds.isEmpty()
					? null
					: partitionIds;
			});
		}
	}

	Collection<ResultPartitionID> stopTrackingPartitions(JobID jobId) {
		Preconditions.checkNotNull(jobId);

		synchronized (lock) {
			Set<ResultPartitionID> storedPartitions = trackedPartitionsPerJob.remove(jobId);
			return storedPartitions == null
				? Collections.emptyList()
				: storedPartitions;
		}
	}

	/**
	 * Stops the tracking of the given set of partitions for the given job.
	 *
	 * @param jobId
	 * @param partitionIds
	 */
	void stopTrackingPartitions(JobID jobId, Collection<ResultPartitionID> partitionIds) {
		Preconditions.checkNotNull(jobId);
		Preconditions.checkNotNull(partitionIds);

		synchronized (lock) {
			// If the JobID is unknown we do not fail here, in line with ShuffleEnvironment#releaseFinishedPartitions
			trackedPartitionsPerJob.computeIfPresent(
				jobId,
				(key, resultPartitionIDS) -> {
					resultPartitionIDS.removeAll(partitionIds);
					return resultPartitionIDS.isEmpty()
						? null
						: resultPartitionIDS;
				});
		}
	}
}
