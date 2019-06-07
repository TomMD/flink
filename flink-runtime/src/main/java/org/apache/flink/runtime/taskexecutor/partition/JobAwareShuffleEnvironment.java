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

package org.apache.flink.runtime.taskexecutor.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

import java.io.IOException;
import java.util.Collection;

/**
 * Job-aware variation of the {@link ShuffleEnvironment} interface.
 *
 * @see ShuffleEnvironment
 */
public interface JobAwareShuffleEnvironment<P extends ResultPartitionWriter, G extends InputGate> extends AutoCloseable {

	/**
	 * Start the internal related services upon {@link TaskExecutor}'s startup.
	 *
	 * @return a port to connect to the task executor for shuffle data exchange, -1 if only local connection is possible.
	 */
	int start() throws IOException;

	/**
	 * Factory method for the task's {@link ResultPartitionWriter}s.
	 *
	 * @parem jobId the job id, used for tracking which job the created partitions belongs to
	 * @param taskName the task name, used for logs
	 * @param executionAttemptID execution attempt id of the task
	 * @param resultPartitionDeploymentDescriptors descriptors of the partition, produced by the task
	 * @param outputGroup shuffle specific group for output metrics
	 * @param buffersGroup shuffle specific group for buffer metrics
	 * @return collection of the task's {@link ResultPartitionWriter}s
	 */
	Collection<P> createResultPartitionWriters(
		JobID jobId,
		String taskName,
		ExecutionAttemptID executionAttemptID,
		Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		MetricGroup outputGroup,
		MetricGroup buffersGroup);

	/**
	 * Release local resources occupied with the given partitions.
	 *
	 * @param partitionIds partition ids to release
	 */
	void releaseFinishedPartitions(JobID jobId, Collection<ResultPartitionID> partitionIds);

	/**
	 * Release all local resources occupied with partitions belonging to the given job.
	 *
	 * @param jobId
	 */
	void releaseAllFinishedPartitionsForJobAndMarkJobInactive(JobID jobId);

	/**
	 * Report partitions which still occupy some resources locally.
	 *
	 * @return collection of partitions which still occupy some resources locally
	 * and have not been released yet.
	 */
	Collection<ResultPartitionID> getPartitionsOccupyingLocalResources();

	/**
	 * Returns whether there are any unreleased partitions for a given job.
	 *
	 * @param jobId
	 * @return whether there are any unreleased partitions for the given job
	 */
	boolean hasPartitionsOccupyingLocalResources(JobID jobId);

	/**
	 * Marks the given job as active. If a partition is finished while the job is no longer active it will
	 * be immediately released.
	 *
	 * @param jobId
	 */
	void markJobActive(JobID jobId);

	/**
	 * Factory method for the task's {@link InputGate}s.
	 *
	 * @param taskName the task name, used for logs
	 * @param executionAttemptID execution attempt id of the task
	 * @param partitionProducerStateProvider producer state provider to query whether the producer is ready for consumption
	 * @param inputGateDeploymentDescriptors descriptors of the input gates, consumed by the task
	 * @param parentGroup parent of shuffle specific metric group
	 * @param inputGroup shuffle specific group for input metrics
	 * @param buffersGroup shuffle specific group for buffer metrics
	 * @return collection of the task's {@link InputGate}s
	 */
	Collection<G> createInputGates(
		String taskName,
		ExecutionAttemptID executionAttemptID,
		PartitionProducerStateProvider partitionProducerStateProvider,
		Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
		MetricGroup parentGroup,
		MetricGroup inputGroup,
		MetricGroup buffersGroup);

	/**
	 * Update a gate with the newly available partition information, previously unknown.
	 *
	 * @param consumerID execution id to distinguish gates with the same id from the different consumer executions
	 * @param partitionInfo information needed to consume the updated partition, e.g. network location
	 * @return {@code true} if the partition has been updated or {@code false} if the partition is not available anymore.
	 * @throws IOException IO problem by the update
	 * @throws InterruptedException potentially blocking operation was interrupted
	 */
	boolean updatePartitionInfo(
		ExecutionAttemptID consumerID,
		PartitionInfo partitionInfo) throws IOException, InterruptedException;
}
