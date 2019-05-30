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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

import java.io.IOException;
import java.util.Collection;

/**
 * Interface for the implementation of shuffle service environment locally on task executor.
 *
 * <p>Input/Output interface of local shuffle service environment is based on memory {@link Buffer}s.
 * The task can write shuffle data into the buffers,
 * obtained from the created here {@link ResultPartitionWriter}s
 * and read the buffers from the created here {@link InputGate}s.
 *
 * <h2>Lifecycle management.</h2>
 *
 * <p>The interface contains method's to manage the lifecycle of the local shuffle service environment:
 * <ol>
 *     <li>{@code start} is called when the {@link TaskExecutor} is being started.</li>
 *     <li>{@code shutdown} is called when the {@link TaskExecutor} is being stopped.</li>
 * </ol>
 *
 * <h2>Shuffle Input/Output management.</h2>
 *
 * <h3>Result partition management.</h3>
 *
 * <p>The interface implements a factory of result partition writers for the task output: {@code createResultPartitionWriters}.
 * The created writers are grouped per task and handed over to the task thread upon its startup.
 * The task is responsible for the writers' lifecycle from that moment.
 *
 * <p>Partitions are released in the following cases:
 * <ol>
 *     <li>{@link ResultPartitionWriter#fail(Throwable)} and {@link ResultPartitionWriter#close()} are called
 *     if the production has failed.</li>
 *     <li>{@link ResultPartitionWriter#finish()} and {@link ResultPartitionWriter#close()} are called
 *     if the production is done. The actual release can take some time
 *     if 'the end of consumption' confirmation is being awaited implicitly
 *     or the partition is later released by {@code releasePartitions(Collection<ResultPartitionID>)}.</li>
 *     <li>{@code releasePartitions(Collection<ResultPartitionID>)} is called outside of the task thread,
 *     e.g. to manage the local resource lifecycle of external partitions which outlive the task production.</li>
 * </ol>
 * The partitions, which currently still occupy local resources, can be queried with {@code getUnreleasedPartitions}.
 *
 * <h3>Input gate management.</h3>
 *
 * <p>The interface implements a factory for the task input gates: {@code createInputGates}.
 * The created gates are grouped per task and handed over to the task thread upon its startup.
 * The task is responsible for the gates' lifecycle from that moment.
 *
 * <p>When tha task is deployed and the input gates are created, it can happen that not all consumed partitions
 * are known at that moment e.g. because their producers have not been started yet.
 * Therefore, the {@link ShuffleEnvironment} provides a method {@code updatePartitionInfo} to update them
 * externally, ouside of the task thread, when the producer becomes known.
 */
public interface ShuffleEnvironment {

	/**
	 * Start the internal related services upon {@link TaskExecutor}'s startup.
	 *
	 * @return a port to connect to the task executor for shuffle data exchange, -1 if only local connection is possible.
	 */
	int start() throws IOException;

	/**
	 * Shutdown the internal related services upon {@link TaskExecutor}'s stop.
	 */
	void shutdown();

	/**
	 * Factory method for the task's {@link ResultPartitionWriter}s.
	 *
	 * @param taskName the task name, used for logs
	 * @param executionAttemptID execution attempt id of the task
	 * @param resultPartitionDeploymentDescriptors descriptors of the partition, produced by the task
	 * @param outputGroup shuffle specific group for output metrics
	 * @param buffersGroup shuffle specific group for buffer metrics
	 * @return array of the task's {@link ResultPartitionWriter}s
	 */
	ResultPartitionWriter[] createResultPartitionWriters(
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
	void releasePartitions(Collection<ResultPartitionID> partitionIds);

	/**
	 * Report unreleased partitions.
	 *
	 * @return collection of partitions which still occupy some resources locally on this task executor
	 * and have been not released yet.
	 */
	Collection<ResultPartitionID> getUnreleasedPartitions();

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
	 * @return array of the task's {@link InputGate}s
	 */
	InputGate[] createInputGates(
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
