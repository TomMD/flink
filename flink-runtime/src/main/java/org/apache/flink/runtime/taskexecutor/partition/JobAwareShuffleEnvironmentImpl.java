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
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Wraps a {@link ShuffleEnvironment} to allow tracking of partitions per job.
 */
public class JobAwareShuffleEnvironmentImpl<G extends InputGate> implements JobAwareShuffleEnvironment<NotifyingResultPartitionWriter, G> {

	private static final Consumer<ResultPartitionID> NO_OP_NOTIFIER = partitionId -> {};

	private final ShuffleEnvironment<?, G> backingShuffleEnvironment;
	private final PartitionTable inProgressPartitionTable = new PartitionTable();
	private final PartitionTable finishedPartitionTable = new PartitionTable();

	private Consumer<JobID> listener = jobId -> {};

	/**	Tracks which jobs are still being monitored, to ensure cleanup in cases where tasks are finishing while
	 * the jobmanager connection is being terminated. This is a concurrent map since it is modified by both the
	 * Task (via {@link #notifyPartitionFinished(JobID, ResultPartitionID)}} and
	 * TaskExecutor (via {@link #releaseAllFinishedPartitionsForJobAndMarkJobInactive(JobID)}) thread. */
	private final Set<JobID> activeJobs = ConcurrentHashMap.newKeySet();

	public JobAwareShuffleEnvironmentImpl(ShuffleEnvironment<?, G> backingShuffleEnvironment) {
		this.backingShuffleEnvironment = Preconditions.checkNotNull(backingShuffleEnvironment);
	}

	@Override
	public void setPartitionFailedOrFinishedListener(Consumer<JobID> listener) {
		this.listener = listener;
	}

	@Override
	public boolean hasPartitionsOccupyingLocalResources(JobID jobId) {
		return inProgressPartitionTable.hasTrackedPartitions(jobId) || finishedPartitionTable.hasTrackedPartitions(jobId);
	}

	@Override
	public void markJobActive(JobID jobId) {
		activeJobs.add(jobId);
	}

	@Override
	public void releaseFinishedPartitions(JobID jobId, Collection<ResultPartitionID> resultPartitionIds) {
		finishedPartitionTable.stopTrackingPartitions(jobId, resultPartitionIds);
		backingShuffleEnvironment.releasePartitions(resultPartitionIds);
	}

	@Override
	public void releaseAllFinishedPartitionsForJobAndMarkJobInactive(JobID jobId) {
		activeJobs.remove(jobId);
		Collection<ResultPartitionID> finishedPartitionsForJob = finishedPartitionTable.stopTrackingPartitions(jobId);
		backingShuffleEnvironment.releasePartitions(finishedPartitionsForJob);
	}

	/**
	 * This method wraps partition writers for externally managed partitions and introduces callbacks into the lifecycle
	 * methods of the {@link ResultPartitionWriter}.
	 */
	@Override
	public Collection<NotifyingResultPartitionWriter> createResultPartitionWriters(
		JobID jobId,
		String taskName,
		ExecutionAttemptID executionAttemptID,
		Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		MetricGroup outputGroup,
		MetricGroup buffersGroup) {

		final Collection<? extends ResultPartitionWriter> resultPartitionWriters = backingShuffleEnvironment
			.createResultPartitionWriters(taskName, executionAttemptID, resultPartitionDeploymentDescriptors, outputGroup, buffersGroup);

		// determine which partitions are externally managed, and whose writers need to be wrapped
		final Set<ResultPartitionID> externallyManagedPartitions = resultPartitionDeploymentDescriptors.stream()
			.filter(ResultPartitionDeploymentDescriptor::isManagedExternally)
			// TODO: The descriptor should be extended to also contain the ResultPartitionID
			.map(descriptor -> new ResultPartitionID(descriptor.getPartitionId(), executionAttemptID))
			.collect(Collectors.toSet());

		// wrap partition writers to introduce callbacks for lifecycle events
		final Collection<NotifyingResultPartitionWriter> wrappedResultPartitionWriters = new ArrayList<>(resultPartitionWriters.size());
		for (ResultPartitionWriter resultPartitionWriter : resultPartitionWriters) {
			final NotifyingResultPartitionWriter wrapper;
			if (externallyManagedPartitions.contains(resultPartitionWriter.getPartitionId())) {
				wrapper = new NotifyingResultPartitionWriter(
					resultPartitionWriter,
					partitionId -> notifyPartitionSetup(jobId, partitionId),
					partitionId -> notifyPartitionFailed(jobId, partitionId),
					partitionId -> notifyPartitionFinished(jobId, partitionId));
			} else {
				wrapper = new NotifyingResultPartitionWriter(
					resultPartitionWriter,
					NO_OP_NOTIFIER,
					NO_OP_NOTIFIER,
					NO_OP_NOTIFIER);
			}
			wrappedResultPartitionWriters.add(wrapper);
		}

		return wrappedResultPartitionWriters;
	}

	private void notifyPartitionSetup(JobID jobId, ResultPartitionID resultPartitionId) {
		// if the job is marked inactive while a task is starting up it is possible that at this point we're adding
		// a partition for an inactive job. This is fine; the task will go through the cancellation logic and fail the
		// partition later.
		inProgressPartitionTable.startTrackingPartition(jobId, resultPartitionId);
	}

	private void notifyPartitionFinished(JobID jobId, ResultPartitionID resultPartitionId) {
		if (activeJobs.contains(jobId)) {
			// keep this order to ensure #hasPartitionsOccupyingLocalResources returns true at all times
			finishedPartitionTable.startTrackingPartition(jobId, resultPartitionId);
			inProgressPartitionTable.stopTrackingPartition(jobId, resultPartitionId);
		} else {
			// special case for when partitions are finished after the connection to the jobmanager has been terminated
			// this happens if a task shuts down while the connection is being terminated
			inProgressPartitionTable.stopTrackingPartition(jobId, resultPartitionId);
			backingShuffleEnvironment.releasePartitions(Collections.singleton(resultPartitionId));
		}

		if (!hasPartitionsOccupyingLocalResources(jobId)) {
			listener.accept(jobId);
		}
	}

	private void notifyPartitionFailed(JobID jobId, ResultPartitionID resultPartitionId) {
		inProgressPartitionTable.stopTrackingPartition(jobId, resultPartitionId);
		// also check for finished partitions since tasks may fail (and release) a finished partition if they are canceled
		// after they finished their partitions.
		finishedPartitionTable.stopTrackingPartition(jobId, resultPartitionId);

		if (!hasPartitionsOccupyingLocalResources(jobId)) {
			listener.accept(jobId);
		}
	}

	// ------------------------------------------------------------------------
	//  The following methods are simple delegates
	// ------------------------------------------------------------------------

	@Override
	public int start() throws IOException {
		return backingShuffleEnvironment.start();
	}

	public Collection<G> createInputGates(
		String taskName,
		ExecutionAttemptID executionAttemptID,
		PartitionProducerStateProvider partitionProducerStateProvider,
		Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
		MetricGroup parentGroup,
		MetricGroup inputGroup,
		MetricGroup buffersGroup) {

		return backingShuffleEnvironment.createInputGates(taskName, executionAttemptID, partitionProducerStateProvider, inputGateDeploymentDescriptors, parentGroup, inputGroup, buffersGroup);
	}

	@Override
	public boolean updatePartitionInfo(ExecutionAttemptID consumerID, PartitionInfo partitionInfo) throws IOException, InterruptedException {
		return backingShuffleEnvironment.updatePartitionInfo(consumerID, partitionInfo);
	}

	@Override
	public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
		return backingShuffleEnvironment.getPartitionsOccupyingLocalResources();
	}

	@Override
	public void close() throws Exception {
		backingShuffleEnvironment.close();
	}
}
