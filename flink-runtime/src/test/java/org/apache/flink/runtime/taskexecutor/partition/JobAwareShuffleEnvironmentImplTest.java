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
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertFalse;

/**
 * Tests for the {@link JobAwareShuffleEnvironmentImpl}.
 */
public class JobAwareShuffleEnvironmentImplTest extends TestLogger {

	private JobID jobId = new JobID();
	private TestResultPartitionWriter externallyManagedWriter;
	private TestResultPartitionWriter internallyManagedWriter;
	private TestShuffleEnvironment wrappedEnvironment;
	private JobAwareShuffleEnvironmentImpl<?> jobAwareEnvironment;
	private Collection<ResultPartitionWriter> writers;

	@Before
	public void setup() {
		final ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();
		externallyManagedWriter = new TestResultPartitionWriter(
			new ResultPartitionID(new IntermediateResultPartitionID(), executionAttemptId));
		internallyManagedWriter = new TestResultPartitionWriter(
			new ResultPartitionID(new IntermediateResultPartitionID(), executionAttemptId));

		wrappedEnvironment = new TestShuffleEnvironment(
			Arrays.asList(externallyManagedWriter, internallyManagedWriter));

		jobAwareEnvironment = new JobAwareShuffleEnvironmentImpl<>(wrappedEnvironment);

		ResultPartitionDeploymentDescriptor deploymentDescriptor1 = createResultPartitionDeploymentDescriptor(
			externallyManagedWriter.resultPartitionID, true);
		ResultPartitionDeploymentDescriptor deploymentDescriptor2 = createResultPartitionDeploymentDescriptor(
			internallyManagedWriter.resultPartitionID, false);

		writers = createResultPartitionWriters(
			jobAwareEnvironment,
			jobId,
			executionAttemptId,
			deploymentDescriptor1,
			deploymentDescriptor2
		);
	}

	@Test
	public void testInitialState() {
		final JobID jobId = new JobID();
		final TestResultPartitionWriter partitionWriter1 = new TestResultPartitionWriter(new ResultPartitionID());
		final TestResultPartitionWriter partitionWriter2 = new TestResultPartitionWriter(new ResultPartitionID());

		final TestShuffleEnvironment wrappedEnvironment = new TestShuffleEnvironment(
			Arrays.asList(partitionWriter1, partitionWriter2));

		final JobAwareShuffleEnvironmentImpl jobAwareEnvironment = new JobAwareShuffleEnvironmentImpl(wrappedEnvironment);

		assertFalse(jobAwareEnvironment.hasPartitionsOccupyingLocalResources(jobId));
	}

	@Test
	public void testReleaseAllPartitions() throws IOException {
		jobAwareEnvironment.markJobActive(jobId);

		for (ResultPartitionWriter writer : writers) {
			writer.setup();
		}

		for (ResultPartitionWriter writer : writers) {
			writer.finish();
		}

		assertTrue(jobAwareEnvironment.hasPartitionsOccupyingLocalResources(jobId));

		jobAwareEnvironment.releaseAllFinishedPartitionsForJobAndMarkJobInactive(jobId);
		assertThat(wrappedEnvironment.releasedPartitions, contains(externallyManagedWriter.resultPartitionID));
	}

	@Test
	public void testReleasePartitions() throws IOException {
		jobAwareEnvironment.markJobActive(jobId);

		for (ResultPartitionWriter writer : writers) {
			writer.setup();
		}

		for (ResultPartitionWriter writer : writers) {
			writer.finish();
		}

		assertTrue(jobAwareEnvironment.hasPartitionsOccupyingLocalResources(jobId));

		jobAwareEnvironment.releaseFinishedPartitions(jobId, Collections.singleton(externallyManagedWriter.resultPartitionID));
		assertThat(wrappedEnvironment.releasedPartitions, contains(externallyManagedWriter.resultPartitionID));
	}

	@Test
	public void testPartitionFailure() throws IOException {
		jobAwareEnvironment.markJobActive(jobId);

		for (ResultPartitionWriter writer : writers) {
			writer.setup();
		}

		for (ResultPartitionWriter writer : writers) {
			writer.fail(null);
		}

		assertFalse(jobAwareEnvironment.hasPartitionsOccupyingLocalResources(jobId));

		jobAwareEnvironment.releaseAllFinishedPartitionsForJobAndMarkJobInactive(jobId);
		assertThat(wrappedEnvironment.releasedPartitions, empty());
	}

	@Test
	public void testPartitionFinishWithoutActiveJob() throws IOException {
		for (ResultPartitionWriter writer : writers) {
			writer.setup();
		}

		for (ResultPartitionWriter writer : writers) {
			writer.finish();
		}

		assertFalse(jobAwareEnvironment.hasPartitionsOccupyingLocalResources(jobId));
		assertThat(wrappedEnvironment.releasedPartitions, contains(externallyManagedWriter.resultPartitionID));
	}

	@Test
	public void testPartitionFinishWithActiveJob() throws IOException {
		jobAwareEnvironment.markJobActive(jobId);

		for (ResultPartitionWriter writer : writers) {
			writer.setup();
		}

		for (ResultPartitionWriter writer : writers) {
			writer.finish();
		}

		assertTrue(jobAwareEnvironment.hasPartitionsOccupyingLocalResources(jobId));
		assertThat(wrappedEnvironment.releasedPartitions, empty());
	}

	@Test
	public void testPartitionFinishWithPreviouslyActiveJob() throws IOException {
		jobAwareEnvironment.markJobActive(jobId);

		jobAwareEnvironment.releaseAllFinishedPartitionsForJobAndMarkJobInactive(jobId);

		for (ResultPartitionWriter writer : writers) {
			writer.setup();
		}

		for (ResultPartitionWriter writer : writers) {
			writer.finish();
		}

		assertFalse(jobAwareEnvironment.hasPartitionsOccupyingLocalResources(jobId));
		assertThat(wrappedEnvironment.releasedPartitions, contains(externallyManagedWriter.resultPartitionID));
	}

	@Test
	public void testListenerCalledOnFinishedPartition() throws IOException {
		AtomicBoolean listenerCalled = new AtomicBoolean();

		jobAwareEnvironment.setPartitionFailedOrFinishedListener(jobId -> listenerCalled.set(true));

		for (ResultPartitionWriter writer : writers) {
			writer.setup();
		}

		assertFalse(listenerCalled.get());

		for (ResultPartitionWriter writer : writers) {
			writer.finish();
		}

		assertTrue(listenerCalled.get());
	}

	@Test
	public void testListenerCalledOnFailedPartition() throws IOException {
		AtomicBoolean listenerCalled = new AtomicBoolean();

		jobAwareEnvironment.setPartitionFailedOrFinishedListener(jobId -> listenerCalled.set(true));

		for (ResultPartitionWriter writer : writers) {
			writer.setup();
		}

		assertFalse(listenerCalled.get());

		for (ResultPartitionWriter writer : writers) {
			writer.fail(null);
		}

		assertTrue(listenerCalled.get());
	}

	private static Collection<ResultPartitionWriter> createResultPartitionWriters(
		JobAwareShuffleEnvironment jobAwareShuffleEnvironment,
		JobID jobId,
		ExecutionAttemptID executionAttemptId,
		ResultPartitionDeploymentDescriptor ... resultPartitionDeploymentDescriptors) {

		return jobAwareShuffleEnvironment.createResultPartitionWriters(
			jobId,
			"test",
			executionAttemptId,
			Arrays.asList(resultPartitionDeploymentDescriptors),
			new UnregisteredMetricsGroup(),
			new UnregisteredMetricsGroup()
		);
	}

	private static ResultPartitionDeploymentDescriptor createResultPartitionDeploymentDescriptor(ResultPartitionID resultPartitionId, boolean externallyManaged) {
		return new ResultPartitionDeploymentDescriptor(
			new PartitionDescriptor(
				new IntermediateDataSetID(),
				resultPartitionId.getPartitionId(),
				externallyManaged
					? ResultPartitionType.BLOCKING
					: ResultPartitionType.PIPELINED,
				1,
				0),
			(ShuffleDescriptor) () -> resultPartitionId,
			1,
			true);
	}

	private static class TestResultPartitionWriter implements ResultPartitionWriter {

		private final ResultPartitionID resultPartitionID;

		private TestResultPartitionWriter(ResultPartitionID resultPartitionID) {
			this.resultPartitionID = resultPartitionID;
		}

		@Override
		public void setup() throws IOException {

		}

		@Override
		public ResultPartitionID getPartitionId() {
			return resultPartitionID;
		}

		@Override
		public int getNumberOfSubpartitions() {
			return 0;
		}

		@Override
		public int getNumTargetKeyGroups() {
			return 0;
		}

		@Override
		public BufferBuilder getBufferBuilder() throws IOException, InterruptedException {
			return null;
		}

		@Override
		public boolean addBufferConsumer(BufferConsumer bufferConsumer, int subpartitionIndex) throws IOException {
			return false;
		}

		@Override
		public void flushAll() {

		}

		@Override
		public void flush(int subpartitionIndex) {

		}

		@Override
		public void fail(@Nullable Throwable throwable) {

		}

		@Override
		public void finish() throws IOException {

		}

		@Override
		public void close() throws Exception {

		}
	}

	private static class TestShuffleEnvironment implements ShuffleEnvironment<ResultPartitionWriter, InputGate> {

		private final Collection<ResultPartitionWriter> resultPartitionWriters;
		private final Collection<ResultPartitionID> releasedPartitions = new HashSet<>();

		private TestShuffleEnvironment(Collection<ResultPartitionWriter> writersToReturn) {
			this.resultPartitionWriters = writersToReturn;
		}

		Collection<ResultPartitionID> getReleasedPartitions() {
			return releasedPartitions;
		}

		@Override
		public int start() throws IOException {
			return 1234;
		}

		@Override
		public Collection<ResultPartitionWriter> createResultPartitionWriters(String taskName, ExecutionAttemptID executionAttemptID, Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors, MetricGroup outputGroup, MetricGroup buffersGroup) {
			return resultPartitionWriters;
		}

		@Override
		public void releasePartitions(Collection<ResultPartitionID> partitionIds) {
			releasedPartitions.addAll(partitionIds);
		}

		@Override
		public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
			return null;
		}

		@Override
		public Collection<InputGate> createInputGates(String taskName, ExecutionAttemptID executionAttemptID, PartitionProducerStateProvider partitionProducerStateProvider, Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors, MetricGroup parentGroup, MetricGroup inputGroup, MetricGroup buffersGroup) {
			return Collections.emptyList();
		}

		@Override
		public boolean updatePartitionInfo(ExecutionAttemptID consumerID, PartitionInfo partitionInfo) throws IOException, InterruptedException {
			return false;
		}

		@Override
		public void close() throws Exception {

		}
	}
}
