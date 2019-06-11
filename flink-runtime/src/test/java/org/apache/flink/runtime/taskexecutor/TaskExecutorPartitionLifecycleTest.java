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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.partition.JobAwareShuffleEnvironment;
import org.apache.flink.runtime.taskexecutor.partition.JobAwareShuffleEnvironmentImpl;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.NoOpTaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.TriConsumer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the partition-lifecycle logic in the {@link TaskExecutor}.
 */
public class TaskExecutorPartitionLifecycleTest extends TestLogger {

	private static final Time timeout = Time.seconds(10L);

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private final JobID jobId = new JobID();
	private TestingHighAvailabilityServices haServices;
	private SettableLeaderRetrievalService jobManagerLeaderRetriever;
	private TestingRpcService rpc;

	@Before
	public void setup() {
		haServices = new TestingHighAvailabilityServices();
		jobManagerLeaderRetriever = new SettableLeaderRetrievalService();
		haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
		haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetriever);
		rpc = new TestingRpcService();
	}

	@Test
	public void testConnectionTerminationAfterExternalRelease() throws IOException, InterruptedException, ExecutionException, TimeoutException {
		final JobMasterId jobMasterId = JobMasterId.generate();

		final LibraryCacheManager libraryCacheManager = mock(LibraryCacheManager.class);
		when(libraryCacheManager.getClassLoader(any(JobID.class))).thenReturn(ClassLoader.getSystemClassLoader());

		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);
		when(jobMasterGateway.getFencingToken()).thenReturn(jobMasterId);

		final TaskManagerActions taskManagerActions = new NoOpTaskManagerActions();
		final JobManagerConnection jobManagerConnection = new JobManagerConnection(
			jobId,
			ResourceID.generate(),
			jobMasterGateway,
			taskManagerActions,
			mock(CheckpointResponder.class),
			new TestGlobalAggregateManager(),
			libraryCacheManager,
			new NoOpResultPartitionConsumableNotifier(),
			mock(PartitionProducerStateChecker.class));

		final JobManagerTable jobManagerTable = new JobManagerTable();
		jobManagerTable.put(jobId, jobManagerConnection);

		final AtomicBoolean hasPartitionsOccupyingLocalResources = new AtomicBoolean(true);
		final TestJobAwareShuffleEnvironment jobAwareShuffleEnvironment = new TestJobAwareShuffleEnvironment(jobId -> hasPartitionsOccupyingLocalResources.get());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setJobManagerTable(jobManagerTable)
			.setShuffleEnvironment(jobAwareShuffleEnvironment)
			.build();

		final TestingTaskExecutor taskManager = createTestingTaskExecutor(taskManagerServices, new HeartbeatServices(Long.MAX_VALUE, Long.MAX_VALUE));

		try {
			taskManager.start();
			taskManager.waitUntilStarted();

			assertTrue(taskManagerServices.getJobManagerTable().contains(jobId));

			taskManager.releasePartitions(jobId, Collections.singletonList(new ResultPartitionID()));
			// connection should be kept alive since the environment still says we have local resources
			assertTrue(taskManagerServices.getJobManagerTable().contains(jobId));

			hasPartitionsOccupyingLocalResources.set(false);

			taskManager.releasePartitions(jobId, Collections.singletonList(new ResultPartitionID()));
			assertTrue(taskManagerServices.getJobManagerTable().contains(jobId));
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}
	}

	@Test
	public void testConnectionTerminationAfterInternalRelease() throws Exception {
		final JobMasterId jobMasterId = JobMasterId.generate();

		final LibraryCacheManager libraryCacheManager = mock(LibraryCacheManager.class);
		when(libraryCacheManager.getClassLoader(any(JobID.class))).thenReturn(ClassLoader.getSystemClassLoader());

		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);
		when(jobMasterGateway.getFencingToken()).thenReturn(jobMasterId);

		final TaskManagerActions taskManagerActions = new NoOpTaskManagerActions();
		final JobManagerConnection jobManagerConnection = new JobManagerConnection(
			jobId,
			ResourceID.generate(),
			jobMasterGateway,
			taskManagerActions,
			mock(CheckpointResponder.class),
			new TestGlobalAggregateManager(),
			libraryCacheManager,
			new NoOpResultPartitionConsumableNotifier(),
			mock(PartitionProducerStateChecker.class));

		final JobManagerTable jobManagerTable = new JobManagerTable();
		jobManagerTable.put(jobId, jobManagerConnection);

		final AtomicBoolean hasPartitionsOccupyingLocalResources = new AtomicBoolean(true);
		final TestJobAwareShuffleEnvironment jobAwareShuffleEnvironment = new TestJobAwareShuffleEnvironment(jobId -> hasPartitionsOccupyingLocalResources.get());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setJobManagerTable(jobManagerTable)
			.setShuffleEnvironment(jobAwareShuffleEnvironment)
			.build();

		final TestingTaskExecutor taskManager = createTestingTaskExecutor(taskManagerServices, new HeartbeatServices(Long.MAX_VALUE, Long.MAX_VALUE));

		try {
			taskManager.start();
			taskManager.waitUntilStarted();

			assertTrue(taskManagerServices.getJobManagerTable().contains(jobId));
			assertNotNull(jobAwareShuffleEnvironment.listener);

			jobAwareShuffleEnvironment.listener.accept(jobId);
			// connection should be kept alive since the environment still says we have local resources
			assertTrue(taskManagerServices.getJobManagerTable().contains(jobId));

			hasPartitionsOccupyingLocalResources.set(false);

			jobAwareShuffleEnvironment.listener.accept(jobId);
			assertTrue(taskManagerServices.getJobManagerTable().contains(jobId));
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}
	}

	@Test
	public void testPartitionReleaseAfterDisconnect() throws Exception {
		testPartitionRelease(
			(jobId, partitionId, taskExecutorGateway) -> taskExecutorGateway.disconnectJobManager(jobId, new Exception("test")),
			(jobId, jobAwareShuffleEnvironment) -> assertFalse(jobAwareShuffleEnvironment.hasPartitionsOccupyingLocalResources(jobId)));
	}

	@Test
	public void testPartitionReleaseAfterReleaseCall() throws Exception {
		testPartitionRelease(
			(jobId, partitionId, taskExecutorGateway) -> taskExecutorGateway.releasePartitions(jobId, Collections.singletonList(partitionId)),
			(jobId, jobAwareShuffleEnvironment) -> assertFalse(jobAwareShuffleEnvironment.hasPartitionsOccupyingLocalResources(jobId)));
	}

	@Test
	public void testPartitionReleaseAfterShutdown() throws Exception {
		// don't do any explicit release action, so that the partition must be cleaned up on shutdown
		testPartitionRelease(
			(jobId, partitionId, taskExecutorGateway) -> { },
			(jobId, jobAwareShuffleEnvironment) -> { });
	}

	private void testPartitionRelease(
		TriConsumer<JobID, ResultPartitionID, TaskExecutorGateway> releaseAction,
		BiConsumer<JobID, JobAwareShuffleEnvironmentImpl> postReleaseActionAssertion) throws Exception {

		final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();

		final TaskSlotTable taskSlotTable = new TaskSlotTable(
			Collections.singletonList(ResourceProfile.UNKNOWN),
			new TimerService<>(TestingUtils.defaultExecutor(), timeout.toMilliseconds()));

		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation, RetryingRegistrationConfiguration.defaultConfiguration());

		final TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final JobAwareShuffleEnvironmentImpl<?> jobAwareShuffleEnvironment = new JobAwareShuffleEnvironmentImpl<>(new NettyShuffleEnvironmentBuilder().build());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setJobLeaderService(jobLeaderService)
			.setTaskStateManager(localStateStoresManager)
			.setShuffleEnvironment(jobAwareShuffleEnvironment)
			.build();

		final CompletableFuture<Void> taskFinishedFuture = new CompletableFuture<>();

		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setRegisterTaskManagerFunction((s, location) -> CompletableFuture.completedFuture(new JMTMRegistrationSuccess(ResourceID.generate())))
			.setOfferSlotsFunction((resourceID, slotOffers) -> CompletableFuture.completedFuture(slotOffers))
			.setUpdateTaskExecutionStateFunction(taskExecutionState -> {
				if (taskExecutionState.getExecutionState() == ExecutionState.FINISHED) {
					taskFinishedFuture.complete(null);
				}
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.build();

		final TestingTaskExecutor taskManager = createTestingTaskExecutor(taskManagerServices, new HeartbeatServices(Long.MAX_VALUE, Long.MAX_VALUE));

		try {
			taskManager.start();
			taskManager.waitUntilStarted();

			final String jobMasterAddress = "jm";
			rpc.registerGateway(jobMasterAddress, jobMasterGateway);

			// inform the task manager about the job leader
			jobLeaderService.addJob(jobId, jobMasterAddress);
			jobManagerLeaderRetriever.notifyListener(jobMasterAddress, UUID.randomUUID());

			while (!taskManagerServices.getJobManagerTable().contains(jobId)) {
				Thread.sleep(50);
			}

			final ExecutionAttemptID eid1 = new ExecutionAttemptID();
			final IntermediateResultPartitionID partitionId = new IntermediateResultPartitionID();
			final ResultPartitionID resultPartitionId = new ResultPartitionID(partitionId, eid1);
			final ResultPartitionDeploymentDescriptor task1ResultPartitionDescriptor =
				new ResultPartitionDeploymentDescriptor(
					new PartitionDescriptor(new IntermediateDataSetID(), partitionId, ResultPartitionType.BLOCKING, 1, 0),
					new ShuffleDescriptor() {
						@Override
						public ResultPartitionID getResultPartitionID() {
							return resultPartitionId;
						}
					},
					1,
					true);

			final TaskDeploymentDescriptor taskDeploymentDescriptor =
				TaskExecutorSubmissionTest.createTaskDeploymentDescriptor(
					jobId,
					"job",
					eid1,
					new SerializedValue<>(new ExecutionConfig()),
					"Sender",
					1,
					0,
					1,
					0,
					new Configuration(),
					new Configuration(),
					TestInvokable.class.getName(),
					Collections.singletonList(task1ResultPartitionDescriptor),
					Collections.emptyList(),
					Collections.emptyList(),
					Collections.emptyList(),
					0);

			taskSlotTable.allocateSlot(0, jobId, taskDeploymentDescriptor.getAllocationId(), Time.seconds(60));

			TestInvokable.sync = new BlockerSync();

			taskManager.submitTask(taskDeploymentDescriptor, jobMasterGateway.getFencingToken(), timeout)
				.get();

			TestInvokable.sync.awaitBlocker();

			// the task is still running, so nothing was actually stored yet
			assertTrue(jobAwareShuffleEnvironment.hasPartitionsOccupyingLocalResources(jobId));

			TestInvokable.sync.releaseBlocker();
			taskFinishedFuture.get(timeout.getSize(), timeout.getUnit());

			// the task is still running, so nothing was actually stored yet
			assertTrue(jobAwareShuffleEnvironment.hasPartitionsOccupyingLocalResources(jobId));

			releaseAction.accept(jobId, new ResultPartitionID(partitionId, eid1), taskManager);
			postReleaseActionAssertion.accept(jobId, jobAwareShuffleEnvironment);
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}

		// the partition table should always be cleaned up on shutdown
		assertFalse(jobAwareShuffleEnvironment.hasPartitionsOccupyingLocalResources(jobId));
	}

	/**
	 * Test invokable which completes the given future when executed.
	 */
	public static class TestInvokable extends AbstractInvokable {

		static BlockerSync sync;

		public TestInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			sync.block();
		}
	}

	private TestingTaskExecutor createTestingTaskExecutor(TaskManagerServices taskManagerServices, HeartbeatServices heartbeatServices) throws IOException {
		return new TestingTaskExecutor(
			rpc,
			TaskManagerConfiguration.fromConfiguration(new Configuration()),
			haServices,
			taskManagerServices,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
			new BlobCacheService(
				new Configuration(),
				new VoidBlobStore(),
				null),
			new TestingFatalErrorHandler());
	}

	private static class TestJobAwareShuffleEnvironment implements JobAwareShuffleEnvironment<ResultPartitionWriter, InputGate> {

		private final Function<JobID, Boolean> hasPartitionsOccupyingLocalResourcesFunction;
		private Consumer<JobID> listener = null;

		private TestJobAwareShuffleEnvironment(Function<JobID, Boolean> hasPartitionsOccupyingLocalResourcesFunction) {
			this.hasPartitionsOccupyingLocalResourcesFunction = hasPartitionsOccupyingLocalResourcesFunction;
		}

		@Override
		public int start() throws IOException {
			return 0;
		}

		@Override
		public Collection<ResultPartitionWriter> createResultPartitionWriters(JobID jobId, String taskName, ExecutionAttemptID executionAttemptID, Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors, MetricGroup outputGroup, MetricGroup buffersGroup) {
			return null;
		}

		@Override
		public void releaseFinishedPartitions(JobID jobId, Collection<ResultPartitionID> partitionIds) {

		}

		@Override
		public void releaseAllFinishedPartitionsForJobAndMarkJobInactive(JobID jobId) {

		}

		@Override
		public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
			return null;
		}

		@Override
		public boolean hasPartitionsOccupyingLocalResources(JobID jobId) {
			return hasPartitionsOccupyingLocalResourcesFunction.apply(jobId);
		}

		@Override
		public void markJobActive(JobID jobId) {

		}

		@Override
		public void setPartitionFailedOrFinishedListener(Consumer<JobID> listener) {
			this.listener = listener;
		}

		@Override
		public Collection<InputGate> createInputGates(String taskName, ExecutionAttemptID executionAttemptID, PartitionProducerStateProvider partitionProducerStateProvider, Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors, MetricGroup parentGroup, MetricGroup inputGroup, MetricGroup buffersGroup) {
			return null;
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
