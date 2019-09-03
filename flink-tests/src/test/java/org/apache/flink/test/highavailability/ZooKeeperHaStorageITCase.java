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

package org.apache.flink.test.highavailability;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.FileSystemBlobStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for zookeeper high availability storage.
 */
public class ZooKeeperHaStorageITCase extends TestLogger {

	private static final int NUM_JMS = 1;
	private static final int NUM_TMS = 1;
	private static final int NUM_SLOTS_PER_TM = 1;

	@ClassRule
	public static final TemporaryFolder TEMP_DIR = new TemporaryFolder();

	private static MiniDFSCluster hdfsCluster;
	private static Path hdfsRootPath;

	private static String haStorageDir;
	private static String clusterId = UUID.randomUUID().toString();

	private static TestingServer zkServer;

	private static MiniClusterWithClientResource miniClusterResource;

	private static OneShotLatch waitForCheckpointLatch = new OneShotLatch();

	@BeforeClass
	public static void setup() throws Exception {
		// Start zookeeper server
		zkServer = new TestingServer();

		// Start mini hdfs cluster
		final File tempDir = TEMP_DIR.newFolder();
		org.apache.hadoop.conf.Configuration hdConf = new org.apache.hadoop.conf.Configuration();
		hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
		hdfsCluster = builder.build();
		hdfsRootPath = new Path(hdfsCluster.getURI());
		haStorageDir = hdfsRootPath.toString() + "/flink-ha";

		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, NUM_JMS);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);

		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haStorageDir);
		config.setString(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
		config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
		config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

		miniClusterResource = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(config)
				.setNumberTaskManagers(NUM_TMS)
				.setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
				.build());

		miniClusterResource.before();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (miniClusterResource.getClusterClient()  != null) {
			miniClusterResource.after();
		}

		if (hdfsCluster != null) {
			hdfsCluster.shutdown();
		}
		hdfsCluster = null;
		hdfsRootPath = null;

		zkServer.stop();
		zkServer.close();
	}

	@Test(timeout = 120_000L)
	public void testHighAvailabilityDirectory() throws Exception {
		waitForCheckpointLatch = new OneShotLatch();

		JobGraph jobGraph = createJobGraph();
		ClusterClient<?> clusterClient = miniClusterResource.getClusterClient();
		clusterClient.setDetached(true);
		clusterClient.submitJob(jobGraph, ZooKeeperHaStorageITCase.class.getClassLoader());

		// wait until we did some checkpoints
		waitForCheckpointLatch.await();

		final FileSystem fileSystem = hdfsRootPath.getFileSystem();
		// Only cluster id path exists
		FileStatus[] fileStatuses = fileSystem.listStatus(new Path(haStorageDir));
		Assert.assertEquals(1, fileStatuses.length);

		// Check the blob,submittedJobGraph,checkpoint path should exist under cluster directory.
		Path clusterPath = new Path(haStorageDir + "/" + clusterId);
		Assert.assertTrue(fileSystem.exists(clusterPath));
		int blobNum = 0, submittedJobGraphNum = 0, numCheckpoints = 0;
		fileStatuses = fileSystem.listStatus(clusterPath);
		for (FileStatus fileStatus : fileStatuses) {
			if (fileStatus.getPath().getName().startsWith(FileSystemBlobStore.BLOB_PATH_NAME)) {
				blobNum++;
			} else if (fileStatus.getPath().getName().startsWith(ZooKeeperUtils.HA_STORAGE_SUBMITTED_JOBGRAPH_PREFIX)) {
				submittedJobGraphNum++;
			} else if (fileStatus.getPath().getName().startsWith(ZooKeeperUtils.HA_STORAGE_COMPLETED_CHECKPOINT)) {
				numCheckpoints++;
			}
		}
		assertEquals(1, blobNum);
		assertEquals(1, submittedJobGraphNum);
		assertTrue(numCheckpoints > 0);

		// Stop the flink minicluster
		miniClusterResource.after();
	}

	private static JobGraph createJobGraph() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0));
		env.enableCheckpointing(10);
		env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

		File checkpointLocation = TEMP_DIR.newFolder();
		env.setStateBackend((StateBackend) new FsStateBackend(checkpointLocation.toURI()));

		DataStreamSource<String> source = env.addSource(new UnboundedSource());

		source
			.keyBy((str) -> str)
			.map(new TestFunction());

		return env.getStreamGraph().getJobGraph();
	}

	private static class UnboundedSource implements SourceFunction<String> {
		private static final long serialVersionUID = 1L;
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (running) {
				ctx.collect("hello");
				// don't overdo it ... ;-)
				Thread.sleep(50);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class TestFunction extends RichMapFunction<String, String> implements CheckpointedFunction{

		private static final long serialVersionUID = 1L;
		// also have some state to write to the checkpoint
		private final ValueStateDescriptor<String> stateDescriptor =
			new ValueStateDescriptor<>("state", StringSerializer.INSTANCE);

		@Override
		public String map(String value) throws Exception {
			getRuntimeContext().getState(stateDescriptor).update("42");
			return value;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			if (context.getCheckpointId() > 5) {
				waitForCheckpointLatch.trigger();
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) {

		}
	}
}
