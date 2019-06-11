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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineTaskNotReadyException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test stream task declien checkpoint.
 */
public class StreamTaskDeclineCheckpointTest {

	/**
	 * This test checks that tasks decline the checkpoint, if a "trigger checkpoint" message
	 * comes before they are ready.
	 */
	@Test
	public void testDeclineCheckpointWhenNotReady() throws Exception {
		StreamTaskTestHarness<String> testHarness = new StreamTaskTestHarness<>(
				InitBlockingTask::new, BasicTypeInfo.STRING_TYPE_INFO);
		testHarness.setupOutputForSingletonOperatorChain();

		// start the test - this cannot succeed across the 'init()' method
		final DeclineDummyEnvironment env = new DeclineDummyEnvironment(
			testHarness.jobConfig,
			testHarness.taskConfig,
			testHarness.memorySize,
			new MockInputSplitProvider(),
			testHarness.bufferSize,
			new TestTaskStateManager());
		testHarness.invoke(env);

		StreamTask<String, ?> task = testHarness.getTask();

		// tell the task to commence a checkpoint
		boolean result = task.triggerCheckpoint(new CheckpointMetaData(41L, System.currentTimeMillis()),
			CheckpointOptions.forCheckpointWithDefaultLocation(), false);
		assertFalse("task triggered checkpoint though not ready", result);

		// a cancellation barrier should be downstream
		Object emitted = testHarness.getOutput().poll();
		assertNull("nothing should be emitted", emitted);
		assertEquals(41L, env.getLastDeclinedCheckpointId());
		assertTrue(env.getLastDeclinedCheckpointCause() instanceof CheckpointDeclineTaskNotReadyException);
	}

	static final class DeclineDummyEnvironment extends StreamMockEnvironment {

		private long lastDeclinedCheckpointId;
		private Throwable lastDeclinedCheckpointCause;

		DeclineDummyEnvironment(
				Configuration jobConfig,
				Configuration taskConfig,
				long memorySize,
				MockInputSplitProvider inputSplitProvider,
				int bufferSize,
				TaskStateManager taskStateManager) {
			super(jobConfig, taskConfig, memorySize, inputSplitProvider, bufferSize, taskStateManager);
			this.lastDeclinedCheckpointId = Long.MIN_VALUE;
			this.lastDeclinedCheckpointCause = null;
		}

		@Override
		public void declineCheckpoint(long checkpointId, Throwable cause) {
			this.lastDeclinedCheckpointId = checkpointId;
			this.lastDeclinedCheckpointCause = cause;
		}

		long getLastDeclinedCheckpointId() {
			return lastDeclinedCheckpointId;
		}

		Throwable getLastDeclinedCheckpointCause() {
			return lastDeclinedCheckpointCause;
		}
	}

	// ------------------------------------------------------------------------
	//  test tasks / functions
	// ------------------------------------------------------------------------

	private static class InitBlockingTask extends StreamTask<String, AbstractStreamOperator<String>> {

		private final Object lock = new Object();
		private volatile boolean running = true;

		protected InitBlockingTask(Environment env) {
			super(env);
		}

		@Override
		protected void init() throws Exception {
			synchronized (lock) {
				while (running) {
					lock.wait();
				}
			}
		}

		@Override
		protected void performDefaultAction(ActionContext context) throws Exception {
			context.allActionsCompleted();
		}

		@Override
		protected void cleanup() throws Exception {}

		@Override
		protected void cancelTask() throws Exception {
			running = false;
			synchronized (lock) {
				lock.notifyAll();
			}
		}
	}
}
