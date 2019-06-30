/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.rpc.MainThreadValidatorUtil;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Adapts {@link ScheduledExecutor} to {@link ComponentMainThreadExecutor}.
 */
public class ScheduledExecutorToComponentMainThreadExecutorAdapter implements ComponentMainThreadExecutor {

	private final ScheduledExecutor scheduledExecutor;

	private final Thread mainThread;

	public ScheduledExecutorToComponentMainThreadExecutorAdapter(
			final ScheduledExecutor scheduledExecutor,
			final Thread mainThread) {
		this.scheduledExecutor = checkNotNull(scheduledExecutor);
		this.mainThread = checkNotNull(mainThread);
	}

	@Override
	public void assertRunningInMainThread() {
		assert MainThreadValidatorUtil.isRunningInExpectedThread(mainThread);
	}

	@Override
	public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
		return scheduledExecutor.schedule(command, delay, unit);
	}

	@Override
	public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay, final TimeUnit unit) {
		return scheduledExecutor.schedule(callable, delay, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, final long initialDelay, final long period, final TimeUnit unit) {
		return scheduledExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, final long initialDelay, final long delay, final TimeUnit unit) {
		return scheduledExecutor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
	}

	@Override
	public void execute(final Runnable command) {
		scheduledExecutor.execute(command);
	}
}
