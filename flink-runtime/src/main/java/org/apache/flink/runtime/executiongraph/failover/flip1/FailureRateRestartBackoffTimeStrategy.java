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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import scala.concurrent.duration.Duration;

import java.util.ArrayDeque;

import static org.apache.flink.configuration.ConfigConstants.RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL;
import static org.apache.flink.configuration.ConfigConstants.RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL;
import static org.apache.flink.configuration.ConfigConstants.RESTART_BACKOFF_TIME_STRATEGY_RESTART_BACKOFF_TIME;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Restart strategy which can restart when failure rate is not exceeded.
 */
public class FailureRateRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {

	private final long failuresIntervalMS;

	private final long backoffTimeMS;

	private final int maxFailuresPerInterval;

	private final ArrayDeque<Long> failureTimestamps;

	private final String strategyString;

	public FailureRateRestartBackoffTimeStrategy(int maxFailuresPerInterval, long failuresInterval, long backoffTimeMS) {

		checkArgument(maxFailuresPerInterval > 0, "Maximum number of restart attempts per time unit must be greater than 0.");
		checkArgument(failuresInterval > 0, "Failures interval must be greater than 0 ms.");
		checkArgument(backoffTimeMS >= 0, "Backoff time must be at least 0 ms.");

		this.failuresIntervalMS = failuresInterval;
		this.backoffTimeMS = backoffTimeMS;
		this.maxFailuresPerInterval = maxFailuresPerInterval;
		this.failureTimestamps = new ArrayDeque<>(maxFailuresPerInterval);
		this.strategyString = generateStrategyString();
	}

	@Override
	public boolean canRestart() {
		if (isFailureTimestampsQueueFull()) {
			Long now = System.currentTimeMillis();
			Long earliestFailure = failureTimestamps.peek();

			return (now - earliestFailure) > failuresIntervalMS;
		} else {
			return true;
		}
	}

	@Override
	public long getBackoffTime() {
		return backoffTimeMS;
	}

	@Override
	public void notifyFailure(Throwable cause) {
		if (isFailureTimestampsQueueFull()) {
			failureTimestamps.remove();
		}
		failureTimestamps.add(System.currentTimeMillis());
	}

	@Override
	public String toString() {
		return strategyString;
	}

	private boolean isFailureTimestampsQueueFull() {
		return failureTimestamps.size() >= maxFailuresPerInterval;
	}

	private String generateStrategyString() {
		StringBuilder buf = new StringBuilder("FailureRateRestartBackoffTimeStrategy(");
		buf.append("FailureRateRestartBackoffTimeStrategy(failuresIntervalMS=");
		buf.append(failuresIntervalMS);
		buf.append(",backoffTimeMS=");
		buf.append(backoffTimeMS);
		buf.append(",maxFailuresPerInterval=");
		buf.append(maxFailuresPerInterval);
		buf.append(")");

		return buf.toString();
	}

	public static FailureRateRestartBackoffTimeStrategyFactory createFactory(final Configuration configuration) {
		return new FailureRateRestartBackoffTimeStrategyFactory(configuration);
	}

	/**
	 * The factory for creating {@link FailureRateRestartBackoffTimeStrategy}.
	 */
	public static class FailureRateRestartBackoffTimeStrategyFactory implements RestartBackoffTimeStrategy.Factory {

		private final int maxFailuresPerInterval;

		private final long failuresIntervalMS;

		private final long backoffTimeMS;

		public FailureRateRestartBackoffTimeStrategyFactory(final Configuration configuration) {
			this.maxFailuresPerInterval = configuration.getInteger(
				RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL);
			this.failuresIntervalMS = getIntervalMSFromConfiguration(
				configuration,
				RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL);
			this.backoffTimeMS = getIntervalMSFromConfiguration(
				configuration,
				RESTART_BACKOFF_TIME_STRATEGY_RESTART_BACKOFF_TIME);
		}

		@Override
		public RestartBackoffTimeStrategy create() {
			return new FailureRateRestartBackoffTimeStrategy(maxFailuresPerInterval, failuresIntervalMS, backoffTimeMS);
		}

		private long getIntervalMSFromConfiguration(Configuration configuration, ConfigOption<String> configOption) {
			Duration interval = Duration.apply(configuration.getString(configOption));
			return Time.milliseconds(interval.toMillis()).toMilliseconds();
		}
	}
}
