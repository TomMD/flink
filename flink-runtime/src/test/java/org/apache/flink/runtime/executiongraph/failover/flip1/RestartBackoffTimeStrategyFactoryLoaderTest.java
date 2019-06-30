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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartBackoffTimeStrategyOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

/**
 * Unit tests for {@link RestartBackoffTimeStrategyFactoryLoader}.
 */
public class RestartBackoffTimeStrategyFactoryLoaderTest extends TestLogger {

	private static final RestartStrategies.RestartStrategyConfiguration DEFAULT_RESTART_STRATEGY_CONFIGURATION =
		new RestartStrategies.FallbackRestartStrategyConfiguration();

	@Test
	public void testNewStrategySpecified() throws Exception {
		// specify RestartBackoffTimeStrategy directly in cluster config
		final Configuration conf = new Configuration();
		conf.setString(
			RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_CLASS_NAME,
			TestRestartBackoffTimeStrategy.class.getName());

		// the RestartStrategyConfiguration should not take effect as the loader will
		// directly create the factory from the config of the new version strategy
		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartStrategyFactory(
				new RestartStrategies.FailureRateRestartStrategyConfiguration(
					1,
					Time.milliseconds(1000),
					Time.milliseconds(1000)),
				conf,
				true);

		assertThat(
			factory,
			instanceOf(TestRestartBackoffTimeStrategy.TestRestartBackoffTimeStrategyFactory.class));
	}

	@Test
	public void testInvalidNewStrategySpecified() throws Exception {
		final Configuration conf = new Configuration();
		conf.setString(
			RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_CLASS_NAME,
			InvalidTestRestartBackoffTimeStrategy.class.getName());

		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartStrategyFactory(
				DEFAULT_RESTART_STRATEGY_CONFIGURATION,
				conf,
				true);

		assertThat(
			factory,
			instanceOf(NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.class));
	}

	@Test
	public void testNoStrategySpecifiedWhenCheckpointingEnabled() throws Exception {
		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartStrategyFactory(
				DEFAULT_RESTART_STRATEGY_CONFIGURATION,
				new Configuration(),
				true);

		assertThat(
			factory,
			instanceOf(FixedDelayRestartBackoffTimeStrategy.FixedDelayRestartBackoffTimeStrategyFactory.class));
	}

	@Test
	public void testNoStrategySpecifiedWhenCheckpointingDisabled() throws Exception {
		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartStrategyFactory(
				DEFAULT_RESTART_STRATEGY_CONFIGURATION,
				new Configuration(),
				false);

		assertThat(
			factory,
			instanceOf(NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.class));
	}

	@Test
	public void testLegacyStrategySpecifiedInJobConfig() throws Exception {
		// the fixed delay strategy config in cluster config should not take effect
		// as it will be overridden by the failure rate strategy config in job config
		final Configuration conf = new Configuration();
		conf.setString(ConfigConstants.RESTART_STRATEGY, "fixed-delay");

		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartStrategyFactory(
				new RestartStrategies.FailureRateRestartStrategyConfiguration(
					1,
					Time.milliseconds(1000),
					Time.milliseconds(1000)),
				conf,
				false);

		assertThat(
			factory,
			instanceOf(FailureRateRestartBackoffTimeStrategy.FailureRateRestartBackoffTimeStrategyFactory.class));
	}

	@Test
	public void testLegacyStrategySpecifiedInClusterConfig() throws Exception {
		// the fixed delay strategy config in cluster config should not take effect
		// as it will be overridden by the failure rate strategy config in job config
		final Configuration conf = new Configuration();
		conf.setString(ConfigConstants.RESTART_STRATEGY, "fixed-delay");

		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartStrategyFactory(
				DEFAULT_RESTART_STRATEGY_CONFIGURATION,
				conf,
				false);

		assertThat(
			factory,
			instanceOf(FixedDelayRestartBackoffTimeStrategy.FixedDelayRestartBackoffTimeStrategyFactory.class));
	}

	@Test
	public void testDeprecatedValuesSpecifiedInClusterConfig() throws Exception {
		// the fixed delay strategy config in cluster config should not take effect
		// as it will be overridden by the failure rate strategy config in job config
		final Configuration conf = new Configuration();
		conf.setInteger(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);

		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartStrategyFactory(
				DEFAULT_RESTART_STRATEGY_CONFIGURATION,
				conf,
				false);

		assertThat(
			factory,
			instanceOf(FixedDelayRestartBackoffTimeStrategy.FixedDelayRestartBackoffTimeStrategyFactory.class));
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	/**
	 * A test implementation of {@link RestartBackoffTimeStrategy}.
	 */
	private static class TestRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {

		@Override
		public boolean canRestart() {
			return false;
		}

		@Override
		public long getBackoffTime() {
			return 0;
		}

		@Override
		public void notifyFailure(Throwable cause) {

		}

		public static TestRestartBackoffTimeStrategyFactory createFactory(final Configuration configuration) {
			return new TestRestartBackoffTimeStrategyFactory();
		}

		/**
		 * The factory for creating {@link TestRestartBackoffTimeStrategy}.
		 */
		private static class TestRestartBackoffTimeStrategyFactory implements Factory {

			@Override
			public RestartBackoffTimeStrategy create() {
				return new TestRestartBackoffTimeStrategy();
			}
		}
	}

	/**
	 * A test class that has not implemented {@link RestartBackoffTimeStrategy}.
	 */
	private static class InvalidTestRestartBackoffTimeStrategy {}
}
