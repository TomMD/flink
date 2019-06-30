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
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartBackoffTimeStrategyOptions;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility class to load {@link RestartBackoffTimeStrategy.Factory} from the configuration.
 */
public class RestartBackoffTimeStrategyFactoryLoader {

	private static final Logger LOG = LoggerFactory.getLogger(RestartBackoffTimeStrategyFactoryLoader.class);

	private static final String CREATE_METHOD = "createFactory";

	/**
	 * Creates proper {@link RestartBackoffTimeStrategy.Factory}.
	 * If new version restart strategy is specified, will directly use it.
	 * Otherwise will decide based on legacy restart strategy configs.
	 *
	 * @param jobRestartStrategyConfiguration restart configuration given within the job graph
	 * @param clusterConfiguration cluster(server-side) configuration, usually represented as jobmanager config
	 * @param isCheckpointingEnabled if checkpointing was enabled for the job
	 * @return new version restart strategy factory
	 */
	public static RestartBackoffTimeStrategy.Factory createRestartStrategyFactory(
		final RestartStrategies.RestartStrategyConfiguration jobRestartStrategyConfiguration,
		final Configuration clusterConfiguration,
		final boolean isCheckpointingEnabled) throws Exception {

		checkNotNull(jobRestartStrategyConfiguration);
		checkNotNull(clusterConfiguration);

		final String restartStrategyClassName = clusterConfiguration.getString(
			RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_CLASS_NAME);

		if (restartStrategyClassName != null) {
			// create new restart strategy directly if it is specified in cluster config
			return createRestartStrategyFactoryInternal(clusterConfiguration);
		} else {
			// adapt the legacy restart strategy configs as new restart strategy configs
			final Configuration adaptedConfiguration = getAdaptedConfiguration(
				jobRestartStrategyConfiguration,
				clusterConfiguration,
				isCheckpointingEnabled);

			// create new restart strategy from the adapted config
			return createRestartStrategyFactoryInternal(adaptedConfiguration);
		}
	}

	/**
	 * Decides the {@link RestartBackoffTimeStrategy} to use and its params based on legacy configs,
	 * and records its class name and the params into a adapted configuration.
	 *
	 * <p>The decision making is as follows:
	 * <ol>
	 *     <li>Use strategy of {@link RestartStrategies.RestartStrategyConfiguration}, unless it's a
	 * {@link RestartStrategies.FallbackRestartStrategyConfiguration} or not valid.</li>
	 *     <li>If strategy is not decided, use legacy strategy specified in cluster(server-side) config,
	 * unless it is not set or not valid</li>
	 *     <li>If strategy is not decided, use {@link FixedDelayRestartStrategy} if checkpointing is
	 * enabled. Otherwise use {@link NoRestartStrategy}</li>
	 * </ol>
	 *
	 * @param jobRestartStrategyConfiguration restart configuration given within the job graph
	 * @param clusterConfiguration cluster(server-side) configuration, usually represented as jobmanager config
	 * @param isCheckpointingEnabled if checkpointing was enabled for the job
	 * @return adapted configuration
	 */
	private static Configuration getAdaptedConfiguration(
		final RestartStrategies.RestartStrategyConfiguration jobRestartStrategyConfiguration,
		final Configuration clusterConfiguration,
		final boolean isCheckpointingEnabled) throws Exception {

		// clone a new config to not modify existing one
		final Configuration adaptedConfiguration = new Configuration();

		// try determining restart strategy based on job restart strategy config first
		if (!tryAdaptingJobRestartStrategyConfiguration(jobRestartStrategyConfiguration, adaptedConfiguration)) {

			// if job restart strategy config does not help
			// try determining restart strategy based on cluster config as fallback
			if (!tryAdaptingClusterRestartStrategyConfiguration(clusterConfiguration, adaptedConfiguration)) {

				// if the restart strategy is not decided yet
				// determine the strategy based on whether checkpointing is enabled
				setDefaultRestartStrategyToConfiguration(isCheckpointingEnabled, adaptedConfiguration);
			}
		}

		return adaptedConfiguration;
	}

	private static RestartBackoffTimeStrategy.Factory createRestartStrategyFactoryInternal(
		final Configuration configuration) {

		final String restartStrategyClassName = configuration.getString(
			RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_CLASS_NAME);

		if (restartStrategyClassName == null) {
			LOG.info("No restart strategy is configured. Using no restart strategy.");
			return new NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory();
		}

		try {
			final Class<?> clazz = Class.forName(restartStrategyClassName);
			if (clazz != null && RestartBackoffTimeStrategy.class.isAssignableFrom(clazz)) {
				final Method method = clazz.getMethod(CREATE_METHOD, Configuration.class);
				if (method != null) {
					final Object result = method.invoke(null, configuration);

					if (result != null) {
						return (RestartBackoffTimeStrategy.Factory) result;
					}
				}
			}
		} catch (ClassNotFoundException cnfe) {
			LOG.warn("Could not find restart strategy class {}.", restartStrategyClassName);
		} catch (NoSuchMethodException nsme) {
			LOG.warn("Class {} does not has static method {}.", restartStrategyClassName, CREATE_METHOD);
		} catch (InvocationTargetException ite) {
			LOG.warn("Cannot call static method {} from class {}.", CREATE_METHOD, restartStrategyClassName);
		} catch (IllegalAccessException iae) {
			LOG.warn("Illegal access while calling method {} from class {}.", CREATE_METHOD, restartStrategyClassName);
		}

		// fallback in case of an error
		LOG.info("Configured restart strategy {} is not valid. Fall back to no restart strategy.",
			restartStrategyClassName);
		return new NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory();
	}

	private static boolean tryAdaptingJobRestartStrategyConfiguration(
		final RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration,
		final Configuration adaptedConfiguration) throws Exception {

		if (restartStrategyConfiguration instanceof RestartStrategies.NoRestartStrategyConfiguration) {
			setNoRestartStrategyToConfiguration(adaptedConfiguration);

			return true;
		} else if (restartStrategyConfiguration instanceof RestartStrategies.FixedDelayRestartStrategyConfiguration) {
			final RestartStrategies.FixedDelayRestartStrategyConfiguration fixedDelayConfig =
				(RestartStrategies.FixedDelayRestartStrategyConfiguration) restartStrategyConfiguration;

			setFixedDelayRestartStrategyToConfiguration(
				adaptedConfiguration,
				fixedDelayConfig.getDelayBetweenAttemptsInterval().toMilliseconds(),
				fixedDelayConfig.getRestartAttempts());

			return true;
		} else if (restartStrategyConfiguration instanceof RestartStrategies.FailureRateRestartStrategyConfiguration) {
			final RestartStrategies.FailureRateRestartStrategyConfiguration failureRateConfig =
				(RestartStrategies.FailureRateRestartStrategyConfiguration) restartStrategyConfiguration;

			setFailureRateRestartStrategyToConfiguration(
				adaptedConfiguration,
				failureRateConfig.getDelayBetweenAttemptsInterval().toMilliseconds(),
				failureRateConfig.getFailureInterval().toMilliseconds(),
				failureRateConfig.getMaxFailureRate());

			return true;
		} else if (restartStrategyConfiguration instanceof RestartStrategies.FallbackRestartStrategyConfiguration) {
			return false;
		} else {
			throw new IllegalArgumentException("Unknown restart strategy configuration " +
				restartStrategyConfiguration + ".");
		}
	}

	private static boolean tryAdaptingClusterRestartStrategyConfiguration(
		final Configuration clusterConfiguration,
		final Configuration adaptedConfiguration) throws Exception {

		final String restartStrategyName = clusterConfiguration.getString(ConfigConstants.RESTART_STRATEGY, null);
		if (restartStrategyName == null) {
			return tryAdaptingClusterConfigurationOfDeprecatedValues(clusterConfiguration, adaptedConfiguration);
		}

		switch (restartStrategyName.toLowerCase()) {
			case "none":
			case "off":
			case "disable":
				setNoRestartStrategyToConfiguration(adaptedConfiguration);
				return true;
			case "fixeddelay":
			case "fixed-delay":
				adaptClusterConfigurationOfFixedDelayRestartStrategy(
					clusterConfiguration,
					adaptedConfiguration);
				return true;
			case "failurerate":
			case "failure-rate":
				adaptClusterConfigurationOfFailureRateRestartStrategy(
					clusterConfiguration,
					adaptedConfiguration);
				return true;
			default:
				return false;
		}
	}

	private static boolean tryAdaptingClusterConfigurationOfDeprecatedValues(
		final Configuration clusterConfiguration,
		final Configuration adaptedConfiguration) throws Exception {

		// support deprecated ConfigConstants values
		final int maxAttempts = clusterConfiguration.getInteger(
			ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
			ConfigConstants.DEFAULT_EXECUTION_RETRIES);
		final String pauseString = clusterConfiguration.getString(
			AkkaOptions.WATCH_HEARTBEAT_PAUSE);
		final String delayString = clusterConfiguration.getString(
			ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY,
			pauseString);

		final long delay;
		try {
			delay = Duration.apply(delayString).toMillis();
		} catch (NumberFormatException nfe) {
			if (delayString.equals(pauseString)) {
				throw new Exception("Invalid config value for " +
					AkkaOptions.WATCH_HEARTBEAT_PAUSE.key() + ": " + pauseString +
					". Value must be a valid duration (such as '10 s' or '1 min')");
			} else {
				throw new Exception("Invalid config value for " +
					ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY + ": " + delayString +
					". Value must be a valid duration (such as '100 milli' or '10 s')");
			}
		}

		if (maxAttempts > 0 && delay >= 0) {
			setFixedDelayRestartStrategyToConfiguration(adaptedConfiguration, delay, maxAttempts);
			return true;
		} else {
			return false;
		}
	}

	private static void adaptClusterConfigurationOfFixedDelayRestartStrategy(
		final Configuration clusterConfiguration,
		final Configuration adaptedConfiguration) throws Exception {

		final int maxAttempts = clusterConfiguration.getInteger(
			ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
			1);
		final String delayString = clusterConfiguration.getString(
			ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY);

		final long delay;
		try {
			delay = Duration.apply(delayString).toMillis();
		} catch (NumberFormatException nfe) {
			throw new Exception("Invalid config value for " +
				ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY + ": " + delayString +
				". Value must be a valid duration (such as '100 milli' or '10 s')");
		}

		setFixedDelayRestartStrategyToConfiguration(adaptedConfiguration, delay, maxAttempts);
	}

	private static void adaptClusterConfigurationOfFailureRateRestartStrategy(
		final Configuration clusterConfiguration,
		final Configuration adaptedConfiguration) throws Exception {

		final int maxFailuresPerInterval = clusterConfiguration.getInteger(
			ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL,
			1);
		final String failuresIntervalString = clusterConfiguration.getString(
			ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL,
			Duration.apply(1, TimeUnit.MINUTES).toString()
		);
		final String timeoutString = clusterConfiguration.getString(
			AkkaOptions.WATCH_HEARTBEAT_INTERVAL);
		final String delayString = clusterConfiguration.getString(
			ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_DELAY, timeoutString);

		final long failuresInterval;
		try {
			failuresInterval = Duration.apply(failuresIntervalString).toMillis();
		} catch (NumberFormatException nfe) {
			throw new Exception("Invalid config value for " +
				ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL + ": "
				+ failuresIntervalString + ". Value must be a valid duration (such as '100 milli' or '10 s')");
		}
		final long delay;
		try {
			delay = Duration.apply(delayString).toMillis();
		} catch (NumberFormatException nfe) {
			throw new Exception("Invalid config value for " +
				ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_DELAY + ": " + delayString +
				". Value must be a valid duration (such as '100 milli' or '10 s')");
		}

		setFailureRateRestartStrategyToConfiguration(adaptedConfiguration, delay, failuresInterval, maxFailuresPerInterval);
	}

	private static void setDefaultRestartStrategyToConfiguration(
		final boolean isCheckpointingEnabled,
		final Configuration configuration) {

		if (isCheckpointingEnabled) {
			// fixed delay restart strategy with default params
			setFixedDelayRestartStrategyToConfiguration(configuration, 0L, Integer.MAX_VALUE);
		} else {
			// no restart strategy
			setNoRestartStrategyToConfiguration(configuration);
		}
	}

	private static void setNoRestartStrategyToConfiguration(final Configuration configuration) {
		setRestartBackoffTimeStrategyToConfiguration(
			NoRestartBackoffTimeStrategy.class,
			configuration);
	}

	private static void setFixedDelayRestartStrategyToConfiguration(
		final Configuration configuration,
		final long backoffTime,
		final int maxAttempts) {

		setRestartBackoffTimeStrategyToConfiguration(
			FixedDelayRestartBackoffTimeStrategy.class,
			configuration);
		configuration.setLong(
			RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_FIXED_DELAY_BACKOFF_TIME,
			backoffTime);
		configuration.setInteger(
			RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_FIXED_DELAY_ATTEMPTS,
			maxAttempts);
	}

	private static void setFailureRateRestartStrategyToConfiguration(
		final Configuration configuration,
		final long backoffTime,
		final long failuresInterval,
		final int maxFailuresPerInterval) {

		setRestartBackoffTimeStrategyToConfiguration(
			FailureRateRestartBackoffTimeStrategy.class,
			configuration);
		configuration.setLong(
			RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_FAILURE_RATE_BACKOFF_TIME,
			backoffTime);
		configuration.setLong(
			RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL,
			failuresInterval);
		configuration.setInteger(
			RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL,
			maxFailuresPerInterval);
	}

	private static void setRestartBackoffTimeStrategyToConfiguration(
		final Class<?> strategyClass,
		final Configuration configuration) {

		checkArgument(RestartBackoffTimeStrategy.class.isAssignableFrom(strategyClass),
			strategyClass + " is not a valid RestartBackoffTimeStrategy implementation.");

		configuration.setString(
			RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_CLASS_NAME,
			strategyClass.getName());
	}
}
