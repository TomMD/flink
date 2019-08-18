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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.io.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.InputGateUtil;
import org.apache.flink.streaming.runtime.io.InputProcessorUtil;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamstatus.ForwardingValveOutputHandler;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link StreamTask} for executing a {@link OneInputStreamOperator}.
 */
@Internal
public class OneInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

	private final WatermarkGauge inputWatermarkGauge = new WatermarkGauge();

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * @param env The task environment for this task.
	 */
	public OneInputStreamTask(Environment env) {
		super(env);
	}

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * <p>This constructor accepts a special {@link ProcessingTimeService}. By default (and if
	 * null is passes for the time provider) a {@link SystemProcessingTimeService DefaultTimerService}
	 * will be used.
	 *
	 * @param env The task environment for this task.
	 * @param timeProvider Optionally, a specific time provider to use.
	 */
	@VisibleForTesting
	public OneInputStreamTask(
			Environment env,
			@Nullable ProcessingTimeService timeProvider) {
		super(env, timeProvider);
	}

	@Override
	public void init() throws Exception {
		initializeStreamInputProcessor();

		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge);
		// wrap watermark gauge since registered metrics must be unique
		getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge::getValue);
	}

	private void initializeStreamInputProcessor() throws IOException {
		StreamConfig configuration = getConfiguration();

		if (configuration.getNumberOfInputs() > 0) {
			InputGate[] inputGates = getEnvironment().getAllInputGates();
			InputGate inputGate = InputGateUtil.createInputGate(inputGates);

			CheckpointedInputGate barrierHandler = InputProcessorUtil.createCheckpointedInputGate(
				this,
				configuration.getCheckpointMode(),
				getEnvironment().getIOManager(),
				inputGate,
				getEnvironment().getTaskManagerInfo().getConfiguration(),
				getTaskNameWithSubtaskAndId());
			getEnvironment().getMetricGroup().getIOMetricGroup().gauge(
				"checkpointAlignmentTime", barrierHandler::getAlignmentDurationNanos);

			StatusWatermarkValve statusWatermarkValve = new StatusWatermarkValve(
				inputGate.getNumberOfInputChannels(),
				new ForwardingValveOutputHandler(
					(watermark) -> {
						synchronized (getCheckpointLock()) {
							inputWatermarkGauge.setCurrentWatermark(watermark.getTimestamp());
							headOperator.processWatermark(watermark);
						}
					},
					(status) -> {
						synchronized (getCheckpointLock()) {
							getStreamStatusMaintainer().toggleStreamStatus(status);
						}
					}
				));

			TypeSerializer<IN> inputSerializer = configuration.getTypeSerializerIn1(getUserCodeClassLoader());
			StreamTaskInput input = new StreamTaskNetworkInput(
				barrierHandler, inputSerializer, getEnvironment().getIOManager(), 0);

			inputProcessor = new StreamOneInputProcessor<>(
				input,
				getCheckpointLock(),
				headOperator,
				statusWatermarkValve,
				operatorChain,
				setupNumRecordsInCounter(headOperator));
		}
	}
}
