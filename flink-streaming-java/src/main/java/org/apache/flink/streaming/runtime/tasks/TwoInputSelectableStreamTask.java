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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.io.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.InputGateUtil;
import org.apache.flink.streaming.runtime.io.InputProcessorUtil;
import org.apache.flink.streaming.runtime.io.InputSelectionHandler;
import org.apache.flink.streaming.runtime.io.PushBasedAsyncDataInput;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkOutput;
import org.apache.flink.streaming.runtime.io.StreamTwoInputSelectableProcessor;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamstatus.ForwardingValveOutputHandler;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StreamTask} for executing a {@link TwoInputStreamOperator} and supporting
 * the {@link TwoInputStreamOperator} to select input for reading.
 */
@Internal
public class TwoInputSelectableStreamTask<IN1, IN2, OUT> extends AbstractTwoInputStreamTask<IN1, IN2, OUT> {

	public TwoInputSelectableStreamTask(Environment env) {
		super(env);
	}

	@Override
	protected void createInputProcessor(
		Collection<InputGate> inputGates1,
		Collection<InputGate> inputGates2,
		TypeSerializer<IN1> inputDeserializer1,
		TypeSerializer<IN2> inputDeserializer2) throws IOException {

		checkState(headOperator instanceof InputSelectable);
		InputSelectionHandler inputSelectionHandler = new InputSelectionHandler((InputSelectable) headOperator);

		InputGate unionedInputGate1 = InputGateUtil.createInputGate(inputGates1.toArray(new InputGate[0]));
		InputGate unionedInputGate2 = InputGateUtil.createInputGate(inputGates2.toArray(new InputGate[0]));

		// create a Input instance for each input
		CheckpointedInputGate[] checkpointedInputGates = InputProcessorUtil.createCheckpointedInputGatePair(
			this,
			getConfiguration().getCheckpointMode(),
			getEnvironment().getIOManager(),
			unionedInputGate1,
			unionedInputGate2,
			getEnvironment().getTaskManagerInfo().getConfiguration(),
			getTaskNameWithSubtaskAndId());
		checkState(checkpointedInputGates.length == 2);

		inputProcessor = new StreamTwoInputSelectableProcessor(
			createDataInput(checkpointedInputGates[0], inputDeserializer1, 0),
			createDataInput(checkpointedInputGates[1], inputDeserializer2, 1),
			createDataOutput(inputSelectionHandler, 0),
			createDataOutput(inputSelectionHandler, 1),
			getCheckpointLock(),
			operatorChain,
			inputSelectionHandler);
	}

	private StreamTaskInput createDataInput(
			CheckpointedInputGate checkpointedInputGate,
			TypeSerializer<?> inputSerializer,
			int inputIndex) {

		return new StreamTaskNetworkInput(
			checkpointedInputGate,
			inputSerializer,
			getEnvironment().getIOManager(),
			createStatusWatermarkValve(checkpointedInputGate.getNumberOfInputChannels(), inputIndex),
			inputIndex);
	}

	private StatusWatermarkValve createStatusWatermarkValve(
			int numberOfInputChannels,
			int inputIndex) {

		WatermarkGauge watermarkGauge = (inputIndex == 0) ? input1WatermarkGauge : input2WatermarkGauge;

		return new StatusWatermarkValve(
			numberOfInputChannels,
			new ForwardingValveOutputHandler(
				(watermark) -> {
					synchronized (getCheckpointLock()) {
						watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
						if (inputIndex == 0) {
							headOperator.processWatermark1(watermark);
						} else {
							headOperator.processWatermark2(watermark);
						}
					}
				},
				(status) -> {
					synchronized (getCheckpointLock()) {
						getStreamStatusMaintainer().toggleStreamStatus(status);
					}
				}
			));
	}

	private PushBasedAsyncDataInput.DataOutput createDataOutput(
			InputSelectionHandler inputSelectionHandler,
			int inputIndex) {
		Counter numRecordsIn = setupNumRecordsInCounter(headOperator);

		return new StreamTaskNetworkOutput(
			(record) -> {
				synchronized (getCheckpointLock()) {
					numRecordsIn.inc();
					if (inputIndex == 0) {
						headOperator.setKeyContextElement1(record);
						headOperator.processElement1(record);
					} else {
						headOperator.setKeyContextElement2(record);
						headOperator.processElement2(record);
					}
					inputSelectionHandler.nextSelection();
				}
			},
			(latencyMarker) -> {
				synchronized (getCheckpointLock()) {
					if (inputIndex == 0) {
						headOperator.processLatencyMarker1(latencyMarker);
					} else {
						headOperator.processLatencyMarker2(latencyMarker);
					}
				}
			});
	}
}
