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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.TwoInputSelectableStreamTask}
 * in the case that the operator is InputSelectable.
 */
@Internal
public final class StreamTwoInputSelectableProcessor implements StreamInputProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(StreamTwoInputSelectableProcessor.class);

	private static final CompletableFuture<?> UNAVAILABLE = new CompletableFuture<>();

	private final InputSelectionHandler inputSelectionHandler;

	private final Object lock;

	private final StreamTaskInput input1;
	private final StreamTaskInput input2;

	private final PushBasedAsyncDataInput.DataOutput output1;
	private final PushBasedAsyncDataInput.DataOutput output2;

	private final OperatorChain<?, ?> operatorChain;

	/** always try to read from the first input. */
	private int lastReadInputIndex = 1;

	private boolean isPrepared = false;

	public StreamTwoInputSelectableProcessor(
			StreamTaskInput input1,
			StreamTaskInput input2,
			PushBasedAsyncDataInput.DataOutput output1,
			PushBasedAsyncDataInput.DataOutput output2,
			Object lock,
			OperatorChain<?, ?> operatorChain,
			InputSelectionHandler inputSelectionHandler) {

		this.input1 = checkNotNull(input1);
		this.input2 = checkNotNull(input2);

		this.output1 = checkNotNull(output1);
		this.output2 = checkNotNull(output2);

		this.lock = checkNotNull(lock);

		this.operatorChain = checkNotNull(operatorChain);

		this.inputSelectionHandler = checkNotNull(inputSelectionHandler);
	}

	@Override
	public boolean isFinished() {
		return input1.isFinished() && input2.isFinished();
	}

	@Override
	public CompletableFuture<?> isAvailable() {
		if (inputSelectionHandler.isALLInputsSelected()) {
			return isAnyInputAvailable();
		} else {
			StreamTaskInput input = (inputSelectionHandler.isFirstInputSelected()) ? input1 : input2;
			return input.isAvailable();
		}
	}

	@Override
	public InputStatus processInput() throws Exception {
		if (!isPrepared) {
			// the preparations here are not placed in the constructor because all work in it
			// must be executed after all operators are opened.
			prepareForProcessing();
		}

		int readingInputIndex = selectNextReadingInputIndex();
		if (readingInputIndex == -1) {
			return InputStatus.NOTHING_AVAILABLE;
		}
		lastReadInputIndex = readingInputIndex;

		InputStatus status;
		if (readingInputIndex == 0) {
			status = input1.emitNext(output1);
			checkFinished(input1, lastReadInputIndex);
		} else {
			status = input2.emitNext(output2);
			checkFinished(input2, lastReadInputIndex);
		}

		if (status != InputStatus.MORE_AVAILABLE) {
			inputSelectionHandler.setUnavailableInput(readingInputIndex);
		}

		return status;
	}

	private void checkFinished(StreamTaskInput input, int inputIndex) throws Exception {
		if (input.isFinished()) {
			synchronized (lock) {
				operatorChain.endInput(getInputId(inputIndex));
				inputSelectionHandler.nextSelection();
			}
		}
	}

	@Override
	public void close() throws IOException {
		IOException ex = null;
		try {
			input1.close();
		} catch (IOException e) {
			ex = ExceptionUtils.firstOrSuppressed(e, ex);
		}

		try {
			input2.close();
		} catch (IOException e) {
			ex = ExceptionUtils.firstOrSuppressed(e, ex);
		}

		if (ex != null) {
			throw ex;
		}
	}

	private int selectNextReadingInputIndex() throws IOException {
		updateAvailability();
		checkInputSelectionAgainstIsFinished();

		int readingInputIndex = inputSelectionHandler.selectNextInputIndex(lastReadInputIndex);
		if (readingInputIndex == -1) {
			return -1;
		}

		// to avoid starvation, if the input selection is ALL and availableInputsMask is not ALL,
		// always try to check and set the availability of another input
		// TODO: because this can be a costly operation (checking volatile inside CompletableFuture`
		//  this might be optimized to only check once per processed NetworkBuffer
		if (inputSelectionHandler.shouldSetAvailableForAnotherInput()) {
			checkAndSetAvailable(1 - readingInputIndex);
		}

		return readingInputIndex;
	}

	private void checkInputSelectionAgainstIsFinished() throws IOException {
		if (inputSelectionHandler.isALLInputsSelected()) {
			return;
		}
		if (inputSelectionHandler.isFirstInputSelected() && input1.isFinished()) {
			throw new IOException("Can not make a progress: only first input is selected but it is already finished");
		}
		if (inputSelectionHandler.isSecondInputSelected() && input2.isFinished()) {
			throw new IOException("Can not make a progress: only second input is selected but it is already finished");
		}
	}

	private void updateAvailability() {
		if (!input1.isFinished() && input1.isAvailable() == AVAILABLE) {
			inputSelectionHandler.setAvailableInput(input1.getInputIndex());
		}
		if (!input2.isFinished() && input2.isAvailable() == AVAILABLE) {
			inputSelectionHandler.setAvailableInput(input2.getInputIndex());
		}
	}

	private void prepareForProcessing() {
		// Note: the first call to nextSelection () on the operator must be made after this operator
		// is opened to ensure that any changes about the input selection in its open()
		// method take effect.
		inputSelectionHandler.nextSelection();

		isPrepared = true;
	}

	private void checkAndSetAvailable(int inputIndex) {
		StreamTaskInput input = getInput(inputIndex);
		if (!input.isFinished() && input.isAvailable().isDone()) {
			inputSelectionHandler.setAvailableInput(inputIndex);
		}
	}

	private CompletableFuture<?> isAnyInputAvailable() {
		if (input1.isFinished()) {
			return input2.isFinished() ? AVAILABLE : input2.isAvailable();
		}

		if (input2.isFinished()) {
			return input1.isAvailable();
		}

		CompletableFuture<?> input1Available = input1.isAvailable();
		CompletableFuture<?> input2Available = input2.isAvailable();

		return (input1Available == AVAILABLE || input2Available == AVAILABLE) ?
			AVAILABLE : CompletableFuture.anyOf(input1Available, input2Available);
	}

	private StreamTaskInput getInput(int inputIndex) {
		return inputIndex == 0 ? input1 : input2;
	}

	private int getInputId(int inputIndex) {
		return inputIndex + 1;
	}
}
