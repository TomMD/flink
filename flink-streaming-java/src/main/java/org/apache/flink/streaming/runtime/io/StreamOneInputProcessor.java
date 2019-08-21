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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards them to event subscribers once the
 * {@link StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or
 * that a {@link StreamStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link OneInputStreamOperator} concurrently with the timer callback or other things.
 */
@Internal
public final class StreamOneInputProcessor implements StreamInputProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(StreamOneInputProcessor.class);

	private final StreamTaskInput input;

	private final PushBasedAsyncDataInput.DataOutput output;

	private final OperatorChain<?, ?> operatorChain;

	private final Object lock;

	public StreamOneInputProcessor(
			StreamTaskInput input,
			PushBasedAsyncDataInput.DataOutput output,
			OperatorChain<?, ?> operatorChain,
			Object lock) {
		this.input = checkNotNull(input);
		this.output = checkNotNull(output);

		this.operatorChain = checkNotNull(operatorChain);

		this.lock = checkNotNull(lock);
	}

	@Override
	public boolean isFinished() {
		return input.isFinished();
	}

	@Override
	public CompletableFuture<?> isAvailable() {
		return input.isAvailable();
	}

	@Override
	public InputStatus processInput() throws Exception {
		InputStatus status = input.emitNext(output);

		checkFinished();

		return status;
	}

	private void checkFinished() throws Exception {
		if (input.isFinished()) {
			synchronized (lock) {
				operatorChain.endInput(1);
			}
		}
	}

	@Override
	public void close() throws IOException {
		input.close();
	}
}
