/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implements a stream status maintainer to emit {@link StreamStatus} via
 * {@link RecordWriterOutput} while the status has changed.
 */
@Internal
public class StreamStatusMaintainerImpl implements StreamStatusMaintainer {

	private final RecordWriterOutput<?>[] recordWriterOutputs;

	private StreamStatus currentStatus;

	private final StreamStatus[] streamStatuses;

	public StreamStatusMaintainerImpl(RecordWriterOutput<?>[] recordWriterOutputs, int numberOfInputs) {
		this.recordWriterOutputs = checkNotNull(recordWriterOutputs);

		checkArgument(numberOfInputs > 0);
		this.streamStatuses = new StreamStatus[numberOfInputs];
		for (int i = 0; i < numberOfInputs; i++) {
			streamStatuses[i] = StreamStatus.ACTIVE;
		}

		this.currentStatus = StreamStatus.ACTIVE;
	}

	@Override
	public StreamStatus getStreamStatus() {
		return currentStatus;
	}

	@Override
	public void toggleStreamStatus(StreamStatus streamStatus) {
		if (shouldToggleStreamStatus(streamStatus)) {
			currentStatus = streamStatus;

			// try and forward the stream status change to all outgoing connections
			for (RecordWriterOutput<?> output : recordWriterOutputs) {
				output.emitStreamStatus(streamStatus);
			}
		}
	}

	private boolean shouldToggleStreamStatus(StreamStatus streamStatus) {
		if (streamStatus.equals(currentStatus)) {
			return false;
		}

		if (streamStatus.isIdle()) {
			for (StreamStatus status : streamStatuses) {
				if (status.isActive()) {
					return false;
				}
			}
		}
		return true;
	}
}
