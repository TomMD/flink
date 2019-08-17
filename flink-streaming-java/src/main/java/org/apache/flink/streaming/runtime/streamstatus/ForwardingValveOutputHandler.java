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
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve.ValveOutputHandler;
import org.apache.flink.util.function.ThrowingConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A unified implementation of {@link ValveOutputHandler} which is called by the
 * {@link StatusWatermarkValve} only when it determines a new watermark or stream
 * status can be propagated.
 */
@Internal
public class ForwardingValveOutputHandler implements ValveOutputHandler {

	private final ThrowingConsumer<Watermark, Exception> watermarkConsumer;
	private final ThrowingConsumer<StreamStatus, Exception> statusConsumer;

	public ForwardingValveOutputHandler(
			ThrowingConsumer<Watermark, Exception> watermarkConsumer,
			ThrowingConsumer<StreamStatus, Exception> statusConsumer) {

		this.watermarkConsumer = checkNotNull(watermarkConsumer);
		this.statusConsumer = checkNotNull(statusConsumer);
	}

	@Override
	public void handleWatermark(Watermark watermark) throws Exception {
		watermarkConsumer.accept(watermark);
	}

	@Override
	public void handleStreamStatus(StreamStatus streamStatus) throws Exception {
		statusConsumer.accept(streamStatus);
	}
}
