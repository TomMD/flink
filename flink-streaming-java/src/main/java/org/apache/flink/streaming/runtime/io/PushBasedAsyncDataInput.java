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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.AsyncDataInput;
import org.apache.flink.runtime.io.AvailabilityListener;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * The variant of {@link AsyncDataInput} that is for handling both network input
 * and source input in a unified way from {@link #emitNext(DataOutput)} instead
 * returning {@code Optional.empty()} from {@link AsyncDataInput#pollNext()}.
 */
@Internal
public interface PushBasedAsyncDataInput<T> extends AvailabilityListener {

	/**
	 * Pushes the next element to the output from current data input, and returns
	 * the input status to indicate whether there are more available data in
	 * current input.
	 *
	 * <p>This method should be non blocking.
	 */
	InputStatus emitNext(DataOutput output) throws Exception;

	/**
	 * Basic interface of outputs used in emitting the next element from data input.
	 */
	interface DataOutput {

		void emitRecord(StreamRecord record) throws Exception;

		void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception;
	}
}
