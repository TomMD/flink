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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.IOException;

/**
 * A buffer-oriented runtime result writer API for producing results.
 */
public interface ResultPartitionWriter extends AutoCloseable {

	/**
	 * Setup partition, potentially heavy-weight, blocking operation comparing to just creation.
	 */
	void setup() throws IOException;

	ResultPartitionID getPartitionId();

	int getNumberOfSubpartitions();

	int getNumTargetKeyGroups();

	/**
	 * Writes the {@link AbstractEvent} to all the sub partitions.
	 */
	void broadcastEvents(AbstractEvent event) throws IOException;

	/**
	 * Requests a {@link BufferBuilder} and adds the created {@link BufferConsumer} to the sub partition
	 * with the given index.
	 *
	 * <p>For PIPELINED {@link org.apache.flink.runtime.io.network.partition.ResultPartitionType}s,
	 * this will trigger the deployment of consuming tasks after the first buffer has been added.
	 *
	 * <p>To avoid problems with data re-ordering, before requesting new {@link BufferBuilder} the
	 * previously requested one with the given {@code subpartitionIndex} must be marked as
	 * {@link BufferBuilder#isFinished()}.
	 */
	BufferBuilder requestBufferBuilder(int subpartitionIndex) throws IOException, InterruptedException;

	/**
	 * Manually trigger consumption from enqueued {@link BufferConsumer BufferConsumers} in all subpartitions.
	 */
	void flushAll();

	/**
	 * Manually trigger consumption from enqueued {@link BufferConsumer BufferConsumers} in one specified subpartition.
	 */
	void flush(int subpartitionIndex);
}
