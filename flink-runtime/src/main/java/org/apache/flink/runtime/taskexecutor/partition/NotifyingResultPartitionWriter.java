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

package org.apache.flink.runtime.taskexecutor.partition;

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * TODO: add javadoc.
 */
public class NotifyingResultPartitionWriter implements ResultPartitionWriter {

	private final ResultPartitionWriter backingResultPartitionWriter;
	private final Consumer<ResultPartitionID> onSetupAction;
	private final Consumer<ResultPartitionID> onFailAction;
	private final Consumer<ResultPartitionID> onFinishAction;

	public NotifyingResultPartitionWriter(
		ResultPartitionWriter backingResultPartitionWriter,
		Consumer<ResultPartitionID> onSetupAction,
		Consumer<ResultPartitionID> onFailAction,
		Consumer<ResultPartitionID> onFinishAction) {

		this.backingResultPartitionWriter = backingResultPartitionWriter;
		this.onSetupAction = onSetupAction;
		this.onFailAction = onFailAction;
		this.onFinishAction = onFinishAction;
	}

	@Override
	public void setup() throws IOException {
		backingResultPartitionWriter.setup();
		onSetupAction.accept(getPartitionId());
	}

	@Override
	public ResultPartitionID getPartitionId() {
		return backingResultPartitionWriter.getPartitionId();
	}

	@Override
	public int getNumberOfSubpartitions() {
		return backingResultPartitionWriter.getNumberOfSubpartitions();
	}

	@Override
	public int getNumTargetKeyGroups() {
		return backingResultPartitionWriter.getNumTargetKeyGroups();
	}

	@Override
	public BufferBuilder getBufferBuilder() throws IOException, InterruptedException {
		return backingResultPartitionWriter.getBufferBuilder();
	}

	@Override
	public boolean addBufferConsumer(BufferConsumer bufferConsumer, int subpartitionIndex) throws IOException {
		return backingResultPartitionWriter.addBufferConsumer(bufferConsumer, subpartitionIndex);
	}

	@Override
	public void flushAll() {
		backingResultPartitionWriter.flushAll();
	}

	@Override
	public void flush(int subpartitionIndex) {
		backingResultPartitionWriter.flush(subpartitionIndex);
	}

	@Override
	public void fail(@Nullable Throwable throwable) {
		backingResultPartitionWriter.fail(throwable);
		onFailAction.accept(getPartitionId());
	}

	@Override
	public void finish() throws IOException {
		backingResultPartitionWriter.finish();
		onFinishAction.accept(getPartitionId());
	}

	@Override
	public void close() throws Exception {
		backingResultPartitionWriter.close();
	}
}
