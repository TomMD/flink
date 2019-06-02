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

package org.apache.flink.table.runtime.watermarkassigner;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.table.dataformat.BaseRow;

/**
 * A stream operator that emits mini-batch marker in a given period.
 * NOTE: the mini-batch marker uses watermark instead
 */
public class MiniBatchAssignerOperator extends AbstractStreamOperator<BaseRow>
	implements OneInputStreamOperator<BaseRow, BaseRow>, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	private final long intervalMs;

	private transient long currentWatermark;

	public MiniBatchAssignerOperator(long intervalMs) {
		this.intervalMs = intervalMs;
		this.chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();

		currentWatermark = 0;

		long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + intervalMs, this);

		// report marker metric
		getRuntimeContext().getMetricGroup().gauge("currentBatch", (Gauge<Long>) () -> currentWatermark);
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		long now = getProcessingTimeService().getCurrentProcessingTime();
		long currentBatch = now - now % intervalMs;
		if (currentBatch > currentWatermark) {
			currentWatermark = currentBatch;
			// emit
			output.emitWatermark(new Watermark(currentBatch));
		}
		output.collect(element);
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		long now = getProcessingTimeService().getCurrentProcessingTime();
		long currentBatch = now - now % intervalMs;
		if (currentBatch > currentWatermark) {
			currentWatermark = currentBatch;
			// emit
			output.emitWatermark(new Watermark(currentBatch));
		}
		getProcessingTimeService().registerTimer(currentBatch + intervalMs, this);
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream (we rely only on the {@link AssignerWithPeriodicWatermarks} to emit
	 * watermarks from here).
	 */
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
			currentWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
	}
}
