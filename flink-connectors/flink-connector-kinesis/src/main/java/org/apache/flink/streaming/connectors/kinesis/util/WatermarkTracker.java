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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.io.Closeable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * The watermark tracker is responsible for monitoring watermarks across distributed operators.
 * <p/>It can be used for sub tasks of a single Flink source as well as multiple heterogeneous
 * sources or other operators.
 * <p/>The class essentially functions like a distributed hash table that enclosing operators can
 * use to adopt their processing / IO rates.
 */
public abstract class WatermarkTracker implements Closeable, Serializable {

	public static final long DEFAULT_UPDATE_TIMEOUT_MILLIS = 60_000;

	/**
	 * Unique id for the subtask.
	 * Using string (instead of subtask index) so synchronization can spawn across multiple sources.
	 */
	private String subtaskId;

	/**
	 * Subtasks that have not provided a watermark update within the configured interval will be
	 * considered idle and excluded from target watermark calculation.
	 */
	private long updateTimeoutMillis = DEFAULT_UPDATE_TIMEOUT_MILLIS;

	private long updateTimeoutCount = 0;

	/** Watermark state. */
	protected static class WatermarkState {
		protected long watermark = Long.MIN_VALUE;
		protected long lastUpdated;

		public long getWatermark() {
			return watermark;
		}

		@Override
		public String toString() {
			return "WatermarkState{watermark=" + watermark + ", lastUpdated=" + lastUpdated + '}';
		}
	}

	/**
	 * The watermarks of all sub tasks that participate in the synchronization.
	 */
	private final Map<String, WatermarkState> watermarks;

	protected WatermarkTracker() {
		this.watermarks = new HashMap<>();
	}

	protected String getSubtaskId() {
		return this.subtaskId;
	}

	protected long getUpdateTimeoutMillis() {
		return this.updateTimeoutMillis;
	}

	/**
	 * Subtasks that have not provided a watermark update within the configured interval will be
	 * considered idle and excluded from target watermark calculation.
	 *
	 * @param updateTimeoutMillis
	 */
	public void setUpdateTimeoutMillis(long updateTimeoutMillis) {
		this.updateTimeoutMillis = updateTimeoutMillis;
	}

	/**
	 * Set the current watermark of the owning subtask and return the global low watermark based on
	 * the current state snapshot. Periodically called by the enclosing consumer instance, which is
	 * responsible for any timer management etc.
	 *
	 * @param localWatermark
	 * @return
	 */
	public long updateWatermarks(final long localWatermark) {
		refreshWatermarkSnapshot(this.watermarks);

		long currentTime = getCurrentTime();

		WatermarkState ws = watermarks.get(this.subtaskId);
		if (ws == null) {
			watermarks.put(this.subtaskId, ws = new WatermarkState());
		}
		ws.lastUpdated = currentTime;
		ws.watermark = Math.max(ws.watermark, localWatermark);
		saveWatermark(this.subtaskId, ws);

		long globalWatermark = ws.watermark;
		for (Map.Entry<String, WatermarkState> e : watermarks.entrySet()) {
			ws = e.getValue();
			if (ws.lastUpdated + updateTimeoutMillis < currentTime) {
				// ignore outdated subtask
				updateTimeoutCount++;
				continue;
			}
			globalWatermark = Math.min(ws.watermark, globalWatermark);
		}
		return globalWatermark;
	}

	protected long getCurrentTime() {
		return System.currentTimeMillis();
	}

	/**
	 * Fetch the watermarks from (remote) shared state.
	 */
	protected abstract void refreshWatermarkSnapshot(Map<String, WatermarkState> watermarks);

	/**
	 * Save the watermark to (remote) shared state.
	 *
	 * @param id
	 * @param ws
	 */
	protected abstract void saveWatermark(String id, WatermarkState ws);

	public void open(RuntimeContext context) {
		if (context instanceof StreamingRuntimeContext) {
			this.subtaskId = ((StreamingRuntimeContext) context).getOperatorUniqueID()
				+ "-" + context.getIndexOfThisSubtask();
		} else {
			this.subtaskId = context.getTaskNameWithSubtasks();
		}
	}

	@Override
	public void close() {
		// no work to do here
	}

	public long getUpdateTimeoutCount() {
		return updateTimeoutCount;
	}

}
