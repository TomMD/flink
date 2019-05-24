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

import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/** Test for {@link WatermarkTracker}. */
public class WatermarkTrackerTest {

	WatermarkTracker.WatermarkState wm1 = new WatermarkTracker.WatermarkState();
	MutableLong clock = new MutableLong(0);

	private class TestWatermarkSynchronizer extends WatermarkTracker {

		@Override
		protected long getCurrentTime() {
			return clock.longValue();
		}

		@Override
		protected void refreshWatermarkSnapshot(Map<String, WatermarkState> watermarks) {
			watermarks.put("wm1", wm1);
		}

		@Override
		protected void saveWatermark(String id, WatermarkState ws) {
			// do nothing
		}
	}

	@Test
	public void test() {
		long watermark = 0;
		TestWatermarkSynchronizer ws = new TestWatermarkSynchronizer();
		ws.open(new MockStreamingRuntimeContext(false, 1, 0));
		Assert.assertEquals(Long.MIN_VALUE, ws.updateWatermarks(Long.MIN_VALUE));
		Assert.assertEquals(Long.MIN_VALUE, ws.updateWatermarks(watermark));
		// timeout wm1
		clock.add(WatermarkTracker.DEFAULT_UPDATE_TIMEOUT_MILLIS + 1);
		Assert.assertEquals(watermark, ws.updateWatermarks(watermark));
		Assert.assertEquals(watermark, ws.updateWatermarks(watermark - 1));

		// min watermark
		wm1.watermark = watermark + 1;
		wm1.lastUpdated = clock.longValue();
		Assert.assertEquals(watermark, ws.updateWatermarks(watermark));
		Assert.assertEquals(watermark + 1, ws.updateWatermarks(watermark + 1));
	}

}
