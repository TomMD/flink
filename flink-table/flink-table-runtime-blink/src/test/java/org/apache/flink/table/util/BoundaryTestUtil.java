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

package org.apache.flink.table.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.BinaryString;

import org.apache.commons.lang3.RandomStringUtils;

import static org.junit.Assert.assertEquals;

/**
 * Utils to generate specific length binary rows
 * to test the reading and writing on the boundary of some binary structures.
 */
public class BoundaryTestUtil {
	public static BinaryRow get24BytesBinaryRow() {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, BinaryString.fromString(RandomStringUtils.randomNumeric(2)));
		writer.writeString(1, BinaryString.fromString(RandomStringUtils.randomNumeric(2)));
		writer.complete();
		return row;
	}

	public static BinaryRow get160BytesBinaryRow() {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, BinaryString.fromString(RandomStringUtils.randomNumeric(72)));
		writer.writeString(1, BinaryString.fromString(RandomStringUtils.randomNumeric(64)));
		writer.complete();
		return row;
	}

	public static BinaryRow getVarSeg160BytesBinaryRow(BinaryRow row160) {
		BinaryRow varSegRow160 = new BinaryRow(2);
		MemorySegment[] segments = new MemorySegment[6];
		int baseOffset = 8;
		int posInSeg = baseOffset;
		int remainSize = 160;
		for (int i = 0; i < segments.length; i++) {
			segments[i] = MemorySegmentFactory.wrap(new byte[32]);
			int copy = Math.min(32 - posInSeg, remainSize);
			row160.getSegments()[0].copyTo(160 - remainSize, segments[i], posInSeg, copy);
			remainSize -= copy;
			posInSeg = 0;
		}
		varSegRow160.pointTo(segments, baseOffset, 160);
		assertEquals(row160, varSegRow160);
		return varSegRow160;
	}

	public static BinaryRow getVarSeg160BytesInOneSegRow(BinaryRow row160) {
		MemorySegment[] segments = new MemorySegment[2];
		segments[0] = row160.getSegments()[0];
		segments[1] = MemorySegmentFactory.wrap(new byte[row160.getSegments()[0].size()]);
		row160.pointTo(segments, 0, row160.getSizeInBytes());
		return row160;
	}
}
