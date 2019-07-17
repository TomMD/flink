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

package org.apache.flink.table.dataformat;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.dataformat.BinaryRowTest.MyObj;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.typeutils.BaseArraySerializer;
import org.apache.flink.table.typeutils.BaseMapSerializer;
import org.apache.flink.table.typeutils.BaseRowSerializer;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for nestedRow, array, map.
 */
public class ComplexTest {

	@Test
	public void testNestedRow() {
		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter writer = new BinaryRowWriter(row);

		GenericTypeInfo<MyObj> info = new GenericTypeInfo<>(MyObj.class);
		TypeSerializer<MyObj> genericSerializer = info.createSerializer(new ExecutionConfig());
		GenericRow gRow = new GenericRow(5);
		gRow.setField(0, 1);
		gRow.setField(1, 5L);
		gRow.setField(2, BinaryString.fromString("12345678"));
		gRow.setField(3, null);
		gRow.setField(4, new BinaryGeneric<>(new MyObj(15, 5), genericSerializer));

		BaseRowSerializer serializer = new BaseRowSerializer(
			new LogicalType[]{
				DataTypes.INT().getLogicalType(),
				DataTypes.BIGINT().getLogicalType(),
				DataTypes.STRING().getLogicalType(),
				DataTypes.STRING().getLogicalType(),
				DataTypes.ANY(info).getLogicalType()
			},
			new TypeSerializer[]{
				IntSerializer.INSTANCE,
				LongSerializer.INSTANCE,
				StringSerializer.INSTANCE,
				StringSerializer.INSTANCE,
				genericSerializer
			});
		writer.writeRow(0, gRow, serializer);
		writer.complete();

		{
			BaseRow nestedRow = row.getRow(0, 5);
			assertEquals(nestedRow.getInt(0), 1);
			assertEquals(nestedRow.getLong(1), 5L);
			assertEquals(nestedRow.getString(2), BinaryString.fromString("12345678"));
			assertTrue(nestedRow.isNullAt(3));
			assertEquals(new MyObj(15, 5),
				BinaryGeneric.getJavaObjectFromBinaryGeneric(nestedRow.getGeneric(4), genericSerializer));
		}

		MemorySegment[] segments = splitBytes(row.getSegments()[0].getHeapMemory(), 3);
		row.pointTo(segments, 3, row.getSizeInBytes());
		{
			BaseRow nestedRow = row.getRow(0, 5);
			assertEquals(nestedRow.getInt(0), 1);
			assertEquals(nestedRow.getLong(1), 5L);
			assertEquals(nestedRow.getString(2), BinaryString.fromString("12345678"));
			assertTrue(nestedRow.isNullAt(3));
			assertEquals(new MyObj(15, 5),
				BinaryGeneric.getJavaObjectFromBinaryGeneric(nestedRow.getGeneric(4), genericSerializer));
		}
	}

	@Test
	public void testNestInNestedRow() {
		//1.layer1
		GenericRow gRow = new GenericRow(4);
		gRow.setField(0, 1);
		gRow.setField(1, 5L);
		gRow.setField(2, BinaryString.fromString("12345678"));
		gRow.setField(3, null);

		//2.layer2
		BaseRowSerializer serializer = new BaseRowSerializer(
			new LogicalType[]{
				DataTypes.INT().getLogicalType(),
				DataTypes.BIGINT().getLogicalType(),
				DataTypes.STRING().getLogicalType(),
				DataTypes.STRING().getLogicalType()
			},
			new TypeSerializer[]{
				IntSerializer.INSTANCE,
				LongSerializer.INSTANCE,
				StringSerializer.INSTANCE,
				StringSerializer.INSTANCE
			});
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, BinaryString.fromString("hahahahafff"));
		writer.writeRow(1, gRow, serializer);
		writer.complete();

		//3.layer3
		BinaryRow row2 = new BinaryRow(1);
		BinaryRowWriter writer2 = new BinaryRowWriter(row2);
		writer2.writeRow(0, row, null);
		writer2.complete();

		// verify
		{
			NestedRow nestedRow = (NestedRow) row2.getRow(0, 2);
			BinaryRow binaryRow = new BinaryRow(2);
			binaryRow.pointTo(nestedRow.getSegments(), nestedRow.getOffset(),
				nestedRow.getSizeInBytes());
			assertEquals(binaryRow, row);
		}

		assertEquals(row2.getRow(0, 2).getString(0), BinaryString.fromString("hahahahafff"));
		BaseRow nestedRow = row2.getRow(0, 2).getRow(1, 4);
		assertEquals(nestedRow.getInt(0), 1);
		assertEquals(nestedRow.getLong(1), 5L);
		assertEquals(nestedRow.getString(2), BinaryString.fromString("12345678"));
		assertTrue(nestedRow.isNullAt(3));
	}

	@Test
	public void testGenericArray() {
		// 1. array test
		Integer[] javaArray = {6, null, 666};
		GenericArray array = new GenericArray(javaArray, 3, false);

		assertEquals(array.getInt(0), 6);
		assertTrue(array.isNullAt(1));
		assertEquals(array.getInt(2), 666);

		// 2. test write array to binary row.
		BinaryRow row2 = new BinaryRow(1);
		BinaryRowWriter writer2 = new BinaryRowWriter(row2);
		BaseArraySerializer serializer = new BaseArraySerializer(
			DataTypes.INT().getLogicalType(), new ExecutionConfig());
		writer2.writeArray(0, array, serializer);
		writer2.complete();

		BaseArray array2 = row2.getArray(0);
		assertEquals(6, array2.getInt(0));
		assertTrue(array2.isNullAt(1));
		assertEquals(666, array2.getInt(2));
	}

	@Test
	public void testBinaryArray() {
		// 1. array test
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(
			array, 3, BinaryArray.calculateFixLengthPartSize(DataTypes.INT().getLogicalType()));

		writer.writeInt(0, 6);
		writer.setNullInt(1);
		writer.writeInt(2, 666);
		writer.complete();

		assertEquals(array.getInt(0), 6);
		assertTrue(array.isNullAt(1));
		assertEquals(array.getInt(2), 666);

		// 2. test write array to binary row.
		BinaryRow row2 = new BinaryRow(1);
		BinaryRowWriter writer2 = new BinaryRowWriter(row2);
		BaseArraySerializer serializer = new BaseArraySerializer(
			DataTypes.INT().getLogicalType(), new ExecutionConfig());
		writer2.writeArray(0, array, serializer);
		writer2.complete();

		BinaryArray array2 = (BinaryArray) row2.getArray(0);
		assertEquals(array, array2);
		assertEquals(6, array2.getInt(0));
		assertTrue(array2.isNullAt(1));
		assertEquals(666, array2.getInt(2));
	}

	@Test
	public void testGenericMap() {
		Map javaMap = new HashMap();
		javaMap.put(6, BinaryString.fromString("6"));
		javaMap.put(5, BinaryString.fromString("5"));
		javaMap.put(666, BinaryString.fromString("666"));
		javaMap.put(0, null);

		GenericMap genericMap = new GenericMap(javaMap);

		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter rowWriter = new BinaryRowWriter(row);
		BaseMapSerializer serializer = new BaseMapSerializer(
			DataTypes.INT().getLogicalType(),
			DataTypes.STRING().getLogicalType(),
			new ExecutionConfig());
		rowWriter.writeMap(0, genericMap, serializer);
		rowWriter.complete();

		Map map = row.getMap(0).toJavaMap(DataTypes.INT().getLogicalType(), DataTypes.STRING().getLogicalType());
		assertEquals(BinaryString.fromString("6"), map.get(6));
		assertEquals(BinaryString.fromString("5"), map.get(5));
		assertEquals(BinaryString.fromString("666"), map.get(666));
		assertTrue(map.containsKey(0));
		assertNull(map.get(0));
	}

	@Test
	public void testBinaryMap() {
		BinaryArray array1 = new BinaryArray();
		BinaryArrayWriter writer1 = new BinaryArrayWriter(
			array1, 4, BinaryArray.calculateFixLengthPartSize(DataTypes.INT().getLogicalType()));
		writer1.writeInt(0, 6);
		writer1.writeInt(1, 5);
		writer1.writeInt(2, 666);
		writer1.writeInt(3, 0);
		writer1.complete();

		BinaryArray array2 = new BinaryArray();
		BinaryArrayWriter writer2 = new BinaryArrayWriter(
			array2, 4, BinaryArray.calculateFixLengthPartSize(DataTypes.STRING().getLogicalType()));
		writer2.writeString(0, BinaryString.fromString("6"));
		writer2.writeString(1, BinaryString.fromString("5"));
		writer2.writeString(2, BinaryString.fromString("666"));
		writer2.setNullAt(3, DataTypes.STRING().getLogicalType());
		writer2.complete();

		BinaryMap binaryMap = BinaryMap.valueOf(array1, array2);

		BinaryRow row = new BinaryRow(1);
		BinaryRowWriter rowWriter = new BinaryRowWriter(row);
		BaseMapSerializer serializer = new BaseMapSerializer(
			DataTypes.STRING().getLogicalType(),
			DataTypes.INT().getLogicalType(),
			new ExecutionConfig());
		rowWriter.writeMap(0, binaryMap, serializer);
		rowWriter.complete();

		BinaryMap map = (BinaryMap) row.getMap(0);
		BinaryArray key = map.keyArray();
		BinaryArray value = map.valueArray();

		assertEquals(binaryMap, map);
		assertEquals(array1, key);
		assertEquals(array2, value);

		assertEquals(5, key.getInt(1));
		assertEquals(BinaryString.fromString("5"), value.getString(1));
		assertEquals(0, key.getInt(3));
		assertTrue(value.isNullAt(3));
	}

	public static MemorySegment[] splitBytes(byte[] bytes, int baseOffset) {
		int newSize = (bytes.length + 1) / 2 + baseOffset;
		MemorySegment[] ret = new MemorySegment[2];
		ret[0] = MemorySegmentFactory.wrap(new byte[newSize]);
		ret[1] = MemorySegmentFactory.wrap(new byte[newSize]);

		ret[0].put(baseOffset, bytes, 0, newSize - baseOffset);
		ret[1].put(0, bytes, newSize - baseOffset, bytes.length - (newSize - baseOffset));
		return ret;
	}

	@Test
	public void testToArray() {
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(
			array, 3, BinaryArray.calculateFixLengthPartSize(DataTypes.SMALLINT().getLogicalType()));
		writer.writeShort(0, (short) 5);
		writer.writeShort(1, (short) 10);
		writer.writeShort(2, (short) 15);
		writer.complete();

		short[] shorts = array.toShortArray();
		assertEquals(5, shorts[0]);
		assertEquals(10, shorts[1]);
		assertEquals(15, shorts[2]);

		MemorySegment[] segments = splitBytes(writer.segment.getHeapMemory(), 3);
		array.pointTo(segments, 3, array.getSizeInBytes());
		short[] shorts2 = array.toShortArray();
		assertEquals(5, shorts2[0]);
		assertEquals(10, shorts2[1]);
		assertEquals(15, shorts2[2]);
	}
}
