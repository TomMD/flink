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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.operators.util.TaskConfig;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link InputOutputFormatStub}.
 */
public class InputOutputFormatStubTest {

	@Test
	public void testInputOutputFormat() {
		InputOutputFormatStub formatStub = new InputOutputFormatStub();

		OperatorID operatorID1 = new OperatorID();
		formatStub.addInputFormat(operatorID1, new TestInputFormat("test input format"));
		formatStub.addParameters(operatorID1, "parameter1", "abc123");

		OperatorID operatorID2 = new OperatorID();
		formatStub.addOutputFormat(operatorID2, new DiscardingOutputFormat());
		formatStub.addParameters(operatorID2, "parameter1", "bcd234");

		OperatorID operatorID3 = new OperatorID();
		formatStub.addOutputFormat(operatorID3, new DiscardingOutputFormat());
		formatStub.addParameters(operatorID3, "parameter1", "cde345");

		TaskConfig taskConfig = new TaskConfig(new Configuration());
		formatStub.write(taskConfig);

		InputOutputFormatStub loadedStub = new InputOutputFormatStub(taskConfig, getClass().getClassLoader());

		Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> inputFormats = loadedStub.getInputFormats();
		Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats = loadedStub.getOutputFormats();
		assertEquals(1, inputFormats.size());
		assertEquals(2, outputFormats.size());

		// verify the input format
		TestInputFormat inputFormat = (TestInputFormat) inputFormats.get(operatorID1).getUserCodeObject();
		assertEquals("test input format", inputFormat.getName());

		Configuration inputFormatParams = loadedStub.getParameters(operatorID1);
		assertEquals(1, inputFormatParams.keySet().size());
		assertEquals("abc123", inputFormatParams.getString("parameter1", null));

		// verify the output formats
		assertTrue(outputFormats.get(operatorID2).getUserCodeObject() instanceof DiscardingOutputFormat);
		Configuration outputFormatParams1 = loadedStub.getParameters(operatorID2);
		assertEquals(1, outputFormatParams1.keySet().size());
		assertEquals("bcd234", outputFormatParams1.getString("parameter1", null));

		assertTrue(outputFormats.get(operatorID3).getUserCodeObject() instanceof DiscardingOutputFormat);
		Configuration outputFormatParams2 = loadedStub.getParameters(operatorID3);
		assertEquals(1, outputFormatParams2.keySet().size());
		assertEquals("cde345", outputFormatParams2.getString("parameter1", null));
	}

	@Test
	public void testOnlyInputFormat() {
		InputOutputFormatStub formatStub = new InputOutputFormatStub();

		OperatorID operatorID = new OperatorID();
		formatStub.addInputFormat(operatorID, new TestInputFormat("test input format"));
		formatStub.addParameters(operatorID, "parameter1", "abc123");

		TaskConfig taskConfig = new TaskConfig(new Configuration());
		formatStub.write(taskConfig);

		InputOutputFormatStub loadedStub = new InputOutputFormatStub(taskConfig, getClass().getClassLoader());

		Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> inputFormats = loadedStub.getInputFormats();
		assertEquals(1, inputFormats.size());
		assertEquals(0, loadedStub.getOutputFormats().size());

		TestInputFormat inputFormat = (TestInputFormat) inputFormats.get(operatorID).getUserCodeObject();
		assertEquals("test input format", inputFormat.getName());

		Configuration parameters = loadedStub.getParameters(operatorID);
		assertEquals(1, parameters.keySet().size());
		assertEquals("abc123", parameters.getString("parameter1", null));
	}

	@Test
	public void testOnlyOutputFormat() {
		InputOutputFormatStub formatStub = new InputOutputFormatStub();

		OperatorID operatorID = new OperatorID();
		formatStub.addOutputFormat(operatorID, new DiscardingOutputFormat<>());

		Configuration parameters = new Configuration();
		parameters.setString("parameter1", "bcd234");
		formatStub.addParameters(operatorID, parameters);

		TaskConfig taskConfig = new TaskConfig(new Configuration());
		formatStub.write(taskConfig);

		InputOutputFormatStub loadedStub = new InputOutputFormatStub(taskConfig, getClass().getClassLoader());

		Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats = loadedStub.getOutputFormats();
		assertEquals(1, outputFormats.size());
		assertEquals(0, loadedStub.getInputFormats().size());

		assertTrue(outputFormats.get(operatorID).getUserCodeObject() instanceof DiscardingOutputFormat);

		Configuration loadedParameters = loadedStub.getParameters(operatorID);
		assertEquals(1, loadedParameters.keySet().size());
		assertEquals("bcd234", loadedParameters.getString("parameter1", null));
	}

	// -------------------------------------------------------------------------
	//                          Utilities
	// -------------------------------------------------------------------------

	private static final class TestInputFormat extends GenericInputFormat<Object> {

		private final String name;

		TestInputFormat(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		@Override
		public boolean reachedEnd()  {
			return true;
		}

		@Override
		public Object nextRecord(Object reuse) {
			return null;
		}

		@Override
		public GenericInputSplit[] createInputSplits(int numSplits) {
			return null;
		}
	}
}
