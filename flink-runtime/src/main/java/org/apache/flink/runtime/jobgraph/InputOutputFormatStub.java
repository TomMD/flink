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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility class for {@link InputFormat} and {@link OutputFormat} stubs.
 */
public class InputOutputFormatStub {

	private final FormatUserCodeTable formats;

	private final Configuration parameters;

	private final ClassLoader userCodeClassLoader;

	public InputOutputFormatStub() {
		this.formats = new FormatUserCodeTable();
		this.parameters = new Configuration();
		this.userCodeClassLoader = Thread.currentThread().getContextClassLoader();
	}

	public InputOutputFormatStub(TaskConfig config, ClassLoader classLoader) {
		checkNotNull(config);
		this.userCodeClassLoader = checkNotNull(classLoader);

		final UserCodeWrapper<FormatUserCodeTable> wrapper;

		try {
			wrapper = config.getStubWrapper(classLoader);
		} catch (Throwable t) {
			throw new RuntimeException("Deserializing the input/output formats failed: " + t.getMessage(), t);
		}

		if (wrapper == null) {
			throw new RuntimeException("No InputFormat or OutputFormat present in task configuration.");
		}

		try {
			this.formats = wrapper.getUserCodeObject(FormatUserCodeTable.class, classLoader);
		} catch (Throwable t) {
			throw new RuntimeException("Instantiating the input/output formats failed: " + t.getMessage(), t);
		}

		this.parameters = new Configuration();
		Configuration stubParameters = config.getStubParameters();
		for (String key : stubParameters.keySet()) { // copy only the parameters of input/output formats
			parameters.setString(key, stubParameters.getString(key, null));
		}
	}

	public Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> getInputFormats() {
		return formats.getInputFormats();
	}

	public Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> getOutputFormats() {
		return formats.getOutputFormats();
	}

	@SuppressWarnings("unchecked")
	public <OT, T extends InputSplit> InputFormat<OT, T> getUniqueInputFormat() {
		Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> inputFormats = formats.getInputFormats();
		Preconditions.checkState(inputFormats.size() == 1);

		return (InputFormat<OT, T>) inputFormats
			.values()
			.iterator()
			.next()
			.getUserCodeObject(InputFormat.class, userCodeClassLoader);
	}

	@SuppressWarnings("unchecked")
	public <IT> OutputFormat<IT> getUniqueOutputFormat() {
		Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats = formats.getOutputFormats();
		Preconditions.checkState(outputFormats.size() == 1);

		return (OutputFormat<IT>) outputFormats
			.values()
			.iterator()
			.next()
			.getUserCodeObject(OutputFormat.class, userCodeClassLoader);
	}

	public InputOutputFormatStub addInputFormat(OperatorID operatorId, InputFormat<?, ?> inputFormat) {
		formats.addInputFormat(operatorId, new UserCodeObjectWrapper<>(inputFormat));
		return this;
	}

	public InputOutputFormatStub addInputFormat(OperatorID operatorId, UserCodeWrapper<? extends InputFormat<?, ?>> wrapper) {
		formats.addInputFormat(operatorId, wrapper);
		return this;
	}

	public InputOutputFormatStub addOutputFormat(OperatorID operatorId, OutputFormat<?> outputFormat) {
		formats.addOutputFormat(operatorId, new UserCodeObjectWrapper<>(outputFormat));
		return this;
	}

	public InputOutputFormatStub addOutputFormat(OperatorID operatorId, UserCodeWrapper<? extends OutputFormat<?>> wrapper) {
		formats.addOutputFormat(operatorId, wrapper);
		return this;
	}

	public Configuration getParameters(OperatorID operatorId) {
		return new DelegatingConfiguration(parameters, getParamKeyPrefix(operatorId));
	}

	public InputOutputFormatStub addParameters(OperatorID operatorId, Configuration parameters) {
		for (String key : parameters.keySet()) {
			addParameters(operatorId, key, parameters.getString(key, null));
		}
		return this;
	}

	public InputOutputFormatStub addParameters(OperatorID operatorId, String key, String value) {
		parameters.setString(getParamKeyPrefix(operatorId) + key, value);
		return this;
	}

	public void write(TaskConfig config) {
		config.setStubWrapper(new UserCodeObjectWrapper<>(formats));
		config.setStubParameters(parameters);
	}

	private String getParamKeyPrefix(OperatorID operatorId) {
		return operatorId + ".";
	}

	/**
	 * Container for multiple wrappers containing {@link InputFormat} and {@link OutputFormat} code.
	 */
	public static class FormatUserCodeTable implements Serializable {

		private static final long serialVersionUID = 1L;

		private final Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> inputFormats;
		private final Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats;

		public FormatUserCodeTable() {
			this.inputFormats = new HashMap<>();
			this.outputFormats = new HashMap<>();
		}

		public void addInputFormat(OperatorID operatorId, UserCodeWrapper<? extends InputFormat<?, ?>> wrapper) {
			if (inputFormats.containsKey(checkNotNull(operatorId))) {
				throw new IllegalStateException("The input format has been set for the operator: " + operatorId);
			}

			inputFormats.put(operatorId, checkNotNull(wrapper));
		}

		public void addOutputFormat(OperatorID operatorId, UserCodeWrapper<? extends OutputFormat<?>> wrapper) {
			if (outputFormats.containsKey(checkNotNull(operatorId))) {
				throw new IllegalStateException("The output format has been set for the operator: " + operatorId);
			}

			outputFormats.put(operatorId, checkNotNull(wrapper));
		}

		public Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> getInputFormats() {
			return Collections.unmodifiableMap(inputFormats);
		}

		public Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> getOutputFormats() {
			return Collections.unmodifiableMap(outputFormats);
		}
	}
}
