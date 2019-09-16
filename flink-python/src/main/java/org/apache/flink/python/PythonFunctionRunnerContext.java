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

package org.apache.flink.python;

import org.apache.flink.annotation.Internal;

import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;

/**
 * The context used to manage the Python execution environment. It can be used to create
 * the {@link StageBundleFactory} which is the entry point to manage the Python execution environment.
 */
@Internal
public interface PythonFunctionRunnerContext extends AutoCloseable {

	/**
	 * Gets the bundle factory for the specified {@link ExecutableStage} which has all of the resources
	 * it needs to provide new {@link RemoteBundle}s.
	 */
	StageBundleFactory getStageBundleFactory(ExecutableStage executableStage);
}
