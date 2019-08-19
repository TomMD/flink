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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.AvailabilityListener;

import java.io.Closeable;

/**
 * Interface for processing records by {@link org.apache.flink.streaming.runtime.tasks.StreamTask}.
 */
@Internal
public interface StreamInputProcessor extends AvailabilityListener, Closeable {
	/**
	 * @return true if {@link StreamInputProcessor} estimates that more records can be processed
	 * immediately. Otherwise false, which means that there are no more records available at the
	 * moment and the caller should check {@link #isFinished()} and/or {@link #isAvailable()}.
	 */
	InputStatus processInput() throws Exception;
}
