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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.strategies.CascadeTypeStrategy;
import org.apache.flink.table.types.inference.strategies.ExplicitTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MissingTypeStrategy;

import java.util.Arrays;

/**
 * Strategies for inferring an output or accumulator data type of a function call.
 *
 * @see TypeStrategy
 */
@Internal
public final class TypeStrategies {

	// --------------------------------------------------------------------------------------------
	// factory methods for type strategies
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a type strategy that return an explicitly given type.
	 */
	public static TypeStrategy explicit(DataType dataType) {
		return new ExplicitTypeStrategy(dataType);
	}

	/**
	 * Creates a type strategy that applies a strategy and then a sequence of transforms to its result.
	 */
	public static TypeStrategy cascade(TypeStrategy strategy, TypeTransformation... transformations) {
		return new CascadeTypeStrategy(strategy, Arrays.asList(transformations));
	}

	// --------------------------------------------------------------------------------------------
	// concrete, reusable type strategies
	// --------------------------------------------------------------------------------------------

	/**
	 * Placeholder for a missing type strategy.
	 */
	public static final TypeStrategy MISSING = new MissingTypeStrategy();

	/**
	 * An {@link #explicit(DataType)} type strategy that returns not null {@link DataTypes#BOOLEAN()}.
	 */
	public static final TypeStrategy BOOLEAN_NOT_NULL = explicit(DataTypes.BOOLEAN().notNull());

	// --------------------------------------------------------------------------------------------

	private TypeStrategies() {
		// no instantiation
	}
}
