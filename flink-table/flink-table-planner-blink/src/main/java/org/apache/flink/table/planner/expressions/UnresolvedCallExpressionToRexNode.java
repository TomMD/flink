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

package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.resolver.rules.ResolveCallByArgumentsRule;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.typeLiteral;

/**
 * Convert {@link UnresolvedCallExpression} to calcite {@link RexNode}, inputs should only contains
 * call unresolved expression, other expression should have been resolved.
 */
public class UnresolvedCallExpressionToRexNode {

	public static RexNode toRexNode(
		RelBuilder relBuilder, Expression expressions) {
		return resolveWithoutCatalog(Collections.singletonList(expressions)).get(0)
			.accept(new RexNodeConverter(relBuilder));
	}

	public static List<ResolvedExpression> resolveWithoutCatalog(List<Expression> expressions) {
		return ResolveCallByArgumentsRule.resolve(expressions,
			new ResolveCallByArgumentsRule.PostResolveCallByArguments() {
				@Override
				public CallExpression cast(ResolvedExpression expression, DataType dataType) {
					BuiltInFunctionDefinition cast = BuiltInFunctionDefinitions.CAST;
					return new CallExpression(
						cast,
						Arrays.asList(expression, typeLiteral(dataType)),
						dataType);
				}

				@Override
				public CallExpression get(ResolvedExpression composite, ValueLiteralExpression key,
					DataType dataType) {
					BuiltInFunctionDefinition get = BuiltInFunctionDefinitions.GET;
					return new CallExpression(
						get,
						Arrays.asList(composite, key),
						dataType);
				}
			},
			PlannerTypeInferenceUtilImpl.INSTANCE);
	}
}
