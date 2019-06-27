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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunctionDefinition;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedAggregateFunction;
import org.apache.flink.table.functions.UserFunctionsTypeHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Simple function catalog to store {@link FunctionDefinition}s in catalogs.
 */
@Internal
public class FunctionCatalog implements FunctionLookup {

	private final CatalogManager catalogManager;

	public FunctionCatalog(CatalogManager catalogManager) {
		this.catalogManager = catalogManager;
	}

	public void registerScalarFunction(String name, ScalarFunction function) {
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());
		registerFunction(
			name,
			new ScalarFunctionDefinition(name, function)
		);
	}

	public <T> void registerTableFunction(
			String name,
			TableFunction<T> function,
			TypeInformation<T> resultType) {
		// check if class not Scala object
		UserFunctionsTypeHelper.validateNotSingleton(function.getClass());
		// check if class could be instantiated
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());

		registerFunction(
			name,
			new TableFunctionDefinition(
				name,
				function,
				resultType)
		);
	}

	public <T, ACC> void registerAggregateFunction(
			String name,
			UserDefinedAggregateFunction<T, ACC> function,
			TypeInformation<T> resultType,
			TypeInformation<ACC> accType) {
		// check if class not Scala object
		UserFunctionsTypeHelper.validateNotSingleton(function.getClass());
		// check if class could be instantiated
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());

		final FunctionDefinition definition;
		if (function instanceof AggregateFunction) {
			definition = new AggregateFunctionDefinition(
				name,
				(AggregateFunction<?, ?>) function,
				resultType,
				accType);
		} else if (function instanceof TableAggregateFunction) {
			definition = new TableAggregateFunctionDefinition(
				name,
				(TableAggregateFunction<?, ?>) function,
				resultType,
				accType);
		} else {
			throw new TableException("Unknown function class: " + function.getClass());
		}

		registerFunction(
			name,
			definition
		);
	}

	public String[] getUserDefinedFunctions() {
		List<String> result = new ArrayList<>();

		// Get functions in catalog
		Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();
		try {
			result.addAll(catalog.listFunctions(catalogManager.getCurrentDatabase()));
		} catch (DatabaseNotExistException e) {
			// Ignore since there will always be a current database of the current catalog
		}

		// Get built-in functions
		result.addAll(
			BuiltInFunctionDefinitions.getDefinitions()
				.stream()
				.map(f -> f.getName())
				.collect(Collectors.toList()));

		return result.stream()
			.map(n -> normalizeName(n))
			.collect(Collectors.toList())
			.toArray(new String[0]);
	}

	@Override
	public Optional<FunctionLookup.Result> lookupFunction(String name) {
		String functionName = normalizeName(name);

		Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();

		FunctionDefinition userCandidate = null;
		try {
			CatalogFunction catalogFunction = catalog.getFunction(
				new ObjectPath(catalogManager.getCurrentDatabase(), functionName));

			// TODO: use FunctionDefintionFactory to initiate a FunctionDefinition from CatalogFunction
			// 		it should also be able to initiate wrappers for Hive functions
		} catch (FunctionNotExistException e) {
			// Ignore
		}

		final Optional<FunctionDefinition> foundDefinition;

		if (userCandidate != null) {
			foundDefinition = Optional.of(userCandidate);
		} else {

			// TODO once we connect this class with the Catalog APIs we need to make sure that
			//  built-in functions are present in "root" built-in catalog. This allows to
			//  overwrite built-in functions but also fallback to the "root" catalog. It should be
			//  possible to disable the "root" catalog if that is desired.

			foundDefinition = BuiltInFunctionDefinitions.getDefinitions()
				.stream()
				.filter(f -> functionName.equals(normalizeName(f.getName())))
				.findFirst()
				.map(Function.identity());
		}

		return foundDefinition.map(definition -> new FunctionLookup.Result(
			ObjectIdentifier.of(catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase(), name),
			definition)
		);
	}

	private static Object initiate(ClassLoader classLoader, String functionIdentifier) {
		try {
			return classLoader.loadClass(functionIdentifier).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			throw new TableException(
				String.format("Failed to initiate an instance of class %s.", functionIdentifier), e);
		}
	}

	private void registerFunction(String name, FunctionDefinition functionDefinition) {

		if (functionDefinition instanceof BuiltInFunctionDefinition) {
			throw new TableException("Cannot register a built-in Flink function");
		} else {
			Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();

			String functionIdentifier;

			if (functionDefinition.getKind() == FunctionKind.SCALAR) {
				functionIdentifier = ((ScalarFunctionDefinition) functionDefinition).getScalarFunction().functionIdentifier();
			} else if (functionDefinition.getKind() == FunctionKind.TABLE) {
				functionIdentifier = ((TableFunctionDefinition) functionDefinition).getTableFunction().functionIdentifier();
			} else if (functionDefinition.getKind() == FunctionKind.AGGREGATE) {
				functionIdentifier = ((AggregateFunctionDefinition) functionDefinition).getAggregateFunction().functionIdentifier();
			} else {
				throw new TableException(
					String.format("FunctionCatalog doesn't support registering functions of %s yet.", functionDefinition.getKind()));
			}

			try {
				catalog.createFunction(
					new ObjectPath(catalogManager.getCurrentDatabase(), normalizeName(name)),
					new CatalogFunctionImpl(
						functionIdentifier,
						// Note the property map cannot be stored in Hive Metastore, so don't put any more properties in it
						new HashMap<String, String>() {{
							put(CatalogConfig.IS_GENERIC, String.valueOf(true));
						}}
					),
					false
				);
			} catch (FunctionAlreadyExistException e) {
				throw new TableException(
					String.format("Function %s already existing in catalog %s database %s.",
						catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase()));
			} catch (DatabaseNotExistException e) {
				// Ignore since there will always be a current database of the current catalog
			}
		}
	}

	private String normalizeName(String name) {
		return name.toUpperCase();
	}
}
