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

package org.apache.flink.table.catalog.hive.factories;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_PROPERTOES_HIVE_METASTORE_URIS;
import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_TYPE_VALUE_HIVE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTIES;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;

/**
 * Catalog factory for {@link HiveCatalog}.
 */
public class HiveCatalogFactory implements CatalogFactory {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_HIVE); // hive
		context.put(CATALOG_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// default database
		properties.add(CATALOG_DEFAULT_DATABASE);

		properties.add(CATALOG_PROPERTIES);
		properties.add(CATALOG_PROPERTOES_HIVE_METASTORE_URIS);

		return properties;
	}

	@Override
	public Catalog createCatalog(String name, Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final Optional<String> defaultDatabase = descriptorProperties.getOptionalString(CATALOG_DEFAULT_DATABASE);

		// Note: More configs of HiveConf can be set here
		HiveConf hiveConf = HiveCatalog.getHiveConf(descriptorProperties.getString(CATALOG_PROPERTOES_HIVE_METASTORE_URIS));

		return createHiveCatalog(name, defaultDatabase.orElse(HiveCatalog.DEFAULT_DB), hiveConf);
	}

	@VisibleForTesting
	protected HiveCatalog createHiveCatalog(String name, String defaultDatabase, HiveConf hiveConf) {
		return new HiveCatalog(name, defaultDatabase, hiveConf);
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new HiveCatalogValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
