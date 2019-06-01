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

package org.apache.flink.table.catalog.hive.descriptors;

import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorTestBase;
import org.apache.flink.table.descriptors.DescriptorValidator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator.CATALOG_PROPERTOES_HIVE_METASTORE_URIS;

/**
 * Tests for {@link HiveCatalogDescriptor}.
 */
public class HiveCatalogDescriptorTest extends DescriptorTestBase {
	private static final String TEST_METASTORE_URIS = "thrift://test";

	@Override
	protected List<Descriptor> descriptors() {
		final Descriptor descriptor = new HiveCatalogDescriptor(TEST_METASTORE_URIS);

		return Arrays.asList(descriptor);
	}

	@Override
	protected List<Map<String, String>> properties() {
		final Map<String, String> props1 = new HashMap<>();
		props1.put("type", "hive");
		props1.put("property-version", "1");
		props1.put(CATALOG_PROPERTOES_HIVE_METASTORE_URIS, TEST_METASTORE_URIS);

		return Arrays.asList(props1);
	}

	@Override
	protected DescriptorValidator validator() {
		return new HiveCatalogValidator();
	}
}
