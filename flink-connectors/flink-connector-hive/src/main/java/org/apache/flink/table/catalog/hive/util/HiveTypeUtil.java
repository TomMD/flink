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

package org.apache.flink.table.catalog.hive.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utils to convert data types between Flink and Hive.
 */
public class HiveTypeUtil {

	private HiveTypeUtil() {
	}

	/**
	 * Convert Flink data type to Hive data type name.
	 *
	 * @param type a Flink data type
	 * @return the corresponding Hive data type name
	 */
	public static String toHiveTypeName(DataType type) {
		checkNotNull(type, "type cannot be null");

		return toHiveTypeInfo(type).getTypeName();
	}

	/**
	 * Convert Flink data type to Hive data type.
	 *
	 * @param type a Flink data type
	 * @return the corresponding Hive data type
	 */
	public static TypeInfo toHiveTypeInfo(DataType type) {
		checkNotNull(type, "type cannot be null");

		if (type instanceof AtomicDataType) {
			if (type.equals(DataTypes.BOOLEAN())) {
				return TypeInfoFactory.booleanTypeInfo;
			} else if (type.equals(DataTypes.TINYINT())) {
				return TypeInfoFactory.byteTypeInfo;
			} else if (type.equals(DataTypes.SMALLINT())) {
				return TypeInfoFactory.shortTypeInfo;
			} else if (type.equals(DataTypes.INT())) {
				return TypeInfoFactory.intTypeInfo;
			} else if (type.equals(DataTypes.BIGINT())) {
				return TypeInfoFactory.longTypeInfo;
			} else if (type.equals(DataTypes.FLOAT())) {
				return TypeInfoFactory.floatTypeInfo;
			} else if (type.equals(DataTypes.DOUBLE())) {
				return TypeInfoFactory.doubleTypeInfo;
			} else if (type.equals(DataTypes.STRING())) {
				return TypeInfoFactory.stringTypeInfo;
			} else if (type.equals(DataTypes.DATE())) {
				return TypeInfoFactory.dateTypeInfo;
			} else if (type.equals(DataTypes.BYTES())) {
				return TypeInfoFactory.binaryTypeInfo;
			} else if (type.equals(DataTypes.TIMESTAMP())) {
				return TypeInfoFactory.timestampTypeInfo;
			} else {
				LogicalType logicalType = type.getLogicalType();
				Class clazz = logicalType.getClass();

				if (clazz == BinaryType.class || clazz == VarBinaryType.class) {
					// Hive doesn't support variable-length binary string
					return TypeInfoFactory.binaryTypeInfo;
				} else if (clazz == CharType.class) {
					CharType charType = (CharType) logicalType;

					if (charType.getLength() > HiveChar.MAX_CHAR_LENGTH) {
						throw new CatalogException(
							String.format("HiveCatalog doesn't support char type with length of '%d'. " +
									"The maximum length is %d",
								charType.getLength(), HiveChar.MAX_CHAR_LENGTH));
					}

					return TypeInfoFactory.getCharTypeInfo(charType.getLength());
				} else if (clazz == VarCharType.class) {
					VarCharType varCharType = (VarCharType) logicalType;

					if (varCharType.getLength() > HiveVarchar.MAX_VARCHAR_LENGTH) {
						throw new CatalogException(
							String.format("HiveCatalog doesn't support varchar type with length of '%d'. " +
									"The maximum length is %d",
								varCharType.getLength(), HiveVarchar.MAX_VARCHAR_LENGTH));
					}

					return TypeInfoFactory.getVarcharTypeInfo(varCharType.getLength());
				} else if (clazz == DecimalType.class) {
					DecimalType decimalType = (DecimalType) logicalType;

					// Flink and Hive share the same precision and scale range
					// Flink already validates the type so we don't need to validate again here
					return TypeInfoFactory.getDecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
				}

				// Flink's primitive types that Hive 2.3.4 doesn't support: Time, TIMESTAMP_WITH_LOCAL_TIME_ZONE
			}
		}

		if (type instanceof CollectionDataType) {

			Class clazz = type.getLogicalType().getClass();

			if (clazz == ArrayType.class) {
				DataType elementType = ((CollectionDataType) type).getElementDataType();

				return TypeInfoFactory.getListTypeInfo(toHiveTypeInfo(elementType));
			}

			// Flink's collection types that Hive 2.3.4 doesn't support: multiset
		}

		if (type instanceof KeyValueDataType) {
			KeyValueDataType keyValueDataType = (KeyValueDataType) type;
			DataType keyType = keyValueDataType.getKeyDataType();
			DataType valueType = keyValueDataType.getValueDataType();

			return TypeInfoFactory.getMapTypeInfo(toHiveTypeInfo(keyType), toHiveTypeInfo(valueType));
		}

		if (type instanceof FieldsDataType) {
			Map<String, DataType> fieldDataTypes = ((FieldsDataType) type).getFieldDataTypes();

			List<String> names = new ArrayList(fieldDataTypes.size());
			List<TypeInfo> typeInfos = new ArrayList<>(fieldDataTypes.size());

			for (Map.Entry<String, DataType> e : fieldDataTypes.entrySet()) {
				names.add(e.getKey());
				typeInfos.add(toHiveTypeInfo(e.getValue()));
			}

			return TypeInfoFactory.getStructTypeInfo(names, typeInfos);
		}

		throw new UnsupportedOperationException(
			String.format("Flink doesn't support converting type %s to Hive type yet.", type.toString()));
	}

	/**
	 * Convert Hive data type to a Flink data type.
	 *
	 * @param hiveType a Hive data type
	 * @return the corresponding Flink data type
	 */
	public static DataType toFlinkType(TypeInfo hiveType) {
		checkNotNull(hiveType, "hiveType cannot be null");

		switch (hiveType.getCategory()) {
			case PRIMITIVE:
				return toFlinkPrimitiveType((PrimitiveTypeInfo) hiveType);
			case LIST:
				ListTypeInfo listTypeInfo = (ListTypeInfo) hiveType;
				return DataTypes.ARRAY(toFlinkType(listTypeInfo.getListElementTypeInfo()));
			case MAP:
				MapTypeInfo mapTypeInfo = (MapTypeInfo) hiveType;
				return DataTypes.MAP(toFlinkType(mapTypeInfo.getMapKeyTypeInfo()), toFlinkType(mapTypeInfo.getMapValueTypeInfo()));
			case STRUCT:
				StructTypeInfo structTypeInfo = (StructTypeInfo) hiveType;

				List<String> names = structTypeInfo.getAllStructFieldNames();
				List<TypeInfo> typeInfos = structTypeInfo.getAllStructFieldTypeInfos();

				DataTypes.Field[] fields = new DataTypes.Field[names.size()];

				for (int i = 0; i < fields.length; i++) {
					fields[i] = DataTypes.FIELD(names.get(i), toFlinkType(typeInfos.get(i)));
				}

				return DataTypes.ROW(fields);
			default:
				throw new UnsupportedOperationException(
					String.format("Flink doesn't support Hive data type %s yet.", hiveType));
		}
	}

	private static DataType toFlinkPrimitiveType(PrimitiveTypeInfo hiveType) {
		checkNotNull(hiveType, "hiveType cannot be null");

		switch (hiveType.getPrimitiveCategory()) {
			case CHAR:
				return DataTypes.CHAR(((CharTypeInfo) hiveType).getLength());
			case VARCHAR:
				return DataTypes.VARCHAR(((VarcharTypeInfo) hiveType).getLength());
			case STRING:
				return DataTypes.STRING();
			case BOOLEAN:
				return DataTypes.BOOLEAN();
			case BYTE:
				return DataTypes.TINYINT();
			case SHORT:
				return DataTypes.SMALLINT();
			case INT:
				return DataTypes.INT();
			case LONG:
				return DataTypes.BIGINT();
			case FLOAT:
				return DataTypes.FLOAT();
			case DOUBLE:
				return DataTypes.DOUBLE();
			case DATE:
				return DataTypes.DATE();
			case TIMESTAMP:
				return DataTypes.TIMESTAMP();
			case BINARY:
				return DataTypes.BYTES();
			case DECIMAL:
				DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) hiveType;
				return DataTypes.DECIMAL(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
			default:
				throw new UnsupportedOperationException(
					String.format("Flink doesn't support Hive primitive type %s yet", hiveType));
		}
	}
}
