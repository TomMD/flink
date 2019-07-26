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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Base test class for JDBC Input and Output formats.
 */
public class JDBCTestBase {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	public static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver";
	public static final String DB_URL = "jdbc:derby:memory:ebookshop";
	public static final String INPUT_TABLE = "books";
	public static final String OUTPUT_TABLE = "newbooks";
	public static final String OUTPUT_TABLE_2 = "newbooks2";
	public static final String SELECT_ALL_BOOKS = "select * from " + INPUT_TABLE;
	public static final String SELECT_ALL_NEWBOOKS = "select * from " + OUTPUT_TABLE;
	public static final String SELECT_ALL_NEWBOOKS_2 = "select * from " + OUTPUT_TABLE_2;
	public static final String SELECT_EMPTY = "select * from books WHERE QTY < 0";
	public static final String INSERT_TEMPLATE =
		"insert into %s (" +
			"id, title, author, price, qty, print_date, print_time, print_timestamp) values (?,?,?,?,?,?,?,?)";
	public static final String SELECT_ALL_BOOKS_SPLIT_BY_ID = SELECT_ALL_BOOKS + " WHERE id BETWEEN ? AND ?";
	public static final String SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR = SELECT_ALL_BOOKS + " WHERE author = ?";
	public static final String SELECT_ALL_BOOKS_SPLIT_BY_TIMESTAMP =
		SELECT_ALL_BOOKS + " WHERE print_timestamp BETWEEN ? AND ?";

	public static final TestEntry[] TEST_DATA = {
			new TestEntry(
				1001, ("Java public for dummies"), ("Tan Ah Teck"), 11.11, 11,
				LocalDate.of(2011, 1, 11), LocalTime.of(1, 1, 11),
				LocalDateTime.of(2011, 1, 11, 1, 1, 11)),
			new TestEntry(
				1002, ("More Java for dummies"), ("Tan Ah Teck"), 22.22, 22,
				LocalDate.of(2012, 2, 12), LocalTime.of(2, 2, 12),
				LocalDateTime.of(2012, 2, 12, 2, 2, 12)),
			new TestEntry(
				1003, ("More Java for more dummies"), ("Mohammad Ali"), 33.33, 33,
				LocalDate.of(2013, 3, 13), LocalTime.of(3, 3, 13),
				LocalDateTime.of(2013, 3, 13, 3, 3, 13)),
			new TestEntry(
				1004, ("A Cup of Java"), ("Kumar"), 44.44, 44,
				LocalDate.of(2014, 4, 14), LocalTime.of(4, 4, 14),
				LocalDateTime.of(2014, 4, 14, 4, 4, 14)),
			new TestEntry(
				1005, ("A Teaspoon of Java"), ("Kevin Jones"), 55.55, 55,
				LocalDate.of(2015, 5, 15), LocalTime.of(5, 5, 15),
				LocalDateTime.of(2015, 5, 15, 5, 5, 15)),
			new TestEntry(
				1006, ("A Teaspoon of Java 1.4"), ("Kevin Jones"), 66.66, 66,
				LocalDate.of(2016, 6, 16), LocalTime.of(6, 6, 16),
				LocalDateTime.of(2016, 6, 16, 6, 6, 16)),
			new TestEntry(
				1007, ("A Teaspoon of Java 1.5"), ("Kevin Jones"), 77.77, 77,
				LocalDate.of(2017, 7, 17), LocalTime.of(7, 7, 17),
				LocalDateTime.of(2017, 7, 17, 7, 7, 17)),
			new TestEntry(
				1008, ("A Teaspoon of Java 1.6"), ("Kevin Jones"), 88.88, 88,
				LocalDate.of(2018, 8, 18), LocalTime.of(8, 8, 18),
				LocalDateTime.of(2018, 8, 18, 8, 8, 18)),
			new TestEntry(
				1009, ("A Teaspoon of Java 1.7"), ("Kevin Jones"), 99.99, 99,
				LocalDate.of(2019, 9, 19), LocalTime.of(9, 9, 19),
				LocalDateTime.of(2019, 9, 19, 9, 9, 19)),
			new TestEntry(
				1010, ("A Teaspoon of Java 1.8"), ("Kevin Jones"), null, 1010,
				LocalDate.of(2020, 10, 20), LocalTime.of(10, 10, 20),
				LocalDateTime.of(2020, 10, 20, 10, 10, 20))
	};

	static class TestEntry {
		final Integer id;
		final String title;
		final String author;
		final Double price;
		final Integer qty;
		final LocalDate printDate;
		final LocalTime printTime;
		final LocalDateTime printTimestamp;

		private TestEntry(
				Integer id, String title, String author, Double price, Integer qty,
				LocalDate printDate, LocalTime printTime, LocalDateTime printTimestamp) {
			this.id = id;
			this.title = title;
			this.author = author;
			this.price = price;
			this.qty = qty;
			this.printDate = printDate;
			this.printTime = printTime;
			this.printTimestamp = printTimestamp;
		}
	}

	public static final RowTypeInfo ROW_TYPE_INFO = new RowTypeInfo(
		BasicTypeInfo.INT_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.DOUBLE_TYPE_INFO,
		BasicTypeInfo.INT_TYPE_INFO,
		LocalTimeTypeInfo.LOCAL_DATE,
		LocalTimeTypeInfo.LOCAL_TIME,
		LocalTimeTypeInfo.LOCAL_DATE_TIME);

	public static final int[] SQL_TYPES = new int[] {
		Types.INTEGER,
		Types.VARCHAR,
		Types.VARCHAR,
		Types.DOUBLE,
		Types.INTEGER,
		Types.DATE,
		Types.TIME,
		Types.TIMESTAMP};

	public static String getCreateQuery(String tableName) {
		return "CREATE TABLE " +
			tableName + " (" +
			"id INT NOT NULL DEFAULT 0," +
			"title VARCHAR(50) DEFAULT NULL," +
			"author VARCHAR(50) DEFAULT NULL," +
			"price FLOAT DEFAULT NULL," +
			"qty INT DEFAULT NULL," +
			"print_date DATE DEFAULT NULL," +
			"print_time TIME DEFAULT NULL," +
			"print_timestamp TIMESTAMP DEFAULT NULL," +
			"PRIMARY KEY (id))";
	}

	public static String getInsertQuery() {
		StringBuilder sqlQueryBuilder = new StringBuilder(
			"INSERT INTO books (id, title, author, price, qty, print_date, print_time, print_timestamp) VALUES ");
		for (int i = 0; i < TEST_DATA.length; i++) {
			sqlQueryBuilder.append("(")
			.append(TEST_DATA[i].id).append(",'")
			.append(TEST_DATA[i].title).append("','")
			.append(TEST_DATA[i].author).append("',")
			.append(TEST_DATA[i].price).append(",")
			.append(TEST_DATA[i].qty).append(",")
			.append("'").append(TEST_DATA[i].printDate).append("',")
			.append("'").append(TEST_DATA[i].printTime).append("',")
			.append("'").append(
				TEST_DATA[i].printTimestamp.toString().replace('T', ' ')).append("')");
			if (i < TEST_DATA.length - 1) {
				sqlQueryBuilder.append(",");
			}
		}
		return sqlQueryBuilder.toString();
	}

	public static final OutputStream DEV_NULL = new OutputStream() {
		@Override
		public void write(int b) {
		}
	};

	@BeforeClass
	public static void prepareDerbyDatabase() throws Exception {
		System.setProperty("derby.stream.error.field", JDBCTestBase.class.getCanonicalName() + ".DEV_NULL");

		Class.forName(DRIVER_CLASS);
		try (Connection conn = DriverManager.getConnection(DB_URL + ";create=true")) {
			createTable(conn, JDBCTestBase.INPUT_TABLE);
			createTable(conn, OUTPUT_TABLE);
			createTable(conn, OUTPUT_TABLE_2);
			insertDataIntoInputTable(conn);
		}
	}

	private static void createTable(Connection conn, String tableName) throws SQLException {
		Statement stat = conn.createStatement();
		stat.executeUpdate(getCreateQuery(tableName));
		stat.close();
	}

	private static void insertDataIntoInputTable(Connection conn) throws SQLException {
		Statement stat = conn.createStatement();
		stat.execute(getInsertQuery());
		stat.close();
	}

	@AfterClass
	public static void cleanUpDerbyDatabases() throws Exception {
		Class.forName(DRIVER_CLASS);
		try (
			Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
			Statement stat = conn.createStatement()) {

			stat.executeUpdate("DROP TABLE " + INPUT_TABLE);
			stat.executeUpdate("DROP TABLE " + OUTPUT_TABLE);
			stat.executeUpdate("DROP TABLE " + OUTPUT_TABLE_2);
		}
	}
}
