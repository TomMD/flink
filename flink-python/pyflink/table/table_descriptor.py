################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import sys
from abc import ABCMeta

from py4j.java_gateway import get_method
from pyflink.table.types import _to_java_type

from pyflink.java_gateway import get_gateway

if sys.version >= '3':
    unicode = str

__all__ = [
    'Rowtime',
    'Schema',
    'OldCsv',
    'Csv',
    'Avro',
    'Json',
    'FileSystem'
]


class Descriptor(object):
    """
    Base class of the descriptors that adds a set of string-based, normalized properties for
    describing DDL information.

    Typical characteristics of a descriptor are:
    - descriptors have a default constructor
    - descriptors themselves contain very little logic
    - corresponding validators validate the correctness (goal: have a single point of validation)

    A descriptor is similar to a builder in a builder pattern, thus, mutable for building
    properties.
    """

    __metaclass__ = ABCMeta

    def __init__(self, j_descriptor):
        self._j_descriptor = j_descriptor

    def to_properties(self):
        """
        Converts this descriptor into a dict of properties.

        :return: Dict object contains all of current properties.
        """
        return dict(self._j_descriptor.toProperties())


class Rowtime(Descriptor):
    """
    Rowtime descriptor for describing an event time attribute in the schema.
    """

    def __init__(self):
        gateway = get_gateway()
        self._j_rowtime = gateway.jvm.Rowtime()
        super(Rowtime, self).__init__(self._j_rowtime)

    def timestamps_from_field(self, field_name):
        """
        Sets a built-in timestamp extractor that converts an existing LONG or TIMESTAMP field into
        the rowtime attribute.

        :param field_name: The field to convert into a rowtime attribute.
        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.timestampsFromField(field_name)
        return self

    def timestamps_from_source(self):
        """
        Sets a built-in timestamp extractor that converts the assigned timestamps from a DataStream
        API record into the rowtime attribute and thus preserves the assigned timestamps from the
        source.

        ..note::
            This extractor only works in streaming environments.

        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.timestampsFromSource()
        return self

    def timestamps_from_extractor(self, extractor):
        """
        Sets a custom timestamp extractor to be used for the rowtime attribute.

        :param extractor: The java fully-qualified class name of the TimestampExtractor to extract
                          the rowtime attribute from the physical type. The TimestampExtractor must
                          have a public no-argument constructor and can be founded by
                          in current Java classloader.
        :return: This rowtime descriptor.
        """
        gateway = get_gateway()
        self._j_rowtime = self._j_rowtime.timestampsFromExtractor(
            gateway.jvm.Thread.currentThread().getContextClassLoader().loadClass(extractor)
                   .newInstance())
        return self

    def watermarks_periodic_ascending(self):
        """
        Sets a built-in watermark strategy for ascending rowtime attributes.

        Emits a watermark of the maximum observed timestamp so far minus 1. Rows that have a
        timestamp equal to the max timestamp are not late.

        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.watermarksPeriodicAscending()
        return self

    def watermarks_periodic_bounded(self, delay):
        """
        Sets a built-in watermark strategy for rowtime attributes which are out-of-order by a
        bounded time interval.

        Emits watermarks which are the maximum observed timestamp minus the specified delay.

        :param delay: Delay in milliseconds.
        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.watermarksPeriodicBounded(delay)
        return self

    def watermarks_from_source(self):
        """
        Sets a built-in watermark strategy which indicates the watermarks should be preserved from
        the underlying DataStream API and thus preserves the assigned watermarks from the source.

        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.watermarksFromSource()
        return self

    def watermarks_from_strategy(self, strategy):
        """
        Sets a custom watermark strategy to be used for the rowtime attribute.

        :param strategy: The java fully-qualified class name of the WatermarkStrategy. The
                         WatermarkStrategy must have a public no-argument constructor and can be
                         founded by in current Java classloader.
        :return: This rowtime descriptor.
        """
        gateway = get_gateway()
        self._j_rowtime = self._j_rowtime.watermarksFromStrategy(
            gateway.jvm.Thread.currentThread().getContextClassLoader().loadClass(strategy)
                   .newInstance())
        return self


class Schema(Descriptor):
    """
    Describes a schema of a table.

    ..note::
        Field names are matched by the exact name by default (case sensitive).
    """

    def __init__(self):
        gateway = get_gateway()
        self._j_schema = gateway.jvm.Schema()
        super(Schema, self).__init__(self._j_schema)

    def field(self, field_name, field_type):
        """
        Adds a field with the field name and the data type or type string. Required.
        This method can be called multiple times. The call order of this method defines
        also the order of the fields in a row. Here is a document that introduces the type strings:
        https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connect.html#type-strings

        :param field_name: The field name.
        :param field_type: The data type or type string of the field.
        :return: This schema object.
        """
        if isinstance(field_type, (str, unicode)):
            self._j_schema = self._j_schema.field(field_name, field_type)
        else:
            self._j_schema = self._j_schema.field(field_name, _to_java_type(field_type))
        return self

    def from_origin_field(self, origin_field_name):
        """
        Specifies the origin of the previously defined field. The origin field is defined by a
        connector or format.

        E.g. field("myString", Types.STRING).from_origin_field("CSV_MY_STRING")

        ..note::
            Field names are matched by the exact name by default (case sensitive).

        :param origin_field_name: The origin field name.
        :return: This schema object.
        """
        self._j_schema = get_method(self._j_schema, "from")(origin_field_name)
        return self

    def proctime(self):
        """
        Specifies the previously defined field as a processing-time attribute.

        E.g. field("proctime", Types.SQL_TIMESTAMP).proctime()

        :return: This schema object.
        """
        self._j_schema = self._j_schema.proctime()
        return self

    def rowtime(self, rowtime):
        """
        Specifies the previously defined field as an event-time attribute.

        E.g. field("rowtime", Types.SQL_TIMESTAMP).rowtime(...)

        :param rowtime: A :class:`RowTime`.
        :return: This schema object.
        """
        self._j_schema = self._j_schema.rowtime(rowtime._j_rowtime)
        return self


class FormatDescriptor(Descriptor):
    """
    Describes the format of data.
    """

    __metaclass__ = ABCMeta

    def __init__(self, j_format_descriptor):
        self._j_format_descriptor = j_format_descriptor
        super(FormatDescriptor, self).__init__(self._j_format_descriptor)


class OldCsv(FormatDescriptor):
    """
    Format descriptor for comma-separated values (CSV).

    ..note::
        This descriptor describes Flink's non-standard CSV table source/sink. In the future, the
        descriptor will be replaced by a proper RFC-compliant version. Use the RFC-compliant `Csv`
        format in the dedicated `flink-formats/flink-csv` module instead when writing to Kafka. Use
        the old one for stream/batch filesystem operations for now.

    .. note::
        Deprecated: use the RFC-compliant `Csv` format instead when writing to Kafka.
    """

    def __init__(self):
        gateway = get_gateway()
        self._j_csv = gateway.jvm.OldCsv()
        super(OldCsv, self).__init__(self._j_csv)

    def field_delimiter(self, delimiter):
        """
        Sets the field delimiter, "," by default.

        :param delimiter: The field delimiter.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.fieldDelimiter(delimiter)
        return self

    def line_delimiter(self, delimiter):
        """
        Sets the line delimiter, "\n" by default.

        :param delimiter: The line delimiter.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.lineDelimiter(delimiter)
        return self

    def field(self, field_name, field_type):
        """
        Adds a format field with the field name and the data type or type string. Required.
        This method can be called multiple times. The call order of this method defines
        also the order of the fields in the format.

        :param field_name: The field name.
        :param field_type: The data type or type string of the field.
        :return: This :class:`OldCsv` object.
        """
        if isinstance(field_type, (str, unicode)):
            self._j_csv = self._j_csv.field(field_name, field_type)
        else:
            self._j_csv = self._j_csv.field(field_name, _to_java_type(field_type))
        return self

    def quote_character(self, quote_character):
        """
        Sets a quote character for String values, null by default.

        :param quote_character: The quote character.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.quoteCharacter(quote_character)
        return self

    def comment_prefix(self, prefix):
        """
        Sets a prefix to indicate comments, null by default.

        :param prefix: The prefix to indicate comments.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.commentPrefix(prefix)
        return self

    def ignore_parse_errors(self):
        """
        Skip records with parse error instead to fail. Throw an exception by default.

        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.ignoreParseErrors()
        return self

    def ignore_first_line(self):
        """
        Ignore the first line. Not skip the first line by default.

        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.ignoreFirstLine()
        return self


class Csv(FormatDescriptor):
    """
    Format descriptor for comma-separated values (CSV).

    This descriptor aims to comply with RFC-4180 ("Common Format and MIME Type for
    Comma-Separated Values (CSV) Files") proposed by the Internet Engineering Task Force (IETF).

    ..note::
        This descriptor does not describe Flink's old non-standard CSV table
        source/sink. Currently, this descriptor can be used when writing to Kafka. The old one is
        still available as :class:`OldCsv` for stream/batch filesystem operations.
    """

    def __init__(self):
        gateway = get_gateway()
        self._j_csv = gateway.jvm.Csv()
        super(Csv, self).__init__(self._j_csv)

    def field_delimiter(self, delimiter):
        """
        Sets the field delimiter character (',' by default).

        :param delimiter: The field delimiter character.
        :return: This :class:`Csv` object.
        """
        if not isinstance(delimiter, (str, unicode)) or len(delimiter) > 1:
            raise TypeError("Only one-character string is supported!")
        self._j_csv = self._j_csv.fieldDelimiter(delimiter)
        return self

    def line_delimiter(self, delimiter):
        """
        Sets the line delimiter ("\n" by default; otherwise "\r" or "\r\n" are allowed).

        :param delimiter: The line delimiter.
        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.lineDelimiter(delimiter)
        return self

    def quote_character(self, quote_character):
        """
        Sets the field delimiter character (',' by default).

        :param delimiter: The field delimiter character.
        :return: This :class:`Csv` object.
        """
        if not isinstance(quote_character, (str, unicode)) or len(quote_character) > 1:
            raise TypeError("Only one-character string is supported!")
        self._j_csv = self._j_csv.quoteCharacter(quote_character)
        return self

    def allow_comments(self):
        """
        Ignores comment lines that start with '#' (disabled by default). If enabled, make sure to
        also ignore parse errors to allow empty rows.

        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.allowComments()
        return self

    def ignore_parse_errors(self):
        """
        Skip records with parse error instead to fail. Throw an exception by default.

        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.ignoreParseErrors()
        return self

    def array_element_delimiter(self, delimiter):
        """
        Sets the array element delimiter string for separating array or row element
        values (";" by default).

        :param delimiter: The array element delimiter.
        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.arrayElementDelimiter(delimiter)
        return self

    def escape_character(self, escape_character):
        """
        Sets the escape character for escaping values (disabled by default).

        :param escape_character: Escaping character (e.g. backslash).
        :return: This :class:`Csv` object.
        """
        if not isinstance(escape_character, (str, unicode)) or len(escape_character) > 1:
            raise TypeError("Only one-character string is supported!")
        self._j_csv = self._j_csv.escapeCharacter(escape_character)
        return self

    def null_literal(self, null_literal):
        """
        Sets the null literal string that is interpreted as a null value (disabled by default).

        :param null_literal:
        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.nullLiteral(null_literal)
        return self

    def schema(self, schema_data_type):
        """
        Sets the format schema with field names and the types. Required if schema is not derived.

        :param schema_data_type: Data type from :class:`DataTypes` that describes the schema.
        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.schema(_to_java_type(schema_data_type))
        return self

    def derive_schema(self):
        """
        Derives the format schema from the table's schema. Required if no format schema is defined.

        This allows for defining schema information only once.

        The names, types, and fields' order of the format are determined by the table's
        schema. Time attributes are ignored if their origin is not a field. A "from" definition
        is interpreted as a field renaming in the format.

        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.deriveSchema()
        return self


class Avro(FormatDescriptor):
    """
    Format descriptor for Apache Avro records.
    """

    def __init__(self):
        gateway = get_gateway()
        self._j_avro = gateway.jvm.Avro()
        super(Avro, self).__init__(self._j_avro)

    def record_class(self, record_class):
        """
        Sets the class of the Avro specific record.

        :param record_class: The java fully-qualified class name of the Avro record.
        :return: This object.
        """
        gateway = get_gateway()
        clz = gateway.jvm.Thread.currentThread().getContextClassLoader().loadClass(record_class)
        self._j_avro = self._j_avro.recordClass(clz)
        return self

    def avro_schema(self, avro_schema):
        """
        Sets the Avro schema for specific or generic Avro records.

        :param avro_schema: Avro schema string.
        :return: This object.
        """
        self._j_avro = self._j_avro.avroSchema(avro_schema)
        return self


class Json(FormatDescriptor):
    """
    Format descriptor for JSON.
    """

    def __init__(self):
        gateway = get_gateway()
        self._j_json = gateway.jvm.Json()
        super(Json, self).__init__(self._j_json)

    def fail_on_missing_field(self, fail_on_missing_field):
        """
        Sets flag whether to fail if a field is missing or not.

        :param fail_on_missing_field: If set to ``True``, the operation fails if there is a missing
                                      field.
                                      If set to ``False``, a missing field is set to null.
        :return: This object.
        """
        if not isinstance(fail_on_missing_field, bool):
            raise TypeError("Only bool value is supported!")
        self._j_json = self._j_json.failOnMissingField(fail_on_missing_field)
        return self

    def json_schema(self, json_schema):
        """
        Sets the JSON schema string with field names and the types according to the JSON schema
        specification: http://json-schema.org/specification.html

        The schema might be nested.

        :param json_schema: The JSON schema string.
        :return: This object.
        """
        self._j_json = self._j_json.jsonSchema(json_schema)
        return self

    def schema(self, schema_data_type):
        """
        Sets the schema using :class:`DataTypes`.

        JSON objects are represented as ROW types.

        The schema might be nested.

        :param schema_data_type: Data type that describes the schema.
        :return: This object.
        """
        self._j_json = self._j_json.schema(_to_java_type(schema_data_type))
        return self

    def derive_schema(self):
        """
        Derives the format schema from the table's schema described.

        This allows for defining schema information only once.

        The names, types, and fields' order of the format are determined by the table's
        schema. Time attributes are ignored if their origin is not a field. A "from" definition
        is interpreted as a field renaming in the format.

        :return: This object.
        """
        self._j_json = self._j_json.deriveSchema()
        return self


class ConnectorDescriptor(Descriptor):
    """
    Describes a connector to an other system.
    """

    __metaclass__ = ABCMeta

    def __init__(self, j_connector_descriptor):
        self._j_connector_descriptor = j_connector_descriptor
        super(ConnectorDescriptor, self).__init__(self._j_connector_descriptor)


class FileSystem(ConnectorDescriptor):
    """
    Connector descriptor for a file system.
    """

    def __init__(self):
        gateway = get_gateway()
        self._j_file_system = gateway.jvm.FileSystem()
        super(FileSystem, self).__init__(self._j_file_system)

    def path(self, path_str):
        """
        Sets the path to a file or directory in a file system.

        :param path_str: The path of a file or directory.
        :return: This :class:`FileSystem` object.
        """
        self._j_file_system = self._j_file_system.path(path_str)
        return self


class ConnectTableDescriptor(Descriptor):
    """
    Common class for table's created with :class:`pyflink.table.TableEnvironment.connect`.
    """

    __metaclass__ = ABCMeta

    def __init__(self, j_table_descriptor):
        self._j_table_descriptor = j_table_descriptor
        super(ConnectTableDescriptor, self).__init__(self._j_table_descriptor)

    def with_format(self, format_descriptor):
        """
        Specifies the format that defines how to read data from a connector.

        :type format_descriptor: The :class:`FormatDescriptor` for the resulting table,
                                 e.g. :class:`OldCsv`.
        :return: This object.
        """
        self._j_table_descriptor = \
            self._j_table_descriptor.withFormat(format_descriptor._j_format_descriptor)
        return self

    def with_schema(self, schema):
        """
        Specifies the resulting table schema.

        :type schema: The :class:`Schema` object for the resulting table.
        :return: This object.
        """
        self._j_table_descriptor = self._j_table_descriptor.withSchema(schema._j_schema)
        return self

    def register_table_sink(self, name):
        """
        Searches for the specified table sink, configures it accordingly, and registers it as
        a table under the given name.

        :param name: Table name to be registered in the table environment.
        :return: This object.
        """
        self._j_table_descriptor = self._j_table_descriptor.registerTableSink(name)
        return self

    def register_table_source(self, name):
        """
        Searches for the specified table source, configures it accordingly, and registers it as
        a table under the given name.

        :param name: Table name to be registered in the table environment.
        :return: This object.
        """
        self._j_table_descriptor = self._j_table_descriptor.registerTableSource(name)
        return self

    def register_table_source_and_sink(self, name):
        """
        Searches for the specified table source and sink, configures them accordingly, and
        registers them as a table under the given name.

        :param name: Table name to be registered in the table environment.
        :return: This object.
        """
        self._j_table_descriptor = self._j_table_descriptor.registerTableSourceAndSink(name)
        return self


class StreamTableDescriptor(ConnectTableDescriptor):
    """
    Descriptor for specifying a table source and/or sink in a streaming environment.
    """

    def __init__(self, j_stream_table_descriptor):
        self._j_stream_table_descriptor = j_stream_table_descriptor
        super(StreamTableDescriptor, self).__init__(self._j_stream_table_descriptor)

    def in_append_mode(self):
        """
        Declares how to perform the conversion between a dynamic table and an external connector.

        In append mode, a dynamic table and an external connector only exchange INSERT messages.

        :return: This object.
        """
        self._j_stream_table_descriptor = self._j_stream_table_descriptor.inAppendMode()
        return self

    def in_retract_mode(self):
        """
        Declares how to perform the conversion between a dynamic table and an external connector.

        In retract mode, a dynamic table and an external connector exchange ADD and RETRACT
        messages.

        An INSERT change is encoded as an ADD message, a DELETE change as a RETRACT message, and an
        UPDATE change as a RETRACT message for the updated (previous) row and an ADD message for
        the updating (new) row.

        In this mode, a key must not be defined as opposed to upsert mode. However, every update
        consists of two messages which is less efficient.

        :return: This object.
        """
        self._j_stream_table_descriptor = self._j_stream_table_descriptor.inRetractMode()
        return self

    def in_upsert_mode(self):
        """
        Declares how to perform the conversion between a dynamic table and an external connector.

        In upsert mode, a dynamic table and an external connector exchange UPSERT and DELETE
        messages.

        This mode requires a (possibly composite) unique key by which updates can be propagated. The
        external connector needs to be aware of the unique key attribute in order to apply messages
        correctly. INSERT and UPDATE changes are encoded as UPSERT messages. DELETE changes as
        DELETE messages.

        The main difference to a retract stream is that UPDATE changes are encoded with a single
        message and are therefore more efficient.

        :return: This object.
        """
        self._j_stream_table_descriptor = self._j_stream_table_descriptor.inUpsertMode()
        return self


class BatchTableDescriptor(ConnectTableDescriptor):
    """
    Descriptor for specifying a table source and/or sink in a batch environment.
    """

    def __init__(self, j_batch_table_descriptor):
        self.j_batch_table_descriptor = j_batch_table_descriptor
        super(BatchTableDescriptor, self).__init__(self.j_batch_table_descriptor)
