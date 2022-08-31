/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.proto

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.google.protobuf.{ByteString, DynamicMessage}
import org.apache.spark.sql.proto.ProtoUtils.toFieldStr
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, StructFilters}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.proto.ProtoUtils.ProtoMatchedField
import org.apache.spark.sql.proto.SchemaConverters.IncompatibleSchemaException
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DayTimeIntervalType, Decimal, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StringType, StructType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.UTF8String
import com.google.protobuf.Descriptors._

private[sql] class ProtoDeserializer(
                                      rootProtoType: Descriptor,
                                      rootCatalystType: DataType,
                                      positionalFieldMatch: Boolean,
                                      datetimeRebaseSpec: RebaseSpec,
                                      filters: StructFilters) {

  def this(
            rootProtoType: Descriptor,
            rootCatalystType: DataType,
            datetimeRebaseMode: String) = {
    this(
      rootProtoType,
      rootCatalystType,
      positionalFieldMatch = false,
      RebaseSpec(LegacyBehaviorPolicy.withName(datetimeRebaseMode)),
      new NoopFilters)
  }

  private val dateRebaseFunc = DataSourceUtils.createDateRebaseFuncInRead(
    datetimeRebaseSpec.mode, "Proto")

  private val converter: Any => Option[Any] = try {
    rootCatalystType match {
      // A shortcut for empty schema.
      case st: StructType if st.isEmpty =>
        (_: Any) => Some(InternalRow.empty)

      case st: StructType =>
        val resultRow = new SpecificInternalRow(st.map(_.dataType))
        val fieldUpdater = new RowUpdater(resultRow)
        val applyFilters = filters.skipRow(resultRow, _)
        val writer = getRecordWriter(rootProtoType, st, Nil, Nil, applyFilters)
        (data: Any) => {
          val record = data.asInstanceOf[DynamicMessage]
          val skipRow = writer(fieldUpdater, record)
          if (skipRow) None else Some(resultRow)
        }
    }
  } catch {
    case ise: IncompatibleSchemaException => throw new IncompatibleSchemaException(
      s"Cannot convert Proto type ${rootProtoType.toProto.toString} to SQL type ${rootCatalystType.sql}.", ise)
  }

  def deserialize(data: Any): Option[Any] = converter(data)

  private def newArrayWriter(
                              protoField: FieldDescriptor,
                              catalystType: DataType,
                              protoPath: Seq[String],
                              catalystPath: Seq[String],
                              elementType: DataType,
                              containsNull: Boolean): (CatalystDataUpdater, Int, Any) => Unit = {


    val protoElementPath = protoPath :+ "element"
    val elementWriter = newWriter(protoField, elementType,
      protoElementPath, catalystPath :+ "element")
    (updater, ordinal, value) =>
      val collection = value.asInstanceOf[java.util.Collection[Any]]
      val result = createArrayData(elementType, collection.size())
      val elementUpdater = new ArrayDataUpdater(result)

      var i = 0
      val iter = collection.iterator()
      while (iter.hasNext) {
        val element = iter.next()
        if (element == null) {
          if (!containsNull) {
            throw new RuntimeException(
              s"Array value at path ${toFieldStr(protoElementPath)} is not allowed to be null")
          } else {
            elementUpdater.setNullAt(i)
          }
        } else {
          elementWriter(elementUpdater, i, element)
        }
        i += 1
      }

      updater.set(ordinal, result)
  }
  /**
    * Creates a writer to write proto values to Catalyst values at the given ordinal with the given
    * updater.
    */
  private def newWriter(
                         field: FieldDescriptor,
                         catalystType: DataType,
                         protoPath: Seq[String],
                         catalystPath: Seq[String]): (CatalystDataUpdater, Int, Any) => Unit = {
    val errorPrefix = s"Cannot convert Proto ${toFieldStr(protoPath)} to " +
      s"SQL ${toFieldStr(catalystPath)} because "
    val incompatibleMsg = errorPrefix +
      s"schema is incompatible (protoType = ${field} ${field.toProto.getLabel} ${field.getJavaType} " +
      s"${field.getType}, sqlType = ${catalystType.sql})"

    (field.getJavaType, catalystType) match {

      case (null, NullType) => (updater, ordinal, _) =>
        updater.setNullAt(ordinal)

      // TODO: we can avoid boxing if future version of proto provide primitive accessors.
      case (BOOLEAN, BooleanType) => (updater, ordinal, value) =>
        updater.setBoolean(ordinal, value.asInstanceOf[Boolean])

      case (BOOLEAN, ArrayType(BooleanType, containsNull)) =>
        newArrayWriter(field, catalystType, protoPath,
          catalystPath, BooleanType, containsNull)

      case (INT, IntegerType) => (updater, ordinal, value) =>
        updater.setInt(ordinal, value.asInstanceOf[Int])

      case (INT, ArrayType(IntegerType, containsNull)) =>
        newArrayWriter(field, catalystType, protoPath,
          catalystPath, IntegerType, containsNull)

      case (INT, DateType) => (updater, ordinal, value) =>
        updater.setInt(ordinal, dateRebaseFunc(value.asInstanceOf[Int]))

      case (LONG, LongType) => (updater, ordinal, value) =>
        updater.setLong(ordinal, value.asInstanceOf[Long])

      case (LONG, ArrayType(LongType, containsNull)) =>
        newArrayWriter(field, catalystType, protoPath,
          catalystPath, LongType, containsNull)

      case (FLOAT, FloatType) => (updater, ordinal, value) =>
        updater.setFloat(ordinal, value.asInstanceOf[Float])

      case (FLOAT, ArrayType(FloatType, containsNull)) =>
        newArrayWriter(field, catalystType, protoPath,
          catalystPath, FloatType, containsNull)

      case (DOUBLE, DoubleType) => (updater, ordinal, value) =>
        updater.setDouble(ordinal, value.asInstanceOf[Double])

      case (DOUBLE, ArrayType(DoubleType, containsNull)) =>
        newArrayWriter(field, catalystType, protoPath,
          catalystPath, DoubleType, containsNull)

      case (STRING, StringType) => (updater, ordinal, value) =>
        val str = value match {
          case s: String => UTF8String.fromString(s)
        }
        updater.set(ordinal, str)

      case (STRING, ArrayType(StringType, containsNull)) =>
        newArrayWriter(field, catalystType, protoPath,
          catalystPath, StringType, containsNull)

      case (BYTE_STRING, BinaryType) => (updater, ordinal, value) =>
        val byte_array = value match {
          case s: ByteString => s.toByteArray
          case _ => throw new Exception("Invalid ByteString format")
        }
        updater.set(ordinal, byte_array)

      case (BYTE_STRING, ArrayType(BinaryType, containsNull)) =>
        newArrayWriter(field, catalystType, protoPath,
          catalystPath, BinaryType, containsNull)

      case (MESSAGE, st: StructType) =>
        val writeRecord = getRecordWriter(field.getMessageType, st, protoPath,
          catalystPath, applyFilters = _ => false)
        (updater, ordinal, value) =>
          val row = new SpecificInternalRow(st)
          writeRecord(new RowUpdater(row), value.asInstanceOf[DynamicMessage])
          updater.set(ordinal, row)

      case (MESSAGE, ArrayType(st: StructType, containsNull)) =>
        newArrayWriter(field, catalystType, protoPath,
          catalystPath, st, containsNull)

      case (ENUM, StringType) => (updater, ordinal, value) =>
        updater.set(ordinal, UTF8String.fromString(value.toString))

      case (ENUM, ArrayType(StringType, containsNull)) =>
        newArrayWriter(field, catalystType, protoPath,
          catalystPath, StringType, containsNull)

      // TBD: Do we need this here ?
      case (INT, _: YearMonthIntervalType) => (updater, ordinal, value) =>
        updater.setInt(ordinal, value.asInstanceOf[Int])

      case (LONG, _: DayTimeIntervalType) => (updater, ordinal, value) =>
        updater.setLong(ordinal, value.asInstanceOf[Long])

      case _ => throw new IncompatibleSchemaException(incompatibleMsg)
    }
  }


  private def getRecordWriter(
                               protoType: Descriptor,
                               catalystType: StructType,
                               protoPath: Seq[String],
                               catalystPath: Seq[String],
                               applyFilters: Int => Boolean): (CatalystDataUpdater, DynamicMessage) => Boolean = {

    val protoSchemaHelper = new ProtoUtils.ProtoSchemaHelper(
      protoType, catalystType, protoPath, catalystPath, positionalFieldMatch)
    
    protoSchemaHelper.validateNoExtraCatalystFields(ignoreNullable = true)
    // no need to validateNoExtraProtoFields since extra Proto fields are ignored

    val (validFieldIndexes, fieldWriters) = protoSchemaHelper.matchedFields.map {
      case ProtoMatchedField(catalystField, ordinal, protoField) =>
        val baseWriter = newWriter(protoField, catalystField.dataType,
          protoPath :+ protoField.getName, catalystPath :+ catalystField.name)
        val fieldWriter = (fieldUpdater: CatalystDataUpdater, value: Any) => {
          if (value == null) {
            fieldUpdater.setNullAt(ordinal)
          } else {
            baseWriter(fieldUpdater, ordinal, value)
          }
        }
        (protoField, fieldWriter)
    }.toArray.unzip

    (fieldUpdater, record) => {
      var i = 0
      var skipRow = false
      while (i < validFieldIndexes.length && !skipRow) {
        fieldWriters(i)(fieldUpdater, record.getField(validFieldIndexes(i)))
        skipRow = applyFilters(i)
        i += 1
      }
      skipRow
    }
  }

  private def createArrayData(elementType: DataType, length: Int): ArrayData = elementType match {
    case BooleanType => UnsafeArrayData.fromPrimitiveArray(new Array[Boolean](length))
    case ByteType => UnsafeArrayData.fromPrimitiveArray(new Array[Byte](length))
    case ShortType => UnsafeArrayData.fromPrimitiveArray(new Array[Short](length))
    case IntegerType => UnsafeArrayData.fromPrimitiveArray(new Array[Int](length))
    case LongType => UnsafeArrayData.fromPrimitiveArray(new Array[Long](length))
    case FloatType => UnsafeArrayData.fromPrimitiveArray(new Array[Float](length))
    case DoubleType => UnsafeArrayData.fromPrimitiveArray(new Array[Double](length))
    case _ => new GenericArrayData(new Array[Any](length))
  }

  /**
    * A base interface for updating values inside catalyst data structure like `InternalRow` and
    * `ArrayData`.
    */
  sealed trait CatalystDataUpdater {
    def set(ordinal: Int, value: Any): Unit

    def setNullAt(ordinal: Int): Unit = set(ordinal, null)
    def setBoolean(ordinal: Int, value: Boolean): Unit = set(ordinal, value)
    def setByte(ordinal: Int, value: Byte): Unit = set(ordinal, value)
    def setShort(ordinal: Int, value: Short): Unit = set(ordinal, value)
    def setInt(ordinal: Int, value: Int): Unit = set(ordinal, value)
    def setLong(ordinal: Int, value: Long): Unit = set(ordinal, value)
    def setDouble(ordinal: Int, value: Double): Unit = set(ordinal, value)
    def setFloat(ordinal: Int, value: Float): Unit = set(ordinal, value)
    def setDecimal(ordinal: Int, value: Decimal): Unit = set(ordinal, value)
  }

  final class RowUpdater(row: InternalRow) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = row.update(ordinal, value)

    override def setNullAt(ordinal: Int): Unit = row.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = row.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit =
      row.setDecimal(ordinal, value, value.precision)
  }

  final class ArrayDataUpdater(array: ArrayData) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = array.update(ordinal, value)

    override def setNullAt(ordinal: Int): Unit = array.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = array.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = array.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = array.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = array.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = array.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = array.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = array.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit = array.update(ordinal, value)
  }

}
