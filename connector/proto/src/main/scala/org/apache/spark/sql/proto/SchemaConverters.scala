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

import scala.collection.JavaConverters._
import com.google.protobuf.DescriptorProtos.{DescriptorProto, FieldDescriptorProto}
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types._

/**
  * This object contains method that are used to convert sparkSQL schemas to proto schemas and vice
  * versa.
  */
@DeveloperApi
object SchemaConverters {
  /**
    * Internal wrapper for SQL data type and nullability.
    * @since 3.4.0
    */
  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
    * Converts an Proto schema to a corresponding Spark SQL schema.
    * @since 3.4.0
    */
  def toSqlType(protoSchema: Descriptor): SchemaType = {
    toSqlTypeHelper(protoSchema)
  }

  def toSqlTypeHelper(descriptor: Descriptor): SchemaType = ScalaReflectionLock.synchronized {
    import scala.collection.JavaConverters._
    SchemaType(StructType(descriptor.getFields.asScala.flatMap(structFieldFor)), nullable = true)
  }

  def structFieldFor(fd: FieldDescriptor): Option[StructField] = {
    import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
    val dataType = fd.getJavaType match {
      case INT => Some(IntegerType)
      case LONG => Some(LongType)
      case FLOAT => Some(FloatType)
      case DOUBLE => Some(DoubleType)
      case BOOLEAN => Some(BooleanType)
      case STRING => Some(StringType)
      case BYTE_STRING => Some(BinaryType)
      case ENUM => Some(StringType)
      case MESSAGE =>
        import collection.JavaConverters._
        Option(fd.getMessageType.getFields.asScala.flatMap(structFieldFor))
          .filter(_.nonEmpty)
          .map(StructType.apply)

    }
    dataType.map( dt => StructField(
      fd.getName,
      if (fd.isRepeated) ArrayType(dt, containsNull = false) else dt,
      nullable = !fd.isRequired && !fd.isRepeated
    ))
  }

  /**
   * Converts a Spark SQL schema to a corresponding Proto Descriptor
   *
   * @since 2.4.0
   */
  def toProtoType(
                  catalystType: DataType,
                  nullable: Boolean = false,
                  recordName: String = "topLevelRecord",
                  nameSpace: String = "",
                  indexNum: Int = 0,
                  descriptorProto: DescriptorProto.Builder = DescriptorProto.newBuilder())
  : DescriptorProto = {
    var index = indexNum
    catalystType match {
      case st: StructType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.$recordName" else recordName
        descriptorProto.setName(childNameSpace)
        st.foreach { f =>
          index = index + 1
          descriptorProto.addField(addProtoField(f.dataType, f.nullable, f.name, childNameSpace, index).build())
        }
//      case ym: YearMonthIntervalType =>
//        val ymIntervalType = builder.intType()
//        ymIntervalType.addProp(CATALYST_TYPE_PROP_NAME, ym.typeName)
//        ymIntervalType
//      case dt: DayTimeIntervalType =>
//        val dtIntervalType = builder.longType()
//        dtIntervalType.addProp(CATALYST_TYPE_PROP_NAME, dt.typeName)
//        dtIntervalType
      // This should never happen.
      case other => throw new IncompatibleSchemaException(s"Unexpected type $other.")
    }
//    if (nullable && catalystType != NullType) {
//      Schema.createUnion(schema, nullSchema)
//    } else {
//      schema
//    }
    descriptorProto.build()
  }


  def addProtoField(catalystType: DataType,
               nullable: Boolean = false,
               recordName: String = "topLevelRecord",
               nameSpace: String = "",
               indexNum: Int = 0) : FieldDescriptorProto.Builder = {
    var index = indexNum
    val protoField = catalystType match {
      case BooleanType =>
        addField("optional", FieldDescriptorProto.Type.TYPE_BOOL, recordName, index, null)
      case ByteType | ShortType | IntegerType | DateType =>
        addField("optional", FieldDescriptorProto.Type.TYPE_INT32, recordName, index, null)
      case LongType =>
        addField("optional", FieldDescriptorProto.Type.TYPE_INT64, recordName, index, null)
      case TimestampType | TimestampNTZType =>
        addField("optional", FieldDescriptorProto.Type.TYPE_INT64, recordName, index, null)
      case FloatType =>
        addField("optional", FieldDescriptorProto.Type.TYPE_FLOAT, recordName, index, null)
      case DoubleType =>
        addField("optional", FieldDescriptorProto.Type.TYPE_DOUBLE, recordName, index, null)
      case StringType =>
        addField("optional", FieldDescriptorProto.Type.TYPE_STRING, recordName, index, null)
//      case NullType =>
//      case d: DecimalType =>
      //        val avroType = LogicalTypes.decimal(d.precision, d.scale)
      //        val fixedSize = minBytesForPrecision(d.precision)
      //        // Need to avoid naming conflict for the fixed fields
      //        val name = nameSpace match {
      //          case "" => s"$recordName.fixed"
      //          case _ => s"$nameSpace.$recordName.fixed"
      //        }
      //        avroType.addToSchema(SchemaBuilder.fixed(name).size(fixedSize))
      case ArrayType(et, containsNull) =>
        addField("repeated", typeToFieldType.get(et.typeName), recordName, index, null)
      case BinaryType =>
        addField("optional", FieldDescriptorProto.Type.TYPE_BYTES, recordName, index, null)

//      case MapType(StringType, vt, valueContainsNull) =>

    }
    protoField
  }

  val labelMap = Map("optional" -> FieldDescriptorProto.Label.LABEL_OPTIONAL,
    "required" -> FieldDescriptorProto.Label.LABEL_REQUIRED,
    "repeated" -> FieldDescriptorProto.Label.LABEL_REPEATED).asJava

  val typeToFieldType = Map("long" -> FieldDescriptorProto.Type.TYPE_INT64,
    "string"-> FieldDescriptorProto.Type.TYPE_STRING,
    "integer"-> FieldDescriptorProto.Type.TYPE_INT32,
    "long"-> FieldDescriptorProto.Type.TYPE_INT64,
    "double"-> FieldDescriptorProto.Type.TYPE_DOUBLE,
    "float"-> FieldDescriptorProto.Type.TYPE_FLOAT,
    "boolean"-> FieldDescriptorProto.Type.TYPE_BOOL,
    "binary" -> FieldDescriptorProto.Type.TYPE_BYTES).asJava

  def addField(label: String, protoType: FieldDescriptorProto.Type, name: String , num: Int, defaultVal: String) : FieldDescriptorProto.Builder = {
    val protoLabel: FieldDescriptorProto.Label = labelMap.get(label)
    if (protoLabel == null) throw new IllegalArgumentException("Illegal label: " + label)
    if (protoType == null) throw new IllegalArgumentException("Illegal type: ")
    val fieldBuilder: FieldDescriptorProto.Builder = FieldDescriptorProto.newBuilder()
    fieldBuilder.setLabel(protoLabel).setType(protoType).setName(name).setNumber(num)
    if (defaultVal != null) fieldBuilder.setDefaultValue(defaultVal)
    fieldBuilder
  }

  private[proto] class IncompatibleSchemaException(
                                                   msg: String, ex: Throwable = null) extends Exception(msg, ex)

  private[proto] class UnsupportedProtoTypeException(msg: String) extends Exception(msg)
  private[proto] class UnsupportedProtoValueException(msg: String) extends Exception(msg)
}
