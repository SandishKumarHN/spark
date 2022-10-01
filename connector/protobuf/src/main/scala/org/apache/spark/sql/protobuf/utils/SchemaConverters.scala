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
package org.apache.spark.sql.protobuf.utils

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.protobuf.ScalaReflectionLock
import org.apache.spark.sql.types._

@DeveloperApi
object SchemaConverters {
  /**
   * Internal wrapper for SQL data type and nullability.
   *
   * @since 3.4.0
   */
  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
   * Converts an Protobuf schema to a corresponding Spark SQL schema.
   *
   * @since 3.4.0
   */
  def toSqlType(protoSchema: Descriptor): SchemaType = {
    toSqlTypeHelper(protoSchema)
  }

  def toSqlTypeHelper(descriptor: Descriptor): SchemaType = ScalaReflectionLock.synchronized {
    SchemaType(StructType(descriptor.getFields.asScala.flatMap(structFieldFor).toSeq),
      nullable = true)
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
      case MESSAGE if fd.getMessageType.getOptions.getMapEntry =>
        var keyType: DataType = NullType
        var valueType: DataType = NullType
        fd.getMessageType.getFields.forEach {
          field =>
            field.getName match {
              case "key" =>
                val spField = structFieldFor(field)
                // keyValueType = (spField.get.dataType, NullType)
                keyType = spField.get.dataType
              case "value" =>
                val spField = structFieldFor(field)
                valueType = spField.get.dataType
                //keyValueType = (spField.get.dataType, NullType)
            }
        }
        return Option(StructField(fd.getName,
          MapType(keyType, valueType, valueContainsNull = false),
          nullable = false))
      case MESSAGE =>
        Option(fd.getMessageType.getFields.asScala.flatMap(structFieldFor).toSeq)
          .filter(_.nonEmpty)
          .map(StructType.apply)
      // Or throw a Unsupported type exception.
      case _ =>
        None
    }

    dataType.map(dt => StructField(
      fd.getName,
      if (fd.isRepeated) ArrayType(dt, containsNull = false) else dt,
      nullable = !fd.isRequired && !fd.isRepeated
    ))
  }

  /**
   * Converts a Spark SQL schema to a corresponding Protobuf Descriptor
   *
   * @since 3.4.0
   */
  def toProtoType(catalystType: DataType, recordName: String = "topLevelRecord"): Descriptor = {
    val schemaBuilder: DynamicSchema#Builder = new DynamicSchema().newBuilder()
    schemaBuilder.setName("DynamicSchema.proto")
    toMessageDefinition(catalystType, recordName, schemaBuilder: DynamicSchema#Builder)
    schemaBuilder.build().getMessageDescriptor(recordName)
  }

  /**
   * Create MessageDefinition for corresponding catalystType
   * @param catalystType
   * @param recordName
   * @param schemaBuilder
   */
  def toMessageDefinition(catalystType: DataType, recordName: String,
                          schemaBuilder: DynamicSchema#Builder): Unit = {
    catalystType match {
      case st: StructType =>
        val queue = mutable.Queue[ProtoMessage]()
        val list = new util.ArrayList[ProtoField]()
        st.foreach { f =>
          list.add(ProtoField(f.name, f.dataType))
        }
        queue += ProtoMessage(recordName, list)
        while (!queue.isEmpty) {
          val protoMessage = queue.dequeue()
          val messageDefinition: MessageDefinition#Builder =
            new MessageDefinition().newBuilder(protoMessage.messageName)
          var index = 0
          protoMessage.fieldList.forEach {
            protoField =>
              protoField.catalystType match {
                case ArrayType(at, containsNull) =>
                  index = index + 1
                  at match {
                    case st: StructType =>
                      messageDefinition.addField("repeated", protoField.name,
                        protoField.name, index)
                      val list = new util.ArrayList[ProtoField]()
                      st.foreach { f =>
                        list.add(ProtoField(f.name, f.dataType))
                      }
                      queue += ProtoMessage(protoField.name, list)
                    case _ =>
                      convertBasicTypes(protoField.catalystType, messageDefinition, "repeated",
                        index, protoField)
                  }
                case st: StructType =>
                  index = index + 1
                  messageDefinition.addField("optional", protoField.name, protoField.name, index)
                  val list = new util.ArrayList[ProtoField]()
                  st.foreach { f =>
                    list.add(ProtoField(f.name, f.dataType))
                  }
                  queue += ProtoMessage(protoField.name, list)
                case _ =>
                  index = index + 1
                  convertBasicTypes(protoField.catalystType, messageDefinition, "optional",
                    index, protoField)
              }
          }
          schemaBuilder.addMessageDefinition(messageDefinition.build())
        }
    }
  }

  def convertBasicTypes(catalystType: DataType, messageDefinition: MessageDefinition#Builder,
                        label: String, index: Int, protoField: ProtoField): Unit = {
    if (sparkToProtoTypeMap.contains(catalystType)) {
      messageDefinition.addField(label, sparkToProtoTypeMap.get(catalystType).orNull,
        protoField.name, index)
    } else {
      throw new IncompatibleSchemaException(s"Cannot convert SQL type ${catalystType.sql} to " +
        s"Proto type, try passing Protobuf Descriptor file path to_protobuf function")
    }
  }

  private val sparkToProtoTypeMap = Map[DataType, String](ByteType -> "int32", ShortType -> "int32",
    IntegerType -> "int32", DateType -> "int32", LongType -> "int64", BinaryType -> "bytes",
    DoubleType -> "double", FloatType -> "float", TimestampType -> "int64",
    TimestampNTZType -> "int64", StringType -> "string", BooleanType -> "bool")

  case class ProtoMessage(messageName: String, fieldList: util.ArrayList[ProtoField])

  case class ProtoField(name: String, catalystType: DataType)

  private[protobuf] class IncompatibleSchemaException(
                                                    msg: String,
                                                    ex: Throwable = null) extends Exception(msg, ex)

  private[protobuf] class UnsupportedProtoValueException(msg: String) extends Exception(msg)
}
