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
package org.apache.spark.sql.protobuf

import scala.collection.JavaConverters._
import scala.collection.mutable
import java.nio.ByteBuffer
import java.io.ByteArrayInputStream
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.DynamicMessage
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.protobuf.{MessageIndexes, ProtobufSchema}
import org.apache.kafka.common.errors.SerializationException
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.protobuf.utils.{ProtobufUtils, SchemaConverters}
import org.apache.spark.sql.types.{BinaryType, DataType}

private[protobuf] case class CatalystDataToProtobuf(
                                               child: Expression,
                                               descFilePath: String,
                                               messageName: String,
                                               schemaRegistryURLs: Option[String],
                                               subject: Option[String])
  extends UnaryExpression {

  override def dataType: DataType = BinaryType

  @transient private lazy val protoType = (schemaRegistryURLs, subject) match {
    case (Some(url), Some(subject)) =>
      lazy val cachedSchemaRegistry =
        new CachedSchemaRegistryClient(url.split(",").toList.asJava, 128)
      SchemaConverters.descriptorFromSchemaRegistry(cachedSchemaRegistry, subject, 0)
    case _ => ProtobufUtils.buildDescriptor(descFilePath, messageName)
  }

  @transient private lazy val serializer = new ProtobufSerializer(child.dataType, protoType,
    child.nullable)

  @transient private lazy val MAGIC_BYTE = 0x0

  @transient private lazy val SCHEMA_ID_SIZE_BYTES = 4

  override def nullSafeEval(input: Any): Any = {
    val dynamicMessage = serializer.serialize(input).asInstanceOf[DynamicMessage]
    dynamicMessage.toByteArray
  }

  override def prettyName: String = "to_protobuf"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(byte[]) $expr.nullSafeEval($input)")
  }

  override protected def withNewChildInternal(newChild: Expression): CatalystDataToProtobuf =
    copy(child = newChild)
}

