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

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.proto.SimpleMessageRepeatedProtos.SimpleMessageRepeated
import org.apache.spark.sql.proto.SimpleMessageEnumProtos.{BasicEnum, SimpleMessageEnum}
import org.apache.spark.sql.proto.SimpleMessageMapProtos.SimpleMessageMap
import org.apache.spark.sql.proto.MessageMultipleMessage.{IncludedExample, MultipleExample, OtherExample}
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.proto.SimpleMessageEnumProtos.SimpleMessageEnum.NestedEnum

class ProtoFunctionsSuite extends QueryTest with SharedSparkSession with Serializable {
  import testImplicits._

  test("roundtrip in to_proto and from_proto - int and string") {
    val SIMPLE_MESSAGE = "protobuf/message_with_repeated.desc"
    val simpleMessagePath = testFile(SIMPLE_MESSAGE).replace("file:/", "/")

    val simpleMessage = SimpleMessageRepeated.newBuilder()
      .setKey("key")
      .setValue("value")
      .addRboolValue(false)
      .addRboolValue(true)
      .addRdoubleValue(1092092)
      .addRdoubleValue(1092093)
      .addRfloatValue(10903.0f)
      .addRfloatValue(10902.0f)
      .build()

    val df = Seq(simpleMessage.toByteArray).toDF("value")

    val dfRes = df.select(functions.from_proto($"value", simpleMessagePath, "SimpleMessageRepeated").as("value"))
    dfRes.select($"value.*").show()
    dfRes.printSchema()

    val dfRes2 = dfRes.select(functions.to_proto($"value", simpleMessagePath, "SimpleMessageRepeated").as("value2"))
    dfRes2.show()

    val dfRes3 = dfRes2.select(functions.from_proto($"value2", simpleMessagePath, "SimpleMessageRepeated").as("value3"))
    dfRes3.select($"value3.*").show()
    dfRes3.printSchema()
  }

  test("roundtrip in to_proto - struct") {
    val messagePath = testFile("protobuf/simple_message.desc").replace("file:/", "/")
    val df = spark.range(10).select(
      struct(
        $"id",
        $"id".cast(org.apache.spark.sql.types.StringType).as("string_value"),
        $"id".cast(org.apache.spark.sql.types.IntegerType).as("int32_value"),
        $"id".cast(org.apache.spark.sql.types.IntegerType).as("uint32_value"),
        $"id".cast(org.apache.spark.sql.types.LongType).as("int64_value"),
        $"id".cast(org.apache.spark.sql.types.LongType).as("uint64_value"),
        $"id".cast(org.apache.spark.sql.types.DoubleType).as("double_value"),
        lit(1202.00).cast(org.apache.spark.sql.types.FloatType).as("float_value"),
        lit(true).as("bool_value"),
        lit("0".getBytes).as("bytes_value")
      ).as("SimpleMessage")
    )
    val protoStructDF = df.select(functions.to_proto($"SimpleMessage").as("proto"))
    df.collect().foreach(f => print(f))
    df.printSchema()
    val actualDf = protoStructDF.select(functions.from_proto($"proto", messagePath, "SimpleMessage").as("SimpleMessage"))
    actualDf.collect().foreach(f => print(f))
    actualDf.printSchema()
    checkAnswer(actualDf, df)
  }

  test("roundtrip in to_proto and from_proto - struct") {
    val messagePath = testFile("protobuf/simple_message.desc").replace("file:/", "/")
    val df = spark.range(10).select(
      struct(
        $"id",
        $"id".cast("string").as("string_value"),
        $"id".cast("int").as("int32_value"),
        $"id".cast("int").as("uint32_value"),
        $"id".cast("long").as("int64_value"),
        $"id".cast("long").as("uint64_value"),
        $"id".cast("double").as("double_value"),
        lit(1202.00).cast(org.apache.spark.sql.types.FloatType).as("float_value"),
        lit(true).as("bool_value"),
        lit("0".getBytes).as("bytes_value")
      ).as("SimpleMessage")
    )
    val protoStructDF = df.select(functions.to_proto($"SimpleMessage", messagePath, "SimpleMessage").as("proto"))
    val actualDf = protoStructDF.select(functions.from_proto($"proto", messagePath, "SimpleMessage").as("proto.*"))
    checkAnswer(actualDf, df)
  }

  test("roundtrip in from_proto and to_proto - Repeated") {
    val messagePath = testFile("protobuf/message_with_repeated.desc").replace("file:/", "/")
    val repeatedMessage = SimpleMessageRepeated.newBuilder()
      .setKey("key")
      .setValue("value")
      .addRstringValue("array_string_value")
      .addRint32Value(1092)
      .addRint64Value(1090902)
      .addRbytesValue(com.google.protobuf.ByteString.copyFrom("0".getBytes))
      .addRboolValue(false)
      .addRboolValue(true)
      .addRdoubleValue(1092092)
      .addRdoubleValue(1092093)
      .addRfloatValue(10903.0f)
      .addRfloatValue(10902.0f)
      .build()

    val df = Seq(repeatedMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", messagePath, "SimpleMessageRepeated").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", messagePath, "SimpleMessageRepeated").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Map") {
    val messagePath = testFile("protobuf/message_with_map.desc").replace("file:/", "/")
    val repeatedMessage = SimpleMessageMap.newBuilder()
      .setKey("key")
      .setValue("value")
      .putBoolMapdata(true, true)
      .putDoubleMapdata("double", 20930930)
      .putInt64Mapdata(109209092, 920920920)
      .putStringMapdata("key", "value")
      .putStringMapdata("key1", "value1")
      .putInt32Mapdata(1092, 902)
      .putFloatMapdata("float", 10920.0f)
      .build()
    val df = Seq(repeatedMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", messagePath, "SimpleMessageMap").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", messagePath, "SimpleMessageMap").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", messagePath, "SimpleMessageMap").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Enum") {
    val messagePath = testFile("protobuf/message_with_enum.desc").replace("file:/", "/")
    val repeatedMessage = SimpleMessageEnum.newBuilder()
      .setKey("key")
      .setValue("value")
      .setBasicEnum(BasicEnum.FIRST)
      .setBasicEnum(BasicEnum.NOTHING)
      .setNestedEnum(NestedEnum.NESTED_FIRST)
      .setNestedEnum(NestedEnum.NESTED_SECOND)
      .build()
    val df = Seq(repeatedMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", messagePath, "SimpleMessageEnum").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", messagePath, "SimpleMessageEnum").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", messagePath, "SimpleMessageEnum").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("roundtrip in from_proto and to_proto - Multiple Message") {
    val messagePath = testFile("protobuf/message_multiple_message.desc").replace("file:/", "/")
    val repeatedMessage = MultipleExample.newBuilder()
      .setIncludedExample(
        IncludedExample.newBuilder().setIncluded("included_value").setOther(
          OtherExample.newBuilder().setOther("other_value"))
      ).build()
    val df = Seq(repeatedMessage.toByteArray).toDF("value")
    val fromProtoDF = df.select(functions.from_proto($"value", messagePath, "MultipleExample").as("value_from"))
    val toProtoDF = fromProtoDF.select(functions.to_proto($"value_from", messagePath, "MultipleExample").as("value_to"))
    val toFromProtoDF = toProtoDF.select(functions.from_proto($"value_to", messagePath, "MultipleExample").as("value_to_from"))
    checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
  }

  test("test Proto Schema") {
    val MULTIPLE_EXAMPLE = "protobuf/protobuf_multiple_message.desc"
    val desc = ProtoUtils.buildDescriptor(testFile(MULTIPLE_EXAMPLE).replace("file:/", "/"), "MultipleExample")
  }

  test("handle invalid input in from_proto") {

  }

  test("roundtrip in to_proto and from_proto - with null") {
    val messagePath = testFile("protobuf/simple_message.desc").replace("file:/", "/")
    val df = spark.range(10).select(
      struct(
        $"id",
        lit(null).cast("string").as("string_value"),
        $"id".cast("int").as("int32_value"),
        $"id".cast("int").as("uint32_value"),
        $"id".cast("int").as("sint32_value"),
        $"id".cast("int").as("fixed32_value"),
        $"id".cast("int").as("sfixed32_value"),
        $"id".cast("long").as("int64_value"),
        $"id".cast("long").as("uint64_value"),
        $"id".cast("long").as("sint64_value"),
        $"id".cast("long").as("fixed64_value"),
        $"id".cast("long").as("sfixed64_value"),
        $"id".cast("double").as("double_value"),
        lit(1202.00).cast(org.apache.spark.sql.types.FloatType).as("float_value"),
        lit(true).as("bool_value"),
        lit("0".getBytes).as("bytes_value")
      ).as("SimpleMessage")
    )

    intercept[SparkException] {
      df.select(functions.to_proto($"SimpleMessage", messagePath, "SimpleMessage").as("proto")).collect()
    }
  }

  test("roundtrip in to_proto and from_proto - struct with nullable Proto schema") {

  }
  test("to_proto with unsupported nullable Proto schema") {

  }
}