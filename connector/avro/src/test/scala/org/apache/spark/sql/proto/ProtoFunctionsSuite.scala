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

import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.proto.SimpleMessageProtos.SimpleMessage

import java.io.{ByteArrayOutputStream, InputStream}

class ProtoFunctionsSuite extends QueryTest with SharedSparkSession with Serializable {
  import testImplicits._

  test("roundtrip in to_proto and from_proto - int and string") {
    //val df = spark.range(10).select($"id", $"id".cast("string").as("str"))
    val simpleMessage = SimpleMessage.newBuilder()
      .setKey("123")
      .setQuery("spark-query")
      .setTstamp(12090)
      .setResultsPerPage(123)
      .addArrayKey("value1")
      .addArrayKey("value2")
      //.setOther(otherMessage)
      .build()

    val df = Seq(simpleMessage.toByteArray).toDF("value")
    val simpleMessageObj = SimpleMessage.newBuilder().build()
    val SIMPLE_MESSAGE = "protobuf/simple_message.desc"
    val simpleMessagePath = testFile(SIMPLE_MESSAGE).replace("file:/", "/")
    val dfRes = df.select(functions.from_proto($"value", simpleMessagePath, Some("SimpleMessage")).as("value"))
    dfRes.select($"value.*").show()
    dfRes.printSchema()
    val dfRes2 = dfRes.select(functions.to_proto($"value", simpleMessageObj).as("value2"))
    dfRes2.show()
    val dfRes3 = dfRes2.select(functions.from_proto($"value2", simpleMessagePath, Some("SimpleMessage")).as("value3"))
    dfRes3.select($"value3.*").show()
    dfRes3.printSchema()
    val MULTIPLE_EXAMPLE = "protobuf/protobuf_multiple_message.desc"
    val desc = ProtoUtils.buildDescriptor(testFile(MULTIPLE_EXAMPLE).replace("file:/", "/"), "MultipleExample")
    println(desc.toProto.toString)
  }

  def parseSchema(intputStream : InputStream) = {
    val buf = new Array[Byte](4096)
    val baos = new ByteArrayOutputStream()
    var len: Int = intputStream.read(buf)
    while(len > 0) {
      baos.write(buf, 0, len)
      len = intputStream.read(buf)
    }
    FileDescriptorSet.parseFrom(baos.toByteArray())
  }
  //  public static DynamicSchema parseFrom (InputStream schemaDescIn) throws Descriptors.DescriptorValidationException
  //  , IOException {
  //    try {
  //      byte[] buf = new byte[4096];
  //      ByteArrayOutputStream baos = new ByteArrayOutputStream();
  //
  //      int len;
  //      while ((len = schemaDescIn.read(buf)) > 0) {
  //        baos.write(buf, 0, len);
  //      }
  //
  //      DynamicSchema var4 = parseFrom(baos.toByteArray());
  //      return var4;
  //    } finally {
  //      schemaDescIn.close();
  //    }
  //  }
}