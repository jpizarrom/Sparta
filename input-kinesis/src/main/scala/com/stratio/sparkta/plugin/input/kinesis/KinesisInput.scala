/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.sparta.plugin.input.kinesis

import java.io.{Serializable => JSerializable}

import akka.event.slf4j.SLF4JLogging


import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.google.gson.Gson
import com.stratio.sparta.sdk.Input
import com.stratio.sparta.sdk.ValidatingPropertyMap._

import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kinesis.KinesisUtils

class KinesisInput(properties: Map[String, JSerializable]) extends Input(properties) with SLF4JLogging {
  private val appName : String = properties.getString("appName")
  private val streamName : String = properties.getString("streamName")
  private val endpointUrl : String = properties.getString("endpointUrl")
  private val regionName : String = properties.getString("regionName")
  private val batchLength : String = properties.getString("batchLength")
  private val awsAccessKeyId : String = properties.getString("awsAccessKeyId")
  private val awsSecretKey : String = properties.getString("awsSecretKey")

  def setUp(ssc: StreamingContext, sparkStorageLevel: String): DStream[Row] = {
    KinesisUtils.createStream(
      ssc, appName, streamName, endpointUrl, regionName,
      InitialPositionInStream.LATEST,
      Milliseconds(batchLength.toLong),
      storageLevel(sparkStorageLevel),
      awsAccessKeyId, awsSecretKey)
      .map(byteArray => Row(new String(byteArray)))
  }
}