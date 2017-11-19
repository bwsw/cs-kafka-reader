/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package com.bwsw.kafka.reader

import java.util.Properties

import com.bwsw.kafka.reader.entities.{CheckpointInfo, CheckpointInfoList}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class Consumer[K,V](brokers: String,
                    groupId: String,
                    pollTimeout: Int = 500,
                    enableAutoCommit: Boolean = false,
                    autoCommitInterval: Int = 5000) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val props = createConsumerConfig()
  protected val consumer: org.apache.kafka.clients.consumer.Consumer[String, String] = new KafkaConsumer[String, String](props)

  private def createConsumerConfig(): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit.toString)
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval.toString)
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def subscribe(checkpointInfoList: CheckpointInfoList): Unit = {
    consumer.subscribe(checkpointInfoList.infoList.map(_.topicInfo.topic).asJavaCollection)
  }

  def assign(checkpointInfoList: CheckpointInfoList): Unit = {
    val topicPartitionsWithOffsets = checkpointInfoList.infoList.flatMap { checkpointInfo =>
      checkpointInfo.partitionInfoList.map { partitionInfo =>
        (new TopicPartition(checkpointInfo.topicInfo.topic, partitionInfo.partition), partitionInfo.offset)
      }
    }
    consumer.assign(
      topicPartitionsWithOffsets.map {
        case (topicPartition, offset) => topicPartition
      }.asJavaCollection
    )
    topicPartitionsWithOffsets.foreach {
      case (topicPartition, offset) => consumer.seek(topicPartition, offset) }
  }
  def poll(): ConsumerRecords[K,V] = ???
  def commit(checkpointInfoList: CheckpointInfoList): Unit = {}
  def close(): Unit = {}

}
