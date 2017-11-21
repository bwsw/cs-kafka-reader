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

import com.bwsw.kafka.reader.entities._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Class is responsible for events extraction from Kafka.
  * It is a wrapper of [[org.apache.kafka.clients.consumer.KafkaConsumer]]
  *
  */
class Consumer[K,V](brokers: String,
                    groupId: String,
                    pollTimeout: Int = 500,
                    autoOffsetReset: String = "earliest",
                    enableAutoCommit: Boolean = false,
                    autoCommitInterval: Int = 5000) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val props = createConsumerConfig()
  protected val consumer: org.apache.kafka.clients.consumer.Consumer[K, V] = new KafkaConsumer[K, V](props)

  private def createConsumerConfig(): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit.toString)
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval.toString)
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  /**
    * Assign a list of topic/partition to this consumer and
    * use "seek()" method to override the fetch offsets that the consumer will use on the next poll
    *
    * @param topicPartitionInfoList entity which contains topics, partitions and offsets.
    */
  def assignWithOffsets(topicPartitionInfoList: TopicPartitionInfoList): Unit = {
    val topicPartitionsWithOffsets = topicPartitionInfoList.entities.map {
      case TopicPartitionInfo(topic, partition, offset) =>
        (new TopicPartition(topic, partition), offset)
    }
    consumer.assign(
      topicPartitionsWithOffsets.map {
        case (topicPartitions, offset) => topicPartitions
      }.asJavaCollection
    )
    topicPartitionsWithOffsets.foreach {
      case (topicPartition, offset) => consumer.seek(topicPartition, offset)
    }
  }

  /**
    * Assign a list of topic/partition to this consumer
    *
    * @param topicInfoList entity which contains topics
    * @throws NoSuchElementException if no one of the topics does not exist in Kafka
    */
  def assign(topicInfoList: TopicInfoList): Unit = {
    val topicPartitions = covertToTopicPartition(topicInfoList).asJavaCollection
    if (!topicPartitions.isEmpty) {
      consumer.assign(topicPartitions)
    } else {
      throw new NoSuchElementException(s"No one of topics: $topicInfoList does not exist")
    }
  }

  /**
    * @see [[org.apache.kafka.clients.consumer.KafkaConsumer#poll(timeout: Long)]]
    */
  def poll(): ConsumerRecords[K,V] = {
    consumer.poll(pollTimeout)
  }

  /**
    * Commit the specified offsets for the specified list of topics and partitions.
    */
  def commit(topicPartitionInfoList: TopicPartitionInfoList): Unit = {
    val topicPartitionsWithMetadata = topicPartitionInfoList.entities.map { topicPartitionInfo =>
      new TopicPartition(topicPartitionInfo.topic, topicPartitionInfo.partition) -> new OffsetAndMetadata(topicPartitionInfo.offset, "")
    }.toMap.asJava
    consumer.commitSync(topicPartitionsWithMetadata)
  }

  /**
    * Close the consumer
    */
  def close(): Unit = {
    consumer.close()
  }

  protected def covertToTopicPartition(topicInfoList: TopicInfoList): List[TopicPartition] = {
    consumer.listTopics().asScala.toList.collect {
      case (consumerTopic, partitionInfoList) if topicInfoList.entities.map(_.topic).contains(consumerTopic) =>
        partitionInfoList.asScala.map { partitionInfo =>
          new TopicPartition(consumerTopic, partitionInfo.partition())
        }.toList
    }.flatten
  }

}
