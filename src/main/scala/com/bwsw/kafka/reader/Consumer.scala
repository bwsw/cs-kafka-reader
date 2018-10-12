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
import org.apache.kafka.common.serialization.Deserializer
import com.typesafe.scalalogging.Logger

import scala.collection.JavaConverters._

/**
  * Class is responsible for events extraction from Kafka.
  * It is a wrapper of [[org.apache.kafka.clients.consumer.KafkaConsumer]]
  *
  * @tparam K type of [[org.apache.kafka.clients.consumer.ConsumerRecord]] key
  * @tparam V type of [[org.apache.kafka.clients.consumer.ConsumerRecord]] value
  * @param settings settings for Kafka Consumer
  */
class Consumer[K, V](kafkaConsumer: org.apache.kafka.clients.consumer.Consumer[K, V],
                     settings: Consumer.Settings) {

  private val logger = Logger(getClass)

  /**
    * Assign a list of topic/partition to this consumer and
    * use "seek()" method to override the fetch offsets that the consumer will use on the next poll
    *
    * @param topicPartitionInfoList entity which contains topics, partitions and offsets.
    */
  def assignWithOffsets(topicPartitionInfoList: TopicPartitionInfoList): Unit = {
    logger.trace(s"assignWithOffsets(topicPartitionInfoList: $topicPartitionInfoList)")
    val offsets = topicPartitionInfoList.entities.map {
      case TopicPartitionInfo(topic, partition, offset) =>
        TopicWithPartition(topic, partition) -> Offset.Concrete(offset)
    }.toMap

    assignWithOffsets(offsets)
  }

  /**
    * Assign a list of topic/partition to this consumer and
    * use "seek()" method to override the fetch offsets that the consumer will use on the next poll
    *
    * @param offsets contains pairs (topic, partition) with offsets
    */
  def assignWithOffsets(offsets: Map[TopicWithPartition, Offset]): Unit = {
    logger.trace(s"assignWithOffsets(offsets: $offsets)")
    val topicPartitionsWithOffsets = offsets.map {
      case (topicPartition, offset) =>
        topicPartition.toTopicPartition -> offset
    }

    val topicPartitions = topicPartitionsWithOffsets.keys

    logger.debug(s"Assign the topic partitions: $topicPartitions")
    kafkaConsumer.assign(topicPartitions.asJavaCollection)

    logger.debug(s"Seek topic partitions with offsets: $topicPartitionsWithOffsets")
    topicPartitionsWithOffsets.foreach {
      case (partition, Offset.Beginning) => kafkaConsumer.seekToBeginning(Seq(partition).asJava)
      case (partition, Offset.End) => kafkaConsumer.seekToEnd(Seq(partition).asJava)
      case (partition, Offset.Concrete(offset)) => kafkaConsumer.seek(partition, offset)
    }
  }

  /**
    * Assign a list of topic/partition to this consumer
    *
    * @param topicInfoList entity which contains topics
    * @throws NoSuchElementException if no one of topics exists in Kafka
    */
  def assign(topicInfoList: TopicInfoList): Unit = {
    logger.trace(s"topicInfoList: $topicInfoList")
    val topicPartitions = convertToTopicPartition(topicInfoList)
    if (topicPartitions.nonEmpty) {
      logger.debug(s"Assign the topic partitions: $topicPartitions")
      kafkaConsumer.assign(topicPartitions.asJavaCollection)
    } else {
      logger.error(s"No one of topics: $topicInfoList exists, NoSuchElementException will be thrown")
      throw new NoSuchElementException(s"No one of topics: $topicInfoList exists")
    }

    val topicPartitionsWithOffsets = topicPartitions.map { partition =>
      (partition, kafkaConsumer.position(partition))
    }

    logger.debug(s"Seek topic partitions with offsets: $topicPartitionsWithOffsets.")
    topicPartitionsWithOffsets.foreach {
      case (partition, offset) => kafkaConsumer.seek(partition, offset)
    }
  }

  /**
    * @see [[org.apache.kafka.clients.consumer.KafkaConsumer#poll(timeout: Long)]]
    */
  def poll(): ConsumerRecords[K, V] = {
    logger.trace("poll()")
    kafkaConsumer.poll(settings.pollTimeout)
  }

  /**
    * Commit the specified offsets for the specified list of topics and partitions.
    */
  def commit(topicPartitionInfoList: TopicPartitionInfoList): Unit = {
    logger.trace(s"commit(topicPartitionInfoList: $topicPartitionInfoList)")
    val topicPartitionsWithMetadata = topicPartitionInfoList.entities.map { topicPartitionInfo =>
      new TopicPartition(topicPartitionInfo.topic, topicPartitionInfo.partition) -> new OffsetAndMetadata(topicPartitionInfo.offset, "")
    }.toMap.asJava
    logger.debug(s"Data for commit is: $topicPartitionsWithMetadata")
    kafkaConsumer.commitSync(topicPartitionsWithMetadata)
  }

  /**
    * Close the consumer
    */
  def close(): Unit = {
    logger.trace("close()")
    kafkaConsumer.close()
  }

  protected def convertToTopicPartition(topicInfoList: TopicInfoList): List[TopicPartition] = {
    logger.trace(s"convertToTopicPartition(topicInfoList: $topicInfoList)")
    val topicPartitions = kafkaConsumer.listTopics().asScala.toList.collect {
      case (consumerTopic, partitionInfoList) if topicInfoList.entities.map(_.topic).contains(consumerTopic) =>
        partitionInfoList.asScala.map { partitionInfo =>
          new TopicPartition(consumerTopic, partitionInfo.partition())
        }.toList
    }.flatten
    logger.debug(s"TopicPartition list: '$topicPartitions' based on topicInfoList: $topicInfoList received")
    topicPartitions
  }

}

object Consumer {

  /**
    * Case class is responsible for keeping Consumer properties
    *
    * @param brokers            Kafka endpoints
    * @param groupId            unique string that identifies the consumer group this consumer belongs to
    * @param pollTimeout        time during which the records are extracted from kafka
    * @param autoOffsetReset    what to do when there is no initial offset in Kafka or if the current offset does not exist
    *                           any more on the server (e.g. because that data has been deleted):
    *                           earliest: automatically reset the offset to the earliest offset
    *                           latest: automatically reset the offset to the latest offset
    *                           none: throw exception to the consumer if no previous offset is found for the consumer's group
    *                           anything else: throw exception to the consumer
    * @param enableAutoCommit   if true the consumer's offset will be periodically committed in the background
    * @param autoCommitInterval frequency in milliseconds that the consumer offsets are auto-committed to Kafka if
    *                           "enableAutoCommit" is set to true
    * @param keyDeserializer    deserializer class for ConsumerRecord key that implements the "Deserializer" interface
    * @param valueDeserializer  deserializer class for ConsumerRecord value that implements the "Deserializer" interface
    */
  case class Settings(brokers: String,
                      groupId: String,
                      pollTimeout: Int = 2000, //scalastyle:ignore
                      autoOffsetReset: String = "earliest",
                      enableAutoCommit: Boolean = false,
                      autoCommitInterval: Int = 5000, //scalastyle:ignore
                      keyDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer",
                      valueDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer")

  /**
    * Creates consumer
    *
    * @param settings settings for Kafka Consumer
    */
  def apply[K, V](settings: Settings): Consumer[K, V] = {
    val props = createConsumerConfig(settings)
    val kafkaConsumer = new KafkaConsumer[K, V](props)

    new Consumer[K, V](kafkaConsumer, settings)
  }

  /**
    * Creates consumer
    *
    * @param settings          settings for Kafka Consumer
    * @param keyDeserializer   key deserializer
    * @param valueDeserializer value deserializer
    */
  def apply[K, V](settings: Settings,
                  keyDeserializer: Deserializer[K],
                  valueDeserializer: Deserializer[V]): Consumer[K, V] = {
    val props = createConsumerConfig(settings)
    val kafkaConsumer = new KafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)

    new Consumer[K, V](kafkaConsumer, settings)
  }


  private def createConsumerConfig[V, K](settings: Settings) = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, settings.groupId)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, settings.autoOffsetReset)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, settings.enableAutoCommit.toString)
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, settings.autoCommitInterval.toString)
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, settings.keyDeserializer)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, settings.valueDeserializer)
    props
  }

}
