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

import com.bwsw.kafka.reader.entities.{OutputEnvelope, TopicInfoList, TopicPartitionInfo, TopicPartitionInfoList}
import com.typesafe.scalalogging.Logger

/**
  * Class is responsible for saving and loading checkpoint data for Kafka
  * Override 'save' and 'load' methods if you need to add the custom logic for managing checkpoint data
  *
  * @tparam K type of [[org.apache.kafka.clients.consumer.ConsumerRecord]] key
  * @tparam V type of [[org.apache.kafka.clients.consumer.ConsumerRecord]] value
  * @tparam T type of output data which is stored in [[com.bwsw.kafka.reader.entities.OutputEnvelope[T] ]]
  */
class CheckpointInfoProcessor[K,V,T](topicInfoList: TopicInfoList, consumer: Consumer[K,V]) {
  private val logger = Logger(getClass)
  /**
    * Saves checkpoint data with help of embedded tools of Kafka
    *
    * @param envelopes list of OutputEnvelope entities
    */
  def save(envelopes: List[OutputEnvelope[T]]): Unit = {
    logger.trace(s"save(envelopes: $envelopes)")
    val partitionsInfo = envelopes.map { envelope =>
      TopicPartitionInfo(envelope.topic, envelope.partition, envelope.offset + 1)
    }
    logger.debug(s"PartitionInfo list: '$partitionsInfo' based on OutputEnvelope entities: '$envelopes' will be committed" )
    consumer.commit(TopicPartitionInfoList(partitionsInfo))
  }

  /**
    * Loads checkpoint data and assigns Kafka consumer
    *
    */
  def load(): Unit = {
    logger.trace("load()")
    consumer.assign(topicInfoList)
  }
}
