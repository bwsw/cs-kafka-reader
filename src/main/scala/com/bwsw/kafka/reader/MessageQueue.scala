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

import com.bwsw.kafka.reader.entities.InputEnvelope
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
  * Class is an intermediate buffer of Kafka events used to decrease a count of requests to Kafka
  * if there is a need to retrieve a small number of messages, e.g. one.
  * Class retrieves a list of ConsumerRecords and convert each of them to InputEnvelope
  *
  * @tparam K type of [[org.apache.kafka.clients.consumer.ConsumerRecord]] key
  * @tparam V type of [[org.apache.kafka.clients.consumer.ConsumerRecord]] value
  * @param consumer see [[com.bwsw.kafka.reader.Consumer[K,V] ]]
  */
class MessageQueue[K,V](consumer: Consumer[K,V]) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var buffer = new ListBuffer[InputEnvelope[V]]

  /**
    * Retrieves 'n' InputEnvelope from the buffer,
    * if buffer does not have enough entities, it tries to retrieve the available data from the consumer
    *
    * @param n count of InputEnvelope entities to extract
    */
  def take(n: Int): List[InputEnvelope[V]] = {
    logger.trace(s"take(n: $n)")
    if (buffer.size < n) {
      logger.debug("Count of entities in buffer is not enough, new entities will be retrieved from Kafka")
      fill()
    }

    val sizeAfterFill = buffer.size
    logger.debug(s"Count of entities in buffer after retrieving from Kafka is: $sizeAfterFill")

    val envelopes = if (sizeAfterFill < n) {
      buffer.take(sizeAfterFill)
    } else {
      buffer.take(n)
    }
    logger.debug(s"The following entities: $envelopes will be returned")

    buffer.remove(0, envelopes.size)
    envelopes.toList
  }

  /**
    * Retrieves a list of ConsumerRecords, convert each of them to InputEnvelope and put to a buffer
    */
  private def fill(): Unit = {
    logger.trace("fill()")
    
    val records = consumer.poll().asScala
    logger.debug(s"Record: $records retrieved from Kafka")

    val envelopes = records.map { record =>
      new InputEnvelope[V](record.topic(), record.partition(), record.offset(), record.value())
    }.toList
    buffer ++= envelopes
  }
}
