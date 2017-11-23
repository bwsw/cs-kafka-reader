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

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

class Producer[K,V](kafkaEndpoints: String,
                    keySerializer: String = "org.apache.kafka.common.serialization.StringSerializer",
                    valueSerializer: String = "org.apache.kafka.common.serialization.StringSerializer") {
  private val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEndpoints)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)

  private val producer = new KafkaProducer[K,V](props)

  def send(records: List[ProducerRecord[K,V]]): List[RecordMetadata] = {
    records.map { record =>
      val javaFuture = producer.send(record)
      javaFuture.get()
    }
  }

  def close(): Unit = {
    producer.close()
  }

}
