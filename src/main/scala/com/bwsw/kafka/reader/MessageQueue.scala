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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

import com.bwsw.kafka.reader.entities.InputEnvelope
import com.typesafe.scalalogging.Logger

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Class is an intermediate buffer of Kafka events used to decrease a count of requests to Kafka
  * if there is a need to retrieve a small number of messages, e.g. one.
  * Class retrieves a list of ConsumerRecords and convert each of them to InputEnvelope
  *
  * @tparam K type of [[org.apache.kafka.clients.consumer.ConsumerRecord]] key
  * @tparam V type of [[org.apache.kafka.clients.consumer.ConsumerRecord]] value
  * @param consumer    see [[com.bwsw.kafka.reader.Consumer[K,V] ]]
  * @param minMessages queue automatically poll messages if it contains messages less than this number
  */
class MessageQueue[K, V](consumer: Consumer[K, V],
                         minMessages: Int = 1) {

  import consumer.settings.pollTimeout

  private val logger = Logger(getClass)
  private val buffer = new LinkedBlockingQueue[InputEnvelope[V]]
  private val isBusy = new AtomicBoolean(false)

  implicit private val executionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())


  /**
    * Retrieves and removes first element of the queue.
    * If queue is empty, it waits up to the `consumer.settings.pollTimeout` milliseconds
    * for an element to become available
    */
  def takeOne(): Option[InputEnvelope[V]] = {
    logger.trace("takeOne()")
    pollIfNeeded()
    val maybeEnvelope = Option(buffer.poll(pollTimeout, TimeUnit.MILLISECONDS))
    pollIfNeeded()

    maybeEnvelope
  }

  /**
    * Retrieves and removes first `n` elements of the queue.
    * If queue is empty, it waits up to the `consumer.settings.pollTimeout` milliseconds
    * for an element to become available
    *
    * @param n count of elements entities to extract
    */
  def take(n: Int): List[InputEnvelope[V]] = {
    logger.trace(s"take(n: $n)")

    val javaList = new java.util.LinkedList[InputEnvelope[V]]
    buffer.drainTo(javaList, n)
    pollIfNeeded()

    if (javaList.isEmpty) {
      Option(buffer.poll(pollTimeout, TimeUnit.MILLISECONDS)) match {
        case Some(x) =>
          buffer.drainTo(javaList, n - 1)
          pollIfNeeded()
          x :: javaList.asScala.toList
        case None => Nil
      }
    } else {
      javaList.asScala.toList
    }
  }


  /**
    * Poll messages from Kafka if the queue doesn't contain enough elements
    */
  def pollIfNeeded(): Unit = {
    if (buffer.size() < minMessages) {
      if (!isBusy.getAndSet(true)) {
        Future(fill())
      }
    }
  }


  private def fill(): Unit = {
    if (buffer.size() < minMessages) {
      logger.trace("fill()")

      val records = consumer.poll().asScala
      logger.debug(s"Record: $records retrieved from Kafka")

      val envelopes = records.map { record =>
        new InputEnvelope[V](record.topic(), record.partition(), record.offset(), record.value())
      }
      buffer.addAll(envelopes.asJavaCollection)

      fill()
    } else {
      isBusy.set(false)
    }
  }
}
