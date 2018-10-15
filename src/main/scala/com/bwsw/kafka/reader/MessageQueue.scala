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
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

/**
  * Class is an intermediate buffer of Kafka events used to decrease a count of requests to Kafka
  * if there is a need to retrieve a small number of messages, e.g. one.
  * Class retrieves a list of ConsumerRecords and convert each of them to InputEnvelope.
  * This class automatically poll messages from Kafka if it doesn't contain enough messages.
  * Autopolling starts after calling one of methods: `start()`, `takeOne()` or `take(n)`.
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
  private val isShutdown = new AtomicBoolean(false)

  implicit private val executionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())


  /**
    * Retrieves and removes first element of the queue.
    * If queue is empty, it waits up to the `consumer.settings.pollTimeout` milliseconds
    * for an element to become available
    */
  def takeOne(): Option[InputEnvelope[V]] = {
    ensureNotShutdown()
    logger.trace("takeOne()")
    val maybeEnvelope = Option(buffer.poll(pollTimeout, TimeUnit.MILLISECONDS))
    pollIfNeeded()

    maybeEnvelope
  }

  /**
    * Retrieves and removes first `n` elements of the queue.
    * If queue is empty, it waits up to the `consumer.settings.pollTimeout` milliseconds
    * for an element to become available
    *
    * @param n count of elements to extract
    */
  def take(n: Int): List[InputEnvelope[V]] = {
    ensureNotShutdown()
    logger.trace(s"take(n: $n)")

    val javaList = new java.util.LinkedList[InputEnvelope[V]]
    buffer.drainTo(javaList, n)

    if (javaList.isEmpty) {
      Option(buffer.poll(pollTimeout, TimeUnit.MILLISECONDS)) match {
        case Some(x) =>
          buffer.drainTo(javaList, n - 1)
          javaList.addFirst(x)
        case None =>
      }
    }
    pollIfNeeded()

    javaList.asScala.toList
  }

  /**
    * Start autopolling
    */
  def start(): Unit = {
    ensureNotShutdown()
    pollIfNeeded()
  }

  /**
    * Shutdown message queue and free resources
    */
  def shutdown(): Unit = {
    if (!isShutdown.getAndSet(true)) {
      executionContext.shutdown()
    }
  }

  /**
    * Poll messages from Kafka if the queue doesn't contain enough elements
    */
  private def pollIfNeeded(): Unit = {
    if (buffer.size() < minMessages) {
      if (!isBusy.getAndSet(true)) {
        Future(fill())
      }
    }
  }


  private def fill(): Unit = {
    if (buffer.size() < minMessages && !isShutdown.get()) {
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

  /*
   * Check that the consumer hasn't been shutdown.
   */
  private def ensureNotShutdown(): Unit = {
    if (isShutdown.get()) {
      throw new IllegalStateException("This message has already been shutdown")
    }
  }
}
