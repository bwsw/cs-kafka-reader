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
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class MessageQueueTestSuite
  extends fixture.FlatSpec
    with Matchers
    with MockitoSugar {

  final case class FixtureParam(consumer: Consumer[String, String],
                                messageQueue: MessageQueue[String, String])

  private val topic = "topic1"
  private val partition = 0
  private val topicPartition = new TopicPartition(topic, partition)
  private val minMessagesCount = 3
  private val pollTimeout: Long = 500
  private val shortTimeout: Long = 100
  private val longTimeout: Long = 1000

  private val offsetsCount = 10
  private val records = (0 until offsetsCount).map { offset =>
    new ConsumerRecord[String, String](topic, partition, offset, s"key-$offset", s"value-$offset")
  }
  private val envelopes = (0 until offsetsCount).map { offset =>
    InputEnvelope(topic, partition, offset, s"value-$offset")
  }
  private val batches = records.grouped(2).toSeq.map { batch =>
    new ConsumerRecords[String, String](
      Map(topicPartition -> batch.asJava).asJava
    )
  }

  private val consumerSettings = Consumer.Settings(
    brokers = "brokers",
    groupId = "groupId",
    pollTimeout = pollTimeout.toInt
  )

  override def withFixture(test: OneArgTest): Outcome = {
    val consumer = mock[Consumer[String, String]]
    when(consumer.settings).thenReturn(consumerSettings)
    when(consumer.poll())
      .thenReturn(batches.head, batches(1))
      .thenAnswer({ _ =>
        Thread.sleep(shortTimeout)
        batches(2)
      })
      .thenAnswer({ _ =>
        Thread.sleep(longTimeout)
        batches(3)
      })

    val messageQueue = new MessageQueue(consumer, minMessagesCount)
    val fixture = FixtureParam(consumer, messageQueue)
    messageQueue.pollIfNeeded()

    test(fixture)
  }


  "MessageQueue" should "poll messages until queue.size >= minMessagesCount" in { fixture =>
    import fixture._

    Thread.sleep(shortTimeout)
    verify(consumer, times(2)).poll()
  }

  it should "poll messages if queue.size < minMessagesCount" in { fixture =>
    import fixture._

    Thread.sleep(shortTimeout)
    verify(consumer, times(2)).poll()

    messageQueue.takeOne() shouldBe Some(envelopes.head)
    messageQueue.takeOne() shouldBe Some(envelopes(1))

    Thread.sleep(shortTimeout)
    verify(consumer, times(3)).poll()

    messageQueue.take(10) should contain theSameElementsInOrderAs envelopes.slice(2, 4) //scalastyle:ignore

    Thread.sleep(longTimeout)
    verify(consumer, Mockito.atLeast(4)).poll() //scalastyle:ignore
  }


  "takeOne" should "return message immediately if queue isn't empty" in { fixture =>
    import fixture._

    Thread.sleep(shortTimeout)
    verify(consumer, times(2)).poll()

    fasterThan(shortTimeout) {
      messageQueue.takeOne() shouldBe Some(envelopes.head)
    }
  }

  it should "wait 'pollTimeout' milliseconds if queue is empty" in { fixture =>
    import fixture._

    Thread.sleep(shortTimeout)
    verify(consumer, times(2)).poll()

    fasterThan(shortTimeout) {
      messageQueue.takeOne() shouldBe Some(envelopes.head)
      messageQueue.takeOne() shouldBe Some(envelopes(1))
      messageQueue.takeOne() shouldBe Some(envelopes(2))
      messageQueue.takeOne() shouldBe Some(envelopes(3))
    }

    fasterThan(pollTimeout) {
      messageQueue.takeOne() shouldBe Some(envelopes(4)) //scalastyle:ignore
      messageQueue.takeOne() shouldBe Some(envelopes(5)) //scalastyle:ignore
    }

    notFasterThan(pollTimeout) {
      messageQueue.takeOne() shouldBe empty
    }

    Thread.sleep(longTimeout - pollTimeout)
    verify(consumer, Mockito.atLeast(3)).poll()

    fasterThan(shortTimeout) {
      messageQueue.takeOne() shouldBe Some(envelopes(6)) //scalastyle:ignore
    }
  }


  "take(n)" should "return required amount of messages immediately if queue.size >= n" in { fixture =>
    import fixture._

    Thread.sleep(shortTimeout)
    verify(consumer, times(2)).poll()
    fasterThan(shortTimeout) {
      messageQueue.take(2) should contain theSameElementsInOrderAs envelopes.take(2)
    }
  }

  it should "return all messages immediately if queue isn't empty and queue.size < n" in { fixture =>
    import fixture._

    Thread.sleep(shortTimeout)
    verify(consumer, times(2)).poll()
    fasterThan(shortTimeout) {
      messageQueue.take(10) should contain theSameElementsInOrderAs envelopes.take(4) //scalastyle:ignore
    }
  }

  it should "wait 'pollTimeout' milliseconds if queue is empty" in { fixture =>
    import fixture._

    Thread.sleep(shortTimeout)
    verify(consumer, times(2)).poll()
    fasterThan(shortTimeout) {
      messageQueue.take(10) should contain theSameElementsInOrderAs envelopes.take(4) //scalastyle:ignore
    }

    fasterThan(pollTimeout) {
      messageQueue.take(1) should contain only envelopes(4) //scalastyle:ignore
      messageQueue.take(10) should contain only envelopes(5) //scalastyle:ignore
    }

    notFasterThan(pollTimeout) {
      messageQueue.take(10) shouldBe empty //scalastyle:ignore
    }

    Thread.sleep(longTimeout - pollTimeout)
    verify(consumer, Mockito.atLeast(3)).poll()

    fasterThan(shortTimeout) {
      messageQueue.take(2) should contain theSameElementsInOrderAs envelopes.slice(6, 8) //scalastyle:ignore
    }
  }


  private def fasterThan(timeLimit: Long)(f: => Unit) = {
    measureTime(f) should be < timeLimit
  }

  private def notFasterThan(timeLimit: Long)(f: => Unit) = {
    measureTime(f) should be >= timeLimit
  }

  private def measureTime(f: => Unit) = {
    val timestamp = System.currentTimeMillis()
    f
    System.currentTimeMillis() - timestamp
  }
}
