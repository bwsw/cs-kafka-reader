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

import java.lang.reflect.Field

import com.bwsw.kafka.reader.entities.InputEnvelope
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import org.scalatest.{Outcome, fixture}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class MessageQueueTestSuite extends fixture.FlatSpec {

  case class FixtureParam(messageQueue: MessageQueue[String, String],
                          expectedInputEnvelopes: List[InputEnvelope[String]],
                          buffer: Field)

  def withFixture(test: OneArgTest): Outcome = {
    val topic = "topic1"
    val partition = 0
    val topicPartition = new TopicPartition(topic, partition)

    val offsets = List(0,1,2,3,4,5)
    val data = "data"
    val records = offsets.map { offset =>
      new ConsumerRecord[String, String](topic, partition, offset, "key", data)
    }

    val testConsumer = new Consumer[String, String]("127.0.0.1:9000", "groupId") {
      override protected val consumer: MockConsumer[String, String] = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)

      override def poll(): ConsumerRecords[String, String] = {
        new ConsumerRecords[String, String](Map(topicPartition -> records.asJava).asJava)
      }
    }

    val messageQueue = new MessageQueue[String, String](testConsumer)

    val messageClass = classOf[MessageQueue[String,String]]

    val buffer = messageClass.getDeclaredField("buffer")
    buffer.setAccessible(true)

    val expectedInputEnvelopes = records.map { record =>
      new InputEnvelope[String](record.topic(), record.partition(), record.offset(), data)
    }

    val theFixture = FixtureParam(messageQueue, expectedInputEnvelopes, buffer)

    Try {
      withFixture(test.toNoArgTest(theFixture))
    } match {
      case Success(x) =>
        Try(testConsumer.close())
        x
      case Failure(e: Throwable) =>
        testConsumer.close()
        throw e
    }
  }

  "fill" should "put InputEnvelope list to buffer" in { fixture =>
    val messageClass = classOf[MessageQueue[String,String]]

    val fill = messageClass.getDeclaredMethod("fill")
    fill.setAccessible(true)
    fill.invoke(fixture.messageQueue)

    val buffer = messageClass.getDeclaredField("buffer")
    buffer.setAccessible(true)

    val actualInputEnvelopes = buffer.get(fixture.messageQueue).asInstanceOf[ListBuffer[InputEnvelope[String]]].toList

    assert(actualInputEnvelopes == fixture.expectedInputEnvelopes)
  }

  "take" should "get N InputEnvelopes from buffer if buffer has enough entities" in { fixture =>
    val size = fixture.expectedInputEnvelopes.size
    fixture.buffer.set(fixture.messageQueue, new ListBuffer[InputEnvelope[String]] ++= fixture.expectedInputEnvelopes)

    assert(fixture.expectedInputEnvelopes.take(size - 1) == fixture.messageQueue.take(size - 1))
    assert(fixture.buffer.get(fixture.messageQueue).asInstanceOf[ListBuffer[String]].size == 1)
  }

  "take" should "get N InputEnvelopes from buffer if buffer has enough entities after execution method fill()" in { fixture =>
    val size = fixture.expectedInputEnvelopes.size
    fixture.buffer.set(fixture.messageQueue, new ListBuffer[InputEnvelope[String]])

    assert(fixture.expectedInputEnvelopes.take(size - 1) == fixture.messageQueue.take(size - 1))
    assert(fixture.buffer.get(fixture.messageQueue).asInstanceOf[ListBuffer[String]].size == 1)
  }

  "take" should "get all InputEnvelopes from buffer if buffer does not have enough entities after execution method fill()" in { fixture =>
    val size = fixture.expectedInputEnvelopes.size
    fixture.buffer.set(fixture.messageQueue, new ListBuffer[InputEnvelope[String]])

    assert(fixture.expectedInputEnvelopes.take(size) == fixture.messageQueue.take(size + 1))
    assert(fixture.buffer.get(fixture.messageQueue).asInstanceOf[ListBuffer[String]].isEmpty)
  }
}
