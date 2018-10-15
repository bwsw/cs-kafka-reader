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

import com.bwsw.kafka.reader.entities.{TopicInfo, TopicInfoList}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{Matchers, Outcome, fixture}

import scala.util.{Failure, Success, Try}


class KafkaReaderIntegrationTest
  extends fixture.FlatSpec
    with Matchers
    with EmbeddedKafka {

  val groupId = "group1"
  val dummyFlag = new AtomicBoolean(true)
  var topicNumber = 0
  val countOfTestElementForTopic = 5
  val pollTimeout = 3000

  case class FixtureParam(producer: Producer[String, String], kafkaEndpoints: String)

  def withFixture(test: OneArgTest): Outcome = {
    withRunningKafkaOnFoundPort(EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)) { kafkaConfig =>
      val kafkaEndpoints = s"localhost:${kafkaConfig.kafkaPort}"

      val producer = new Producer[String, String](kafkaEndpoints)

      val theFixture = FixtureParam(producer, kafkaEndpoints)

      Try {
        withFixture(test.toNoArgTest(theFixture))
      } match {
        case Success(x) =>
          Try(producer.close())
          x
        case Failure(e: Throwable) =>
          producer.close()
          throw e
      }
    }
  }

  it should "process all events from single Kafka topic, save checkpoint data (indicating what events have been processed), " +
    "run from a scratch and not receive any message" in { fixture =>
    val topic = getNextTopic
    val topicInfoList = TopicInfoList(List(TopicInfo(topic)))
    val testDataSet = createSetWithTestData(topic, countOfTestElementForTopic)
    val producerRecords = testDataSet.map {
      case (topicData, value) => new ProducerRecord[String, String](topicData, 0, "key", value)
    }

    fixture.producer.send(producerRecords.toList)

    val testDataSetSize = testDataSet.size

    testForNonEmptyTopics(
      fixture.kafkaEndpoints,
      groupId,
      topicInfoList,
      retrievedCount = testDataSetSize,
      expectedCount = testDataSetSize
    )

    testForEmptyTopics(fixture.kafkaEndpoints, groupId, topicInfoList)
  }


  it should "process all events from multiple Kafka topics, save checkpoint data (indicating what events have been processed), " +
    "run from a scratch and not receive any message" in { fixture =>
    val topic = getNextTopic
    val topic2 = getNextTopic
    val topicInfoList = TopicInfoList(List(TopicInfo(topic), TopicInfo(topic2)))
    val testDataSet = createSetWithTestData(topic, countOfTestElementForTopic) ++ createSetWithTestData(topic2, countOfTestElementForTopic)
    val producerRecords = testDataSet.map {
      case (topicData, value) => new ProducerRecord[String, String](topicData, 0, "key", value)
    }

    fixture.producer.send(producerRecords.toList)

    val testDataSetSize = testDataSet.size

    testForNonEmptyTopics(
      fixture.kafkaEndpoints,
      groupId,
      topicInfoList,
      retrievedCount = testDataSetSize,
      expectedCount = testDataSetSize
    )

    testForEmptyTopics(fixture.kafkaEndpoints, groupId, topicInfoList)
  }

  it should "process some part of events from single Kafka topic, save checkpoint data (indicating what events have been processed). " +
    "Next, run from a scratch and repeat the two first steps for the rest of events. " +
    "Then run again from a scratch and it should not receive any message" in { fixture =>
    val topic = getNextTopic
    val topicInfoList = TopicInfoList(List(TopicInfo(topic)))
    val testDataSet = createSetWithTestData(topic, countOfTestElementForTopic)
    val producerRecords = testDataSet.map {
      case (topicData, value) => new ProducerRecord[String, String](topicData, 0, "key", value)
    }

    fixture.producer.send(producerRecords.toList)

    val testDataSetSize = testDataSet.size

    testForNonEmptyTopics(
      fixture.kafkaEndpoints,
      groupId,
      topicInfoList,
      retrievedCount = testDataSetSize / 2,
      expectedCount = testDataSetSize / 2
    )

    testForNonEmptyTopics(
      fixture.kafkaEndpoints,
      groupId,
      topicInfoList,
      retrievedCount = testDataSetSize,
      expectedCount = testDataSetSize - testDataSetSize / 2
    )

    testForEmptyTopics(fixture.kafkaEndpoints, groupId, topicInfoList)
  }

  it should "process some part of events from multiple Kafka topics, save checkpoint data (indicating what events have been processed), " +
    "repeat it for remains part of events, run from a scratch and not receive any message" in { fixture =>
    val topic = getNextTopic
    val topic2 = getNextTopic
    val topicInfoList = TopicInfoList(List(TopicInfo(topic), TopicInfo(topic2)))
    val testDataSet = createSetWithTestData(topic, countOfTestElementForTopic) ++ createSetWithTestData(topic2, countOfTestElementForTopic)
    val producerRecords = testDataSet.map {
      case (topicData, value) => new ProducerRecord[String, String](topicData, 0, "key", value)
    }

    fixture.producer.send(producerRecords.toList)

    val testDataSetSize = testDataSet.size

    testForNonEmptyTopics(
      fixture.kafkaEndpoints,
      groupId,
      topicInfoList,
      retrievedCount = testDataSetSize / 2,
      expectedCount = testDataSetSize / 2
    )

    testForNonEmptyTopics(
      fixture.kafkaEndpoints,
      groupId,
      topicInfoList,
      retrievedCount = testDataSetSize,
      expectedCount = testDataSetSize - testDataSetSize / 2
    )

    testForEmptyTopics(fixture.kafkaEndpoints, groupId, topicInfoList)
  }

  private def testForNonEmptyTopics(kafkaEndpoints: String,
                                    consumerGroupId: String,
                                    topicInfoList: TopicInfoList,
                                    retrievedCount: Int,
                                    expectedCount: Int): Unit = {

    val testEntities = createTestEntities[String, String, String](
      kafkaEndpoints,
      consumerGroupId,
      topicInfoList,
      retrievedCount
    )

    testEntities.checkpointInfoProcessor.load()
    testEntities.messageQueue.pollIfNeeded()
    Thread.sleep(pollTimeout)

    val outputEnvelopes = testEntities.eventHandler.handle(dummyFlag)

    val actualTestDataList = outputEnvelopes.map { x =>
      x.data
    }

    actualTestDataList should have size expectedCount

    testEntities.checkpointInfoProcessor.save(outputEnvelopes)

    testEntities.consumer.close()
  }

  private def testForEmptyTopics(kafkaEndpoints: String,
                                 consumerGroupId: String,
                                 topicInfoList: TopicInfoList): Unit = {
    val testEntities = createTestEntities[String, String, String](
      kafkaEndpoints,
      consumerGroupId,
      topicInfoList,
      countOfMessages = 10 //scalastyle:ignore
    )

    testEntities.checkpointInfoProcessor.load()
    testEntities.messageQueue.pollIfNeeded()
    Thread.sleep(pollTimeout)

    testEntities.eventHandler.handle(dummyFlag) shouldBe empty

    testEntities.consumer.close()
  }

  private def getNextTopic: String = {
    topicNumber = topicNumber + 1
    s"topic$topicNumber"
  }

  private def createSetWithTestData(data: String, count: Int): Set[(String, String)] = (1 to count).map { x =>
    data -> s"$data + $x"
  }.toSet

  private def createTestEntities[K, V, T](kafkaEndpoints: String,
                                          consumerGroupId: String,
                                          topicInfoList: TopicInfoList,
                                          countOfMessages: Int): TestEntities[K, V, T] = {

    val consumer = Consumer[K, V](Consumer.Settings(kafkaEndpoints, consumerGroupId, pollTimeout))

    val checkpointInfoProcessor = new CheckpointInfoProcessor[K, V, T](
      topicInfoList,
      consumer
    )

    val messageQueue = new MessageQueue[K, V](consumer, countOfMessages)

    val eventHandler = new MockEventHandler[K, V](messageQueue, countOfMessages)

    TestEntities[K, V, T](consumer, checkpointInfoProcessor, messageQueue, eventHandler)
  }
}
