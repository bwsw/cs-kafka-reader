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
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{Outcome, fixture}

import scala.util.{Failure, Success, Try}


class KafkaReaderIntegrationTest extends fixture.FlatSpec {
  val groupId = "group1"
  val dummyFlag = new AtomicBoolean(true)
  var topicNumber = 0
  val countOfTestElementForTopic = 5

  case class FixtureParam(producer: Producer[String, String], kafkaEndpoints: String)

  def withFixture(test: OneArgTest): Outcome = {
    val kafkaEndpoints = ApplicationConfig.getRequiredString("app.kafka.endpoints")

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

  it should "process all events from single Kafka topic, save checkpoint data (indicating what events have been processed), " +
    "run from a scratch and not receive any message" in { fixture =>
    val topic = getNextTopic
    val topicInfoList = TopicInfoList(List(TopicInfo(topic)))
    val expectedTestDataList = createListWithTestData(topic, countOfTestElementForTopic)
    val producerRecords = expectedTestDataList.map {
      case (topicData, value) => new ProducerRecord[String, String](topicData, 0, "key", value)
    }

    fixture.producer.send(producerRecords)

    testForNonEmptyTopics(
      fixture.kafkaEndpoints,
      groupId,
      topicInfoList,
      countOfTestElementForTopic,
      expectedTestDataList.map(_._2).toSet
    )

    testForEmptyTopics(fixture.kafkaEndpoints, groupId, topicInfoList)
  }



  it should "process all events from multiple Kafka topics, save checkpoint data (indicating what events have been processed), " +
    "run from a scratch and not receive any message" in { fixture =>
    val topic = getNextTopic
    val topic2 = getNextTopic
    val topicInfoList = TopicInfoList(List(TopicInfo(topic), TopicInfo(topic2)))
    val expectedTestDataList = createListWithTestData(topic, countOfTestElementForTopic) ::: createListWithTestData(topic2, countOfTestElementForTopic)
    val producerRecords = expectedTestDataList.map {
      case (topicData, value) => new ProducerRecord[String, String](topicData, 0, "key", value)
    }

    fixture.producer.send(producerRecords)

    testForNonEmptyTopics(
      fixture.kafkaEndpoints,
      groupId,
      topicInfoList,
      countOfTestElementForTopic * 2,
      expectedTestDataList.map(_._2).toSet
    )

    testForEmptyTopics(fixture.kafkaEndpoints, groupId, topicInfoList)
  }

  private def testForNonEmptyTopics(kafkaEndpoints: String,
                                    consumerGroupId: String,
                                    topicInfoList: TopicInfoList,
                                    retrieveCount: Int,
                                    expectedTestDataSet: Set[String]): Unit = {

    val testEntities = createTestEntities[String, String, String](
      kafkaEndpoints,
      consumerGroupId,
      topicInfoList,
      retrieveCount
    )

    testEntities.checkpointInfoProcessor.load()


    val outputEnvelopes = testEntities.eventHandler.handle(dummyFlag)

    val actualTestDataList = outputEnvelopes.map { x =>
      x.data
    }

    assert(actualTestDataList.toSet == expectedTestDataSet)

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
      countOfMessages = 10
    )

    testEntities.checkpointInfoProcessor.load()

    assert(testEntities.eventHandler.handle(dummyFlag).isEmpty)

    testEntities.consumer.close()
  }

  private def getNextTopic: String = {
    topicNumber = topicNumber + 1
    s"topic$topicNumber"
  }

  private def createListWithTestData(data: String, count: Int): List[(String,String)] = (1 to count).toList.map { x =>
    data -> s"$data + $x"
  }

  private def createTestEntities[K,V,T](kafkaEndpoints: String,
                                        consumerGroupId: String,
                                        topicInfoList: TopicInfoList,
                                        countOfMessages: Int): TestEntities[K,V,T] = {

    val consumer = new Consumer[K,V](Consumer.Settings(kafkaEndpoints, consumerGroupId, 3000))

    val checkpointInfoProcessor = new CheckpointInfoProcessor[K,V,T](
      topicInfoList,
      consumer
    )

    val messageQueue = new MessageQueue[K,V](consumer)

    val eventHandler = new MockEventHandler[K,V](messageQueue, countOfMessages)

    TestEntities[K,V,T](consumer, checkpointInfoProcessor, messageQueue, eventHandler)
  }
}
