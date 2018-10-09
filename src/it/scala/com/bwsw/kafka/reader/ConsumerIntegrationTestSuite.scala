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

import com.bwsw.kafka.reader.entities.{Offset, TopicWithPartition}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, Outcome, fixture}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class ConsumerIntegrationTestSuite extends fixture.FlatSpec
  with Matchers
  with TableDrivenPropertyChecks
  with EmbeddedKafka {

  val groupId = "group1"
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

  "poll" should "retrieve all events from single Kafka topic even an offset is incorrect if autoOffsetReset = earliest" in { fixture =>
    var offset = 0
    val key = "key"
    val topic = getNextTopic
    val partition = 0
    val testDataSet = createSetWithTestData(topic, countOfTestElementForTopic).toList
    val expectedRecords = testDataSet.map {
      case (topicName, strValue) =>
        val record = TestConsumerRecord[String, String](
          topicName,
          partition,
          offset,
          key,
          strValue
        )
        offset += 1

        record
    }

    val incorrectOffsets = Map(TopicWithPartition(topic, partition) -> Offset.Concrete(countOfTestElementForTopic * 2))

    val producerRecords = testDataSet.map {
      case (topicName, strValue) => new ProducerRecord[String, String](topicName, partition, key, strValue)
    }

    fixture.producer.send(producerRecords)

    val consumer = Consumer[String, String](Consumer.Settings(fixture.kafkaEndpoints, groupId, pollTimeout))

    consumer.assignWithOffsets(incorrectOffsets)

    val records = consumer.poll().asScala.toList.map(x => TestConsumerRecord(x.topic(), x.partition(), x.offset(), x.key(), x.value()))

    records.size shouldBe countOfTestElementForTopic
    records should contain theSameElementsAs expectedRecords
  }

  it should "not retrieve any event from single Kafka topic if an offset is incorrect and autoOffsetReset = latest" in { fixture =>
    val key = "key"
    val topic = getNextTopic
    val partition = 0
    val testDataSet = createSetWithTestData(topic, countOfTestElementForTopic).toList

    val incorrectOffsets = Map(TopicWithPartition(topic, partition) -> Offset.Concrete(countOfTestElementForTopic * 2))

    val producerRecords = testDataSet.map {
      case (topicName, strValue) => new ProducerRecord[String, String](topicName, partition, key, strValue)
    }

    fixture.producer.send(producerRecords)

    val consumer = Consumer[String, String](Consumer.Settings(fixture.kafkaEndpoints, groupId, pollTimeout, autoOffsetReset = "latest"))

    consumer.assignWithOffsets(incorrectOffsets)

    val records = consumer.poll().asScala.toList

    records shouldBe empty
  }

  it should "throw an exception if an offset is incorrect and autoOffsetReset = none" in { fixture =>
    val key = "key"
    val topic = getNextTopic
    val partition = 0
    val testDataSet = createSetWithTestData(topic, countOfTestElementForTopic).toList

    val incorrectOffsets = Map(TopicWithPartition(topic, partition) -> Offset.Concrete(countOfTestElementForTopic * 2))

    val producerRecords = testDataSet.map {
      case (topicName, strValue) => new ProducerRecord[String, String](topicName, partition, key, strValue)
    }

    fixture.producer.send(producerRecords)

    val consumer = Consumer[String, String](Consumer.Settings(fixture.kafkaEndpoints, groupId, pollTimeout, autoOffsetReset = "none"))

    consumer.assignWithOffsets(incorrectOffsets)

    assertThrows[OffsetOutOfRangeException](consumer.poll())
  }

  private def getNextTopic: String = {
    topicNumber = topicNumber + 1
    s"topic$topicNumber"
  }

  private def createSetWithTestData(data: String, count: Int): Set[(String, String)] = (1 to count).map { x =>
    data -> s"$data + $x"
  }.toSet

  case class TestConsumerRecord[K, V](topic: String, partition: Int, offset: Long, key: K, value: V)

}
