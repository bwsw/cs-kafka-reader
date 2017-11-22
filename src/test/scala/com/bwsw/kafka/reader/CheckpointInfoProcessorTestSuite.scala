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

import com.bwsw.kafka.reader.entities._
import org.apache.kafka.clients.consumer.{MockConsumer, OffsetResetStrategy}
import org.scalatest.{Matchers, Outcome, fixture}

import scala.util.{Failure, Success, Try}

class CheckpointInfoProcessorTestSuite extends fixture.FlatSpec with Matchers {
  case class FixtureParam(mockConsumer: MockConsumer[String, String],
                          topicInfoList: TopicInfoList,
                          topicPartitionInfoList: TopicPartitionInfoList)

  def withFixture(test: OneArgTest): Outcome = {
    val topic1 = "topic1"
    val topic2 = "topic2"
    val infoList: List[TopicInfo] = List(
      TopicInfo(topic = topic1),
      TopicInfo(topic = topic2)
    )
    val topicInfoList = TopicInfoList(infoList)

    val firstTopicPartition = TopicPartitionInfo(topic1, partition = 0, offset = 0)
    val secondTopicPartition = TopicPartitionInfo(topic2, partition = 1, offset = 1)
    val thirdTopicPartition = TopicPartitionInfo(topic2, partition = 2, offset = 2)

    val expectedTopicPartitionInfoList: TopicPartitionInfoList = TopicPartitionInfoList(
      List(firstTopicPartition, secondTopicPartition, thirdTopicPartition)
    )

    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)

    val theFixture = FixtureParam(mockConsumer, topicInfoList, expectedTopicPartitionInfoList)

    Try {
      withFixture(test.toNoArgTest(theFixture))
    } match {
      case Success(x) =>
        Try(mockConsumer.close())
        x
      case Failure(e: Throwable) =>
        mockConsumer.close()
        throw e
    }
  }

  "save" should "execute consumer 'commit' method after transform OutputEnvelope entities to TopicPartitionInfoList" in { fixture =>
    var isExecuted = false
    val testConsumer = new Consumer[String, String]("127.0.0.1:9000", "groupId") {
      override protected val consumer: MockConsumer[String, String] = fixture.mockConsumer

      override def commit(topicPartitionInfoList: TopicPartitionInfoList): Unit = {
        isExecuted = true
        assert(fixture.topicPartitionInfoList == topicPartitionInfoList)
      }
    }

    val infoProcessor = new CheckpointInfoProcessor[String, String, String](fixture.topicInfoList, testConsumer)

    val outputEnvelopes = fixture.topicPartitionInfoList.entities.map { entity =>
      OutputEnvelope(entity.topic, entity.partition, entity.offset, "data")
    }

    assert(infoProcessor.save(outputEnvelopes).isInstanceOf[Unit] && isExecuted)
  }

  "load" should "execute consumer 'assign' method" in { fixture =>
    var isExecuted = false
    val testConsumer = new Consumer[String, String]("127.0.0.1:9000", "groupId") {
      override protected val consumer: MockConsumer[String, String] = fixture.mockConsumer

      override def assign(topicInfoList: TopicInfoList): Unit = {
        isExecuted = true
        assert(fixture.topicInfoList == topicInfoList)
      }
    }

    val infoProcessor = new CheckpointInfoProcessor[String, String, String](fixture.topicInfoList, testConsumer)

    assert(infoProcessor.load().isInstanceOf[Unit] && isExecuted)
  }
}
