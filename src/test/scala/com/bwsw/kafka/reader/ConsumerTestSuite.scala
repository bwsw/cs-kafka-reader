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
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{Node, PartitionInfo, TopicPartition}
import org.scalatest.{FlatSpec, Matchers, Outcome, fixture}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class ConsumerTestSuite extends fixture.FlatSpec {

  case class FixtureParam(mockConsumer: MockConsumer[String, String],
                          testConsumer: Consumer[String, String],
                          topicPartitionInfoList: TopicPartitionInfoList)

  def withFixture(test: OneArgTest): Outcome = {
    val firstTopicPartition = TopicPartitionInfo(topic = "topic1", partition = 0, offset = 0)
    val secondTopicPartition = TopicPartitionInfo(topic = "topic2", partition = 1, offset = 1)
    val thirdTopicPartition = TopicPartitionInfo(topic = "topic2", partition = 2, offset = 2)

    val topicPartitionInfoList: TopicPartitionInfoList = TopicPartitionInfoList(
      List(firstTopicPartition, secondTopicPartition, thirdTopicPartition)
    )

    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)

    val testConsumer = new Consumer[String, String]("127.0.0.1:9000", "groupId") {
      override protected val consumer: MockConsumer[String, String] = mockConsumer
    }

    val theFixture = FixtureParam(mockConsumer, testConsumer, topicPartitionInfoList)

    Try {
      withFixture(test.toNoArgTest(theFixture))
    } match {
      case Success(x) => x
      case Failure(e: Throwable) =>
        mockConsumer.close()
        throw e
    }
  }

  "assign" should "assign to topics" in { fixture =>
    val topic1 = "topic1"
    val topic2 = "topic2"
    val infoList: List[TopicInfo] = List(
      TopicInfo(topic = topic1),
      TopicInfo(topic = topic2)
    )
    val topicInfoList = TopicInfoList(infoList)

    fixture.mockConsumer.updatePartitions(topic1, List(new PartitionInfo(topic1, 0, Node.noNode(), Array(Node.noNode()), Array(Node.noNode()))).asJava)
    fixture.mockConsumer.updatePartitions(topic2, List(new PartitionInfo(topic2, 0, Node.noNode(), Array(Node.noNode()), Array(Node.noNode()))).asJava)

    fixture.testConsumer.assign(topicInfoList)

    val topics = fixture.mockConsumer.assignment().asScala.map(_.topic())

    assert(infoList.map(_.topic).toSet == topics)

    fixture.mockConsumer.close()
  }

  "assign" should "throw NoSuchElementException if specified topics are not exists in Kafka" in { fixture =>
    val topic1 = "topic1"
    val infoList: List[TopicInfo] = List(TopicInfo(topic = topic1))
    val topicInfoList = TopicInfoList(infoList)

    assertThrows[NoSuchElementException](fixture.testConsumer.assign(topicInfoList))
  }

  "assign" should "assign to all partitions in topics with offset" in { fixture =>
    fixture.testConsumer.assign(fixture.topicPartitionInfoList)

    val topicPartitions = fixture.mockConsumer.assignment().asScala.toList
    val offsets = topicPartitions.map { x =>
      fixture.mockConsumer.position(x)
    }

    assert(offsets.toSet == fixture.topicPartitionInfoList.entities.map(_.offset).toSet)

    fixture.mockConsumer.close()
  }

  "poll" should "retrieve ConsumerRecord from specified Kafka topic partition" in { fixture =>
    val topicPartition = fixture.topicPartitionInfoList.entities.head
    val expectedRecord = new ConsumerRecord[String, String](
      topicPartition.topic,
      topicPartition.partition,
      topicPartition.offset,
      "key",
      "value"
    )

    fixture.testConsumer.assign(fixture.topicPartitionInfoList)
    fixture.mockConsumer.addRecord(expectedRecord)

    val actualRecord = fixture.testConsumer.poll().asScala.toList.head

    assert(actualRecord == expectedRecord)

    fixture.mockConsumer.close()
  }

  "commit" should "commit offset for topic partition" in { fixture =>
    fixture.testConsumer.assign(fixture.topicPartitionInfoList)

    val commitTopicPartitions = fixture.topicPartitionInfoList.entities.map(x => x.copy(offset = x.offset + 1))
      fixture.testConsumer.commit(TopicPartitionInfoList(commitTopicPartitions))

    val offsets = commitTopicPartitions.map { topicPartitionInfo =>
      fixture.mockConsumer.committed(
        new TopicPartition(topicPartitionInfo.topic, topicPartitionInfo.partition)
      ).offset()
    }

    assert(offsets == commitTopicPartitions.map(_.offset))

    fixture.mockConsumer.close()
  }

  "close" should "close the consumer" in { fixture =>
    fixture.testConsumer.close()

    assert(fixture.mockConsumer.closed())
  }
}
