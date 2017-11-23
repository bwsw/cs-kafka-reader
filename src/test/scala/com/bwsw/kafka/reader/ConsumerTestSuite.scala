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
import org.scalatest._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class ConsumerTestSuite extends fixture.FlatSpec with PrivateMethodTester {

  case class FixtureParam(mockConsumer: MockConsumer[String, String],
                          testConsumer: Consumer[String, String],
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

    val topicPartitionInfoList: TopicPartitionInfoList = TopicPartitionInfoList(
      List(firstTopicPartition, secondTopicPartition, thirdTopicPartition)
    )

    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)

    val testConsumer = new Consumer[String, String](Consumer.Settings("127.0.0.1:9000", "groupId")) {
      override protected val consumer: MockConsumer[String, String] = mockConsumer

      override def covertToTopicPartition(topicInfoList: TopicInfoList): List[TopicPartition] = {
        super.covertToTopicPartition(topicInfoList)
      }
    }

    val theFixture = FixtureParam(mockConsumer, testConsumer, topicInfoList, topicPartitionInfoList)

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

  "assign" should "assign a list of topic/partition to this consumer" in { fixture =>

    createPartitions(fixture.topicInfoList, fixture.mockConsumer)

    fixture.testConsumer.assign(fixture.topicInfoList)

    val topics = fixture.mockConsumer.assignment().asScala.map(_.topic())

    assert(fixture.topicInfoList.entities.map(_.topic).toSet == topics)

    fixture.mockConsumer.close()
  }

  "assign" should "throw NoSuchElementException if specified topics does not exist in Kafka" in { fixture =>
    assertThrows[NoSuchElementException](fixture.testConsumer.assign(fixture.topicInfoList))

    fixture.mockConsumer.close()
  }

  "assignWithOffsets" should "assign a list of topic/partition to this consumer" in { fixture =>
    fixture.testConsumer.assignWithOffsets(fixture.topicPartitionInfoList)

    val topicPartitions = fixture.mockConsumer.assignment().asScala.toList
    val offsets = topicPartitions.map { x =>
      fixture.mockConsumer.position(x)
    }

    assert(offsets.toSet == fixture.topicPartitionInfoList.entities.map(_.offset).toSet)

    fixture.mockConsumer.close()
  }

  "poll" should "retrieve ConsumerRecord from specified Kafka topic partition" in { fixture =>
    val topicPartition = fixture.topicPartitionInfoList.entities.head
    val expectedRecords = fixture.topicPartitionInfoList.entities.map { x =>
      new ConsumerRecord[String, String](
        x.topic,
        x.partition,
        x.offset,
        "key",
        "value"
      )
    }.toSet

    fixture.testConsumer.assignWithOffsets(fixture.topicPartitionInfoList)
    expectedRecords.foreach { record =>
      fixture.mockConsumer.addRecord(record)
    }

    val actualRecords = fixture.testConsumer.poll().asScala.toSet

    assert(actualRecords == expectedRecords)

    fixture.mockConsumer.close()
  }

  "commit" should "commit offset for topic partition" in { fixture =>
    fixture.testConsumer.assignWithOffsets(fixture.topicPartitionInfoList)

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

  "covertToTopicPartition" should "create list of TopicPartition using partitions information " +
    "which retrieves from Kafka based on TopicInfoList" in { fixture =>
    val expectedTopicPartitions = createPartitions(fixture.topicInfoList, fixture.mockConsumer)
    val covertToTopicPartition = PrivateMethod[List[TopicPartition]]('covertToTopicPartition)

    def convert(topicList: TopicInfoList): List[TopicPartition] = fixture.testConsumer invokePrivate covertToTopicPartition(
      topicList
    )

    assert(expectedTopicPartitions == convert(fixture.topicInfoList))
  }

  private def createPartitions(topicList: TopicInfoList, mockConsumer: MockConsumer[String, String]): List[TopicPartition] = {
    val partition = 0
    topicList.entities.map { topicInfo =>
      mockConsumer.updatePartitions(topicInfo.topic, List(new PartitionInfo(topicInfo.topic, partition, Node.noNode(), Array(Node.noNode()), Array(Node.noNode()))).asJava)
      new TopicPartition(topicInfo.topic, partition)
    }
  }
}
