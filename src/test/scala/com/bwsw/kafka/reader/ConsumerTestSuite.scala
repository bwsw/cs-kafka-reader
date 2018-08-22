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

class ConsumerTestSuite
  extends fixture.FlatSpec
    with PrivateMethodTester
    with Matchers {

  case class FixtureParam(mockConsumer: MockConsumer[String, String],
                          testConsumer: Consumer[String, String],
                          topicInfoList: TopicInfoList,
                          topicPartitionInfoList: TopicPartitionInfoList)

  private val topic1 = "topic1"
  private val topic2 = "topic2"
  private val infoList: List[TopicInfo] = List(
    TopicInfo(topic = topic1),
    TopicInfo(topic = topic2)
  )
  private val topicInfoList = TopicInfoList(infoList)

  private val partition1 = 1
  private val partition2 = 2
  private val partition3 = 3

  private val offset1 = 12L
  private val offset2 = 34L
  private val offset3 = 56L

  private val firstTopicPartition = TopicPartitionInfo(topic1, partition = partition1, offset = offset1)
  private val secondTopicPartition = TopicPartitionInfo(topic2, partition = partition2, offset = offset2)
  private val thirdTopicPartition = TopicPartitionInfo(topic2, partition = partition3, offset = offset3)

  private val topicPartitionInfoList: TopicPartitionInfoList = TopicPartitionInfoList(
    List(firstTopicPartition, secondTopicPartition, thirdTopicPartition)
  )


  def withFixture(test: OneArgTest): Outcome = {
    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)

    val testConsumer: Consumer[String, String] = new Consumer[String, String](Consumer.Settings("127.0.0.1:9000", "groupId")) {
      override protected val consumer: MockConsumer[String, String] = mockConsumer

      override def convertToTopicPartition(topicInfoList: TopicInfoList): List[TopicPartition] = {
        super.convertToTopicPartition(topicInfoList)
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
    val offset: Long = 0
    createPartitions(fixture.topicInfoList, fixture.mockConsumer)

    fixture.mockConsumer.updateBeginningOffsets(fixture.topicInfoList.entities.map { x =>
      new TopicPartition(x.topic, 0) -> new java.lang.Long(offset)
    }.toMap.asJava)

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

  it should "assign a list of topic/partition to specified offsets properly" in { fixture =>
    import fixture._
    fixture.mockConsumer.updateBeginningOffsets(
      Map(new TopicPartition(topic2, partition2) -> Long.box(offset2)).asJava
    )
    fixture.mockConsumer.updateEndOffsets(
      Map(new TopicPartition(topic2, partition3) -> Long.box(offset3)).asJava
    )

    val offsets = Map(
      TopicWithPartition(topic1, partition1) -> Offset.Concrete(offset1),
      TopicWithPartition(topic2, partition2) -> Offset.Beginning,
      TopicWithPartition(topic2, partition3) -> Offset.End
    )

    val expectedOffsets = Map(
      TopicWithPartition(topic1, partition1) -> offset1,
      TopicWithPartition(topic2, partition2) -> offset2,
      TopicWithPartition(topic2, partition3) -> offset3
    )
    testConsumer.assignWithOffsets(offsets)

    val topicPartitions = fixture.mockConsumer.assignment().asScala.toList
    val actualOffsets = topicPartitions.map { x =>
      TopicWithPartition(x.topic(), x.partition()) -> fixture.mockConsumer.position(x)
    }.toMap

    actualOffsets shouldEqual expectedOffsets
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

  "convertToTopicPartition" should "create list of TopicPartition using partitions information " +
    "which retrieves from Kafka based on TopicInfoList" in { fixture =>
    val expectedTopicPartitions = createPartitions(fixture.topicInfoList, fixture.mockConsumer)
    val convertToTopicPartition = PrivateMethod[List[TopicPartition]]('convertToTopicPartition)

    def convert(topicList: TopicInfoList): List[TopicPartition] = fixture.testConsumer invokePrivate convertToTopicPartition(
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
