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

import java.util
import java.util.Properties

import com.bwsw.kafka.reader.Consumer
import com.bwsw.kafka.reader.entities._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{Node, PartitionInfo, TopicPartition}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ConsumerTestSuite extends FlatSpec with Matchers {
  val pollTimeout: Int = 500
  val enableAutoCommit: Boolean = false
  val autoCommitInterval: Int = 5000

  "assign" should "assign to topics" in {
    val topic1 = "topic1"
    val topic2 = "topic2"
    val infoList: List[TopicInfo] = List(
      TopicInfo(topic = topic1),
      TopicInfo(topic = topic2)
    )
    val topicInfoList = TopicInfoList(infoList)

    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)

    mockConsumer.updatePartitions(topic1, List(new PartitionInfo(topic1, 0, Node.noNode(), Array(Node.noNode()), Array(Node.noNode()))).asJava)
    mockConsumer.updatePartitions(topic2, List(new PartitionInfo(topic2, 0, Node.noNode(), Array(Node.noNode()), Array(Node.noNode()))).asJava)

    val testConsumer = new Consumer[String, String]("127.0.0.1:9000", "groupId", pollTimeout, enableAutoCommit, autoCommitInterval) {
      override protected val consumer: MockConsumer[String, String] = mockConsumer
    }

    testConsumer.assign(topicInfoList)

    val topics = mockConsumer.assignment().asScala.map(_.topic())

    assert(infoList.map(_.topic).toSet == topics)
  }

  "assign" should "throw Exception" in {
    val topic1 = "topic1"
    val topic2 = "topic2"
    val infoList: List[TopicInfo] = List(
      TopicInfo(topic = topic1),
      TopicInfo(topic = topic2)
    )
    val topicInfoList = TopicInfoList(infoList)

    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)

    val testConsumer = new Consumer[String, String]("127.0.0.1:9000", "groupId", pollTimeout, enableAutoCommit, autoCommitInterval) {
      override protected val consumer: MockConsumer[String, String] = mockConsumer
    }

    assertThrows[Exception](testConsumer.assign(topicInfoList))
  }

  "assign" should "assign to all partitions in topics with offset" in {
    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    val testConsumer = new Consumer[String, String]("127.0.0.1:9000", "groupId", pollTimeout, enableAutoCommit, autoCommitInterval) {
      override protected val consumer: MockConsumer[String, String] = mockConsumer
    }

    val firstTopicPartition = TopicPartitionInfo(topic = "topic1", partition = 0, offset = 1)
    val secondTopicPartition = TopicPartitionInfo(topic = "topic1", partition = 1, offset = 2)
    val thirdTopicPartition = TopicPartitionInfo(topic = "topic2", partition = 2, offset = 3)

    val chechpointInfoList: TopicPartitionInfoList = TopicPartitionInfoList(
      List(firstTopicPartition, secondTopicPartition, thirdTopicPartition)
    )

    testConsumer.assign(chechpointInfoList)

    val topicPartitions = mockConsumer.assignment().asScala.toList
    val offsets = topicPartitions.map { x =>
      mockConsumer.position(x)
    }
    assert(offsets.toSet == chechpointInfoList.entities.map(_.offset).toSet)
  }

  "poll" should "return ConsumerRecord" in {
    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    val testConsumer = new Consumer[String, String]("127.0.0.1:9000", "groupId", pollTimeout, enableAutoCommit, autoCommitInterval) {
      override protected val consumer: MockConsumer[String, String] = mockConsumer
    }

    val firstTopicPartition = TopicPartitionInfo(topic = "topic1", partition = 0, offset = 1)

    val chechpointInfoList: TopicPartitionInfoList = TopicPartitionInfoList(
      List(firstTopicPartition)
    )
    val expectedRecord = new ConsumerRecord[String, String]("topic1", 0, 1, "key", "value")

    testConsumer.assign(chechpointInfoList)
    mockConsumer.addRecord(expectedRecord)

    val actualRecord = testConsumer.poll().asScala.toList.head

    assert(actualRecord == expectedRecord)
  }

  "commit" should "commit offset for topic partition" in {
    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    val testConsumer = new Consumer[String, String]("127.0.0.1:9000", "groupId", pollTimeout, enableAutoCommit, autoCommitInterval) {
      override protected val consumer: MockConsumer[String, String] = mockConsumer
    }

    val assignTopicPartition = TopicPartitionInfo(topic = "topic1", partition = 0, offset = 0)
    val commitTopicPartition = TopicPartitionInfo(topic = "topic1", partition = 0, offset = 1)

    val chechpointInfoList: TopicPartitionInfoList = TopicPartitionInfoList(
      List(assignTopicPartition)
    )

    testConsumer.assign(chechpointInfoList)

    testConsumer.commit(
      TopicPartitionInfoList(List(commitTopicPartition))
    )

    val offset = mockConsumer.committed(new TopicPartition(commitTopicPartition.topic, commitTopicPartition.partition)).offset()
    assert(offset == commitTopicPartition.offset)
  }

  "close" should "close the consumer" in {
    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    val testConsumer = new Consumer[String, String]("127.0.0.1:9000", "groupId", pollTimeout, enableAutoCommit, autoCommitInterval) {
      override protected val consumer: MockConsumer[String, String] = mockConsumer
    }

    val firstTopicPartition = TopicPartitionInfo(topic = "topic1", partition = 0, offset = 1)

    val chechpointInfoList: TopicPartitionInfoList = TopicPartitionInfoList(
      List(firstTopicPartition)
    )
    val expectedRecord = new ConsumerRecord[String, String]("topic1", 0, 1, "key", "value")

    testConsumer.assign(chechpointInfoList)

    testConsumer.close()

    assert(mockConsumer.closed())
  }
}
