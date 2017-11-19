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

import java.util.Properties

import com.bwsw.kafka.reader.Consumer
import com.bwsw.kafka.reader.entities.{CheckpointInfo, CheckpointInfoList, PartitionInfo, TopicInfo}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ConsumerTestSuite extends FlatSpec with Matchers {
  val pollTimeout: Int = 500
  val enableAutoCommit: Boolean = false
  val autoCommitInterval: Int = 5000

  "assign" should "assign to topics" in {
    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    val testConsumer = new Consumer[String, String]("127.0.0.1:9000", "groupId", pollTimeout, enableAutoCommit, autoCommitInterval) {
      override protected val consumer: MockConsumer[String, String] = mockConsumer
    }
    val infoList: List[CheckpointInfo] = List(
      CheckpointInfo(TopicInfo(topic = "topic1"), List.empty),
      CheckpointInfo(TopicInfo(topic = "topic2"), List.empty)
    )
    val checkpointInfoList = CheckpointInfoList(infoList)

    testConsumer.subscribe(checkpointInfoList)

    val topics = mockConsumer.subscription()

    assert(infoList.map(_.topicInfo.topic).toSet.asJava == topics)
  }

  "assign" should "assign to all partitions in topics with offset" in {
    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    val testConsumer = new Consumer[String, String]("127.0.0.1:9000", "groupId", pollTimeout, enableAutoCommit, autoCommitInterval) {
      override protected val consumer: MockConsumer[String, String] = mockConsumer
    }

    val firstPartition = PartitionInfo(partition = 0, offset = 1)
    val secondPartition = PartitionInfo(partition = 1, offset = 2)
    val thirdPartition = PartitionInfo(partition = 2, offset = 3)

    val chechpointInfoList: CheckpointInfoList = CheckpointInfoList(
      List(
        CheckpointInfo(TopicInfo("topic1"), List(firstPartition, secondPartition)),
        CheckpointInfo(TopicInfo("topic2"), List(thirdPartition))
      )
    )

    testConsumer.assign(chechpointInfoList)

    val topicPartitions = mockConsumer.assignment().asScala.toList
    val offsets = topicPartitions.map { x =>
      mockConsumer.position(x)
    }
    assert(offsets.toSet == chechpointInfoList.infoList.flatMap(_.partitionInfoList.map(_.offset)).toSet)
  }

}
