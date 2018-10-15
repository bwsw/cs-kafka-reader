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

lazy val kafkaReader = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    name := "kafka-reader",
    organization := "com.bwsw",
    version := "0.10.2",

    scalaVersion := "2.12.6",
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.0",
      "org.slf4j" % "slf4j-api" % "1.7.25",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      ("org.apache.kafka" % "kafka_2.12" % "0.10.2.1").exclude("org.slf4j", "slf4j-api"),
      "org.scalatest" %% "scalatest" % "3.0.1" % "it,test",
      "net.manub" %% "scalatest-embedded-kafka" % "1.1.1" % "it,test",
      "org.mockito" % "mockito-core" % "2.23.0" % "it,test",
    ),
    pomIncludeRepository := { _ => false },
    licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    homepage := Some(url("https://github.com/bwsw/kafka-reader")),
    publishMavenStyle := true,
    publishArtifact in Test := false,
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value) {
        Some("snapshots" at nexus + "content/repositories/snapshots")
      } else {
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
      }
    },
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/bwsw/kafka-reader"),
        "scm:git@github.com:bwsw/kafka-reader.git"
      )
    ),
    developers := List(
      Developer(
        id = "bitworks",
        name = "Bitworks Software, Ltd.",
        email = "bitworks@bw-sw.com",
        url = url("http://bitworks.software/")
      )
    ),
    inConfig(IntegrationTest)(Defaults.itSettings),
    testOptions += Tests.Argument(
      TestFrameworks.ScalaTest,
      "-oFD", // to show full stack traces and durations
      "-W", "120", "60" // to notify when some test is running longer than a specified amount of time
    ),
  )
