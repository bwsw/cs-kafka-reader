#!/bin/bash -e

docker run -d --rm -p 2181:2181 -p $KAFKA_PORT:$KAFKA_PORT --env ADVERTISED_HOST=$KAFKA_HOST --env ADVERTISED_PORT=$KAFKA_PORT spotify/kafka

sbt test

sbt scalastyle

sbt test:scalastyle
