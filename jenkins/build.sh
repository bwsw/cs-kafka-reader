#!/bin/bash -e

docker run -d --rm -p 2181:2181 -p $KAFKA_PORT:$KAFKA_PORT --env ADVERTISED_HOST=$KAFKA_HOST --env ADVERTISED_PORT=$KAFKA_PORT spotify/kafka

echo "---------------------------------------------"
echo "----------------- Unit tests ----------------"
echo "---------------------------------------------"

sbt clean coverage test coverageReport

echo "---------------------------------------------"
echo "-------------- Integration tests ------------"
echo "---------------------------------------------"

sbt it:test

echo "---------------------------------------------"
echo "-------------- Scalastyle checks ------------"
echo "---------------------------------------------"

sbt scalastyle

sbt test:scalastyle

echo "git branch: $GIT_BRANCH"
if [ -n "$GIT_BRANCH" ]; then
    if [ "$GIT_BRANCH" = "origin/master" ]; then

        echo "---------------------------------------------"
        echo "------- Publish to Maven repository ---------"
        echo "---------------------------------------------"

        sbt publishSigned
        sbt sonatypeRelease
	fi
fi
