#!/bin/bash -e

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
