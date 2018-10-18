#!/bin/bash

set -e

BUILD_ID=seer-${BUILDKITE_BUILD_ID:-local}

# if [[ "$BUILDKITE_PULL_REQUEST" == "false" ]]; then
#   echo 'No PR exists - failing/skipping the build'
#   exit 1
# fi

IMAGE_NAME=$BUILD_ID-seerpy

# build docker image
docker build -t $IMAGE_NAME .

function cleanUp {

  echo "finished cleanup"
}

trap cleanUp EXIT

# run unit tests
docker run \
  $IMAGE_NAME

# sleep 2

docker ps -a
