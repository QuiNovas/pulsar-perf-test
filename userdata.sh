#!/bin/bash

# Install docker
yum install docker

# Whether or not to append a timestamp to each message before sending to attempt uniqueness
UNIQ_MESSAGES=true
# Size of messages in Kb. Defaults to 10Kb max is 10000
MESSAGE_SIZE=10
# Pulsar URL including protocol and port
PRODUCER_URL="pulsar+ssl://echo-0.dev.echostreams.io:6651"
# Topic to publish to
TOPIC="perf-test"
# Number of threads to spawn
CONNECTIONS=1
# Either MESSAGE_COUNT or RUN_TIME, but not both. LEAVE EMPTY IF SETTING A VALUE FOR RUN_TIME, EVEN IF RUN_TIME IS 0!!!
MESSAGE_COUNT=
# Time to run in seconds. 0 Runs forever. LEAVE EMPTY IF USING MESSAGE_COUNT
RUN_TIME=0
# Delay in milliseconds between sending messages
DELAY=1
#Image to use
IMAGE="bashism/pulsar-test:latest"

sudo docker pull ${IMAGE}

sudo docker run -ti --rm \
  -e UNIQ_MESSAGES=${UNIQ_MESSAGES} \
  -e MESSAGE_SIZE=${MESSAGE_SIZE} \
  -e PRODUCER_URL=${PRODUCER_URL} \
  -e TOPIC=${TOPIC} \
  -e CONNECTIONS=${CONNECTIONS} \
  -e MESSAGE_COUNT=${MESSAGE_COUNT} \
  -e RUN_TIME=${RUN_TIME} \
  -e DELAY=${DELAY} \
  ${IMAGE}
