#!/bin/bash


# Install docker
which docker || yum install docker

# Whether or not to append a timestamp to each message before sending to attempt uniqueness
UNIQ_MESSAGES=true
# Size of messages in Kb. Defaults to 10Kb max is 10000
MESSAGE_SIZE=10
# Pulsar URL including protocol and port
URL="pulsar+ssl://echo-0.dev.echostreams.io:6651"
# Topic to publish to
TOPIC="perf-test"
# Number of threads to spawn
CONNECTIONS=1
# Either MESSAGE_COUNT or RUN_TIME, but not both. LEAVE EMPTY IF SETTING A VALUE FOR RUN_TIME, EVEN IF RUN_TIME IS 0!!!
#MESSAGE_COUNT=
# Time to run in seconds. 0 Runs forever. LEAVE EMPTY IF USING MESSAGE_COUNT
RUN_TIME=0
# Delay in milliseconds between sending messages
DELAY=1
# producer or consumer
ROLE=consumer
# Consuimer type. Setting anything other than "shared" will limit us to a single consumer at one time
CONSUMER_TYPE=shared
# Only for consumers. 0 is silent, 1 prints message ID's, 2 prints full message with ID
VERBOSITY=0
#Image to use
IMAGE="quinovas/pulsar-test:latest"

sudo service docker start

sudo docker pull ${IMAGE}

sudo docker run -ti --rm \
  -e UNIQ_MESSAGES=${UNIQ_MESSAGES} \
  -e MESSAGE_SIZE=${MESSAGE_SIZE} \
  -e URL=${URL} \
  -e TOPIC=${TOPIC} \
  -e CONNECTIONS=${CONNECTIONS} \
  -e MESSAGE_COUNT=${MESSAGE_COUNT} \
  -e RUN_TIME=${RUN_TIME} \
  -e DELAY=${DELAY} \
  -e ROLE=${ROLE} \
  -e VERBOSITY=${VERBOSITY} \
  -e CONSUMER_TYPE=${CONSUMER_TYPE} \
  ${IMAGE}
