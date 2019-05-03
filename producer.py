#!/usr/bin/env python

import pulsar
import os
import threading
import sys
import time

sys.settrace

# Define globals
global uniq
global delay
global runTime
global connections
global messageCount
global topic
global producerUrl
global msgSize
global runForever

if "MESSAGE_SIZE" in os.environ:
    # Take message size in Kb
    msgSize = int(os.environ["MESSAGE_SIZE"]) * 1000
    if msgSize > 10000000:
        print("Message cannot be greater than 10MB")
        raise SystemExit
else:
    # default is 10Kb
    msgSize = 10000

# Broker URL for the producer to use
if "PRODUCER_URL" in os.environ:
    producerUrl = os.environ["PRODUCER_URL"]
else:
    producerUrl = 'pulsar://localhost:6650'

# Topic to publish to
if "TOPIC" in os.environ:
    topic = os.environ["TOPIC"]
else:
    topic = 'test-topic'

# Total number of messages to send
if "MESSAGE_COUNT" in os.environ and os.environ["MESSAGE_COUNT"]:
    messageCount = int(os.environ["MESSAGE_COUNT"])
else:
    messageCount = False

if "CONNECTIONS" in os.environ:
    connections = int(os.environ["CONNECTIONS"])
else:
    connections = 1

#If TIME is set we will run for TIME amount of seconds instead of counting by messageCount
if "RUN_TIME" in os.environ and os.environ["RUN_TIME"]:
    runTime = int(os.environ["RUN_TIME"])
    if runTime == 0:
        runForever = True
    else:
        runForever = False
else:
    runTime = False
    runForever = False

# How long to wait between sending messages
if "DELAY" in os.environ:
    delay = int(os.environ["DELAY"])
else:
    delay = 0

# Whether or not to append a uniq string to the messages
if "UNIQ_MESSAGES" in os.environ and os.environ["UNIQ_MESSAGES"]:
    uniq = True
else:
    uniq = False

if (runTime and messageCount) or (runForever and messageCount) or (not runTime and not messageCount and not runForever):
    print("[Config Error] Exactly one of either RUN_TIME or MESSAGE_COUNT is required")
    raise SystemExit

banner = """
    ##################################################
        Starting test with options:
            delay: {delay}
            connections: {connections}
            messages (per connection): {messageCount}
            topic: {topic}
            Pulsar url: {producerUrl}
            message size: {msgSize}
            uniq messages: {uniq}
            run time = {runtime}
    ##################################################
    """.format(delay=delay,connections=connections,messageCount=str(messageCount),topic=topic,producerUrl=producerUrl,msgSize=str((msgSize/1000)) + "KB",uniq=str(uniq), runtime=str(runTime))

print(banner)

def genMsg():
    return str(os.urandom(msgSize))

def makeProducer():
    client = pulsar.Client(producerUrl,
        use_tls=True,
        tls_allow_insecure_connection=True,
    )

    try:
        print(topic)
        producer = client.create_producer(topic,
        block_if_queue_full=True,
        batching_enabled=True,
        batching_max_publish_delay_ms=10
    )
        return producer
    except Exception as e:
        print(e)
        raise SystemExit

def produceByTime():
    client = pulsar.Client(producerUrl,
        use_tls=True,
        tls_allow_insecure_connection=True,
    )

    producer = client.create_producer(topic,
        block_if_queue_full=True,
        batching_enabled=True,
        batching_max_publish_delay_ms=10
    )

    if runForever:
        print("Running FOREVER!!!!")
        endTime = 0
    else:
        print("Running for " + str(runTime) + "seconds.")
        endTime = runTime + time.time()

    msg = genMsg()

    while (time.time() < endTime) or (runForever):
        if uniq:
            msgToSend = msg + str(time.time())
        else:
            msgToSend = msg
        try:
            producer.send_async(msgToSend.encode('utf-8'), None)
        except Exception as e:
            print("Failed to send message: %s", e)
        if delay:
            time.sleep(delay/1000)

def produceByCount():
    startTime = time.time()
    print("STARTING produceByCount(" + str(messageCount) +") at " + str(startTime))
    msg = genMsg()
    client = pulsar.Client(producerUrl,
        use_tls=True,
        tls_allow_insecure_connection=True,
    )

    producer = client.create_producer(topic,
        block_if_queue_full=True,
        batching_enabled=True,
        batching_max_publish_delay_ms=10
    )
    for i in range(messageCount):
        if uniq:
            msgToSend = msg + str(time.time())
        else:
            msgToSend = msg
        try:
            producer.send_async(msgToSend.encode('utf-8'), None)
        except Exception as e:
            print("Failed to send message: %s", e)
        if delay:
            time.sleep(delay/1000)
    producer.flush()
    producer.close()
    print("DONE produceByCount(" + str(messageCount) +") in " + str(time.time() - startTime) + " seconds." )
    print(time.time())


threads = []
for n in range(connections):
    try:
        if runTime or runForever:
            t = threading.Thread(target=produceByTime)
        else:
            t = threading.Thread(target=produceByCount)
        t.daemon = True
        threads.append(t)
        t.start()
        print("Thread " + str(n) + " started")
    except Exception as e:
        print(e)

for x in threads:
    x.join()
print("All threads finished")
