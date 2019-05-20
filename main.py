#!/usr/bin/env python

import os
import PulsarTest

config = {}

if "MESSAGE_SIZE" in os.environ:
    config["msgSize"] = int(os.environ["MESSAGE_SIZE"])
if "TOPIC" in os.environ:
    config["topic"] = os.environ["TOPIC"]
if "MESSAGE_COUNT" in os.environ and os.environ["MESSAGE_COUNT"]:
    config["messageCount"] = int(os.environ["MESSAGE_COUNT"])
if "CONNECTIONS" in os.environ:
    config["connections"] = int(os.environ["CONNECTIONS"])
if "RUN_TIME" in os.environ and os.environ["RUN_TIME"]:
    config["runTime"] = int(os.environ["RUN_TIME"])
if "DELAY" in os.environ:
    config["delay"] = int(os.environ["DELAY"])
if "UNIQ_MESSAGES" in os.environ and os.environ["UNIQ_MESSAGES"]:
    config["uniq"] = os.environ["UNIQ_MESSAGES"]
if "ROLE" in os.environ:
    config["role"] = os.environ["ROLE"]
if "URL" in os.environ:
    config["url"] = os.environ["URL"]
if "SUBSCRIPTION" in os.environ:
    config["subscription"] = os.environ["SUBSCRIPTION"]
if "CONSUMER_TYPE" in os.environ:
    config["consumerType"] = os.environ["CONSUMER_TYPE"]
if "VERBOSITY" in os.environ:
    config["verbosity"] = os.environ["VERBOSITY"]
if "TOPIC_FROM_TOPIC" in os.environ and os.environ["TOPIC_FROM_TOPIC"]:
    config["topicFromTopic"] = os.environ["TOPIC_FROM_TOPIC"]
if "AUTH_TOKEN" in os.environ and os.environ["AUTH_TOKEN"]:
    config["authToken"] = os.environ["AUTH_TOKEN"]

p = PulsarTest.PulsarTest(config)

if "ROLE" in os.environ and (os.environ["ROLE"] == "producer" or os.environ["ROLE"] == "consumer"):
        role = os.environ["ROLE"]
else:
    print("You must define a role of either producer or consumer")
    raise SystemExit
if role == "producer":
    p.produce()
else:
    print("###########CONSUME########")
    p.consume()
