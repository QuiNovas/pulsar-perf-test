#PulsarTest.py

import pulsar
import os
import threading
import sys
import time
import uuid

sys.settrace

class PulsarTest(object):
    uniq = bool()
    delay = int()
    runTime = int()
    connections = int()
    messageCount = int()
    topic = str()
    url = str()
    messageSize = float()
    runForever = bool()
    subscription = str()
    consumerType = str()
    verbosity = int()
    topicFromTopic = bool()
    authToken = str()
    namespace = str()
    tenant = str()
    batchSize = int()
    sendAsync = bool()

    def __init__(self, config):

        if "verbosity" in config:
            self.verbosity = int(config["verbosity"])
        else:
            self.verbosity = 0

        if "messageSize" in config:
            # Take message size in Kb
            self.messagSize = int(float(config["messageSize"]) * 1000)
            if self.messageSize > 10000000:
                print("Message cannot be greater than 10MB")
                raise SystemExit
            if self.messageSize < 1:
                print("Message cannot be greater than 1 byte")
                raise SystemExit
        else:
            # default is 10Kb
            self.messageSize = 10000

        # Set consumerType. Default is exclusive
        if "consumerType" in config:
            if config["consumerType"] == "shared":
                self.consumerType = pulsar.ConsumerType.Shared
            elif config["consumerType"] == "exclusive":
                self.consumerType = pulsar.ConsumerType.Exclusive
            elif config["consumerType"] == "failover":
                self.consumerType = pulsar.ConsumerType.Failover
            else:
                print("Invalid consumer type. Falling back to Exclusive")
                self.consumerType = pulsar.ConsumerType.Exclusive
        else:
            print("Consumer type not define. Falling back to Exclusive")
            self.consumerType = pulsar.ConsumerType.Exclusive

        # Broker URL for the producer to use
        if "url" in config:
            self.url = config["url"]
        else:
            self.url = 'pulsar://localhost:6650'

        # Topic to publish to
        if "topic" in config:
            self.topic = config["tenant"] + "/" + config["namespace"] + "/" + config["topic"]
        else:
            self.topic = config["tenant"] + "/" + config["namespace"] + "/" + 'test-topic'

        # Total number of messages to send
        if "messageCount" in config and config["messageCount"]:
            self.messageCount = int(config["messageCount"])
        else:
            self.messageCount = False

        if "connections" in config:
            self.connections = int(config["connections"])
        else:
            self.connections = 1

        #If TIME is set we will run for TIME amount of seconds instead of counting by messageCount
        if "runTime" in config:
            self.runTime = int(config["runTime"])
            if self.runTime == 0:
                self.runForever = True
            else:
                self.runForever = False
        else:
            self.runTime = False
            self.runForever = False

        # How long to wait between sending messages
        if "delay" in config:
            self.delay = int(config["delay"])
        else:
            self.delay = 0

        # Whether or not to append a uniq string to the messages
        if "uniq" in config and config["uniq"]:
            self.uniq = True
        else:
            self.uniq = False

        if "topicFromTopic" in config:
            self.topicFromTopic = config["tenant"] + "/" + config["namespace"] + "/" + str(uuid.uuid4())
        else:
            self.topicFromTopic = False

        # If we didn't define a subscription name then generate a UUID
        if "subscription" in config and config["subscription"]:
            self.subscription = config["subscription"]
        else:
            self.subcription = False

        if "authToken" in config and config["authToken"]:
            self.authToken = pulsar.AuthenticationToken(config["authToken"])
        else:
            self.authToken = None

        if "batchSize" in config and int(config["batchSize"]) > 0:
            self.batchSize = int(config["batchSize"])
        else:
            self.batchSize = False

        if "sendAsync" in config and config["sendAsync"]:
            self.sendAsync = True
        else:
            self.sendAsync = False

        if (self.runTime and self.messageCount) or (self.runForever and self.messageCount) or (not self.runTime and not self.messageCount and not self.runForever):
            print("[Config Error] Exactly one of either RUN_TIME or MESSAGE_COUNT is required")
            raise SystemExit

    def banner(self):
        message = """
            ##################################################
                Starting test with options:
                    delay: {delay}
                    connections: {connections}
                    messages (per connection): {messageCount}
                    tenant: {tenant}
                    namespace: {namespace}
                    topic: {topic}
                    Pulsar url: {url}
                    message size: {messageSize}
                    uniq messages: {uniq}
                    run time: {runtime}
                    subscription name: {subscription}
                    consumer type: {consumerType}
                    verbosity: {verbosity}
                    generate random topic: {topicFromTopic}
                    authToken : {token}
            ##################################################
            """.format(
                    delay=self.delay,
                    connections=self.connections,
                    messageCount=str(self.messageCount),
                    topic=self.topic,
                    url=self.url,
                    messageSize=str(self.messageSize/1000) + "KB",
                    uniq=str(self.uniq),
                    runtime=str(self.runTime),
                    subscription=str(self.subscription),
                    consumerType=self.consumerType,
                    verbosity=self.verbosity,
                    topicFromTopic=str(bool(self.topicFromTopic)),
                    token=str(self.authToken),
                    namespace=self.namespace,
                    tenant=self.tenant
                )
        print(message)


    def genMsg(self):
        return str(os.urandom(self.messageSize))

    def sendAsyncCallback(self, res, msg):
        pass

    def produceByTime(self):
        try:
            client = pulsar.Client(self.url,
                                use_tls=True,
                                tls_allow_insecure_connection=True,
                                authentication=self.authToken
                            )

            producer = client.create_producer(self.topic)
        except Exception as e:
            print(e)
            raise SystemExit

        # Publish our topic name if we are doing topicFromTopic
        if self.topicFromTopic:
            print("####################### GEN TOPIC #########################")
            try:
                s = producer.send(self.topicFromTopic.encode('utf-8'))
                print("Created topic " + self.topicFromTopic + " published into " + self.topic)
                self.topic = self.topicFromTopic
                producer = client.create_producer(self.topic)
            except Exception as e:
                print("Failed to send topic name: %s", e)
                raise SystemExit

        if self.runForever:
            print("Running FOREVER!!!!")
            endTime = 0
        else:
            print("Running for " + str(self.runTime) + "seconds.")
            endTime = self.runTime + time.time()

        msg = self.genMsg()

        while (time.time() < endTime) or (self.runForever):
            if self.uniq:
                msgToSend = msg + str(time.time())
            else:
                msgToSend = msg
            try:
                if self.sendAsync:
                    producer.send_async(msg, self.sendAsyncCallback)
                else:
                    producer.send(msgToSend.encode('utf-8'))
            except Exception as e:
                print("Failed to send message: %s", e)
            if self.delay:
                time.sleep(self.delay/1000)


    def produceByCount(self):
        startTime = time.time()

        print("STARTING produceByCount(" + str(self.messageCount) +") at " + str(startTime))

        msg = self.genMsg()

        client = pulsar.Client(self.url,
                            use_tls=True,
                            tls_allow_insecure_connection=True,
                            authentication=self.authToken
                        )

        producer = client.create_producer(self.topic)

        # Publish our topic name if we are doing topicFromTopic
        if self.topicFromTopic:
            try:
                s = producer.send(self.topicFromTopic.encode('utf-8'))
                print("Created topic " + self.topicFromTopic)
                self.topic = self.topicFromTopic
                producer = client.create_producer(self.topic)
            except Exception as e:
                print("Failed to send topic name: %s", e)
                raise SystemExit

        for i in range(self.messageCount):
            if self.uniq:
                msgToSend = msg + str(time.time())
            else:
                msgToSend = msg
            try:
                s = producer.send(msgToSend.encode('utf-8'))
                print(s)
            except Exception as e:
                print("Failed to send message: %s", e)
            if self.delay:
                time.sleep(self.delay/1000)
        producer.flush()
        producer.close()
        print("DONE produceByCount(" + str(self.messageCount) +") in " + str(time.time() - startTime) + " seconds." )
        print(time.time())


    def consumeByTime(self):
        self.banner()
        startTime = time.time()
        print("Starting consumeByTime(" + str(self.runTime) +") at " + str(startTime))



        # Grab a topic name if we are doing topicFromTopic
        if self.topicFromTopic:
            client = pulsar.Client(self.url,
                                use_tls=True,
                                tls_allow_insecure_connection=True,
                                authentication=self.authToken
                            )
            try:
                consumer = client.subscribe(self.topic,
                                    "getTopicSubscriber",
                                    consumer_type=pulsar.ConsumerType.Shared
                                )
                msg = consumer.receive()
                consumer.acknowledge(msg)
                client.close()
                print("Received topic " + msg.data().decode('utf-8'))
                self.topic = str(msg.data().decode('utf-8'))
            except Exception as e:
                print("Failed to get topic name: %s", e)
                raise SystemExit

        client = pulsar.Client(self.url,
                            use_tls=True,
                            tls_allow_insecure_connection=True,
                            authentication=self.authToken,
                        )

        if not self.subscription:
            print("No subscription defined. Generating one with UUID")
            consumer = client.subscribe(self.topic,
                                str(uuid.uuid4()),
                                consumer_type=self.consumerType
                            )
        else:
            print("Using subscription " + self.subscription)

            consumer = client.subscribe(self.topic,
                                self.subscription,
                                consumer_type=self.consumerType
                            )

        if self.runForever:
            print("Running FOREVER!!!!")
            endTime = 0
        else:
            print("Running for " + str(self.runTime) + "seconds. Starting at " + str(startTime))
            endTime = self.runTime + startTime

        n = 0
        while (time.time() < endTime) or (self.runForever):
            try:
                msg = consumer.receive()
                if self.batchSize == False:
                    consumer.acknowledge(msg)
                else:
                    if n > self.batchSize:
                        consumer.acknowledge_cumulative(msg)
                if self.verbosity > 1:
                    print("Received message " + str(msg.message_id()) + ": " + str(msg.data()))
                elif self.verbosity == 1:
                    print(msg.message_id())
            except Exception as e:
                print(e)
            n = n + 1
            if self.delay:
                time.sleep(self.delay/1000)

        client.close()
        print("Finished consumeByCount(" + str(self.messageCount) + ") at " + str(time.time()))

    def consumeByCount(self):
        startTime = time.time()

        client = pulsar.Client(self.url,
                            use_tls=True,
                            tls_allow_insecure_connection=True,
                            authentication=self.authToken
                        )

        # Grab a topic name if we are doing topicFromTopic
        if self.topicFromTopic:
            try:
                consumer = client.subscribe(self.topic,
                                    str(uuid.uuid4()),
                                    consumer_type=pulsar.ConsumerType.Exclusive
                                )
                msg = consumer.receive()
                consumer.acknowledge(msg)
                self.topic = str(msg.data())
                if self.verbosity > 1:
                    print("Received topic " + str(msg.message_id()) + ": " + str(msg.data()))
                elif self.verbosity == 1:
                    print(msg.message_id())
            except Exception as e:
                print("Failed to get topic name: %s", e)
                raise SystemExit

        if not self.subscription:
            consumer = client.subscribe(self.topic,
                                str(uuid.uuid4()),
                                consumer_type=self.consumerType
                            )
        else:
            consumer = client.subscribe(self.topic,
                                self.subscription,
                                consumer_type=self.consumerType
                            )

        for i in range(self.messageCount):
            try:
                msg = consumer.receive()
                consumer.acknowledge(msg)
                if self.verbosity > 1:
                    print("Received message " + str(msg.message_id()) + ": " + str(msg.data()))
                elif self.verbosity == 1:
                    print(msg.message_id())
            except Exception as e:
                print(e)
            if self.delay:
                time.sleep(delay/1000)

        client.close()
        print("DONE consumeByCount(" + str(self.messageCount) +") in " + str(time.time() - startTime) + " seconds at " + str(time.time()) )


    def produce(self):
        threads = []
        for n in range(self.connections):
            try:
                if self.runTime or self.runForever:
                    t = threading.Thread(target=self.produceByTime)
                else:
                    t = threading.Thread(target=self.produceByCount)
                t.daemon = True
                threads.append(t)
                t.start()
                print("Thread " + str(n) + " started")
            except Exception as e:
                print(e)

        for x in threads:
            x.join()
        print("All threads finished")


    def consume(self):
        threads = []
        for n in range(self.connections):
            try:
                if self.runTime or self.runForever:
                    print("Consuming by time")
                    t = threading.Thread(target=self.consumeByTime)
                else:
                    t = threading.Thread(target=self.consumeByCount)
                t.daemon = True
                threads.append(t)
                t.start()
                print("Thread " + str(n) + " started")
            except Exception as e:
                print(e)

        for x in threads:
            x.join()
        print("All threads finished")
