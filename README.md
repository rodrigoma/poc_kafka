Poc Kafka

To generate avro classes

    $ mvn compile

To build the project

    $ mvn clean package

Quickstart
----------

Before running the examples, make sure that Zookeeper, Kafka and Schema Registry are
running. In what follows, we assume that Zookeeper, Kafka and Schema Registry are
started with the default settings.

    # Start Zookeeper
    $ bin/zookeeper-server-start etc/kafka/zookeeper.properties

    # Start Kafka
    $ bin/kafka-server-start etc/kafka/server.properties

    # Start Schema Registry
    $ bin/schema-registry-start etc/schema-registry/schema-registry.properties

Example
------------------

Run the producer

    $ mvn exec:java -Dexec.mainClass="br.com.rodrigoma.poc_kafka.ProducerSum" \
      -pl producer

Run the consumer

    $ mvn exec:java -Dexec.mainClass="br.com.rodrigoma.poc_kafka.ConsumerSum" \
      -pl consumer

Example Using Avro
------------------

Run the producer

    $ mvn exec:java -Dexec.mainClass="br.com.rodrigoma.poc_kafka.ProducerAvroClicks" \
      -Dexec.args="http://192.168.0.136:8081" -pl producer

Run the consumer

    $ mvn exec:java -Dexec.mainClass="br.com.rodrigoma.poc_kafka.ConsumerAvroClicks" \
      -Dexec.args="http://192.168.0.136:8081" -pl consumer
