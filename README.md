# storm-topology Example

This project is an example to show how we can use Apache Storm to process Apache Kafka Messages and write data to JanusGraph as a vertex. This project uses Avro serializer to serialize and de-serialize messages for Kafka.

## Prerequisites

Following software and framework are required to run this project
1. Apache Storm 1.0.2 version
2. Docker Toolbox or Docker for mac/windows
3. Docker image of Cassandra 2.1.9
4. Docker image of Elasticsearch 2.4.4
5. JanusGraph version 0.2.0 from https://github.com/JanusGraph/janusgraph/
6. Kafka Image of 10.0.1 or install Kafka with Zookeeper locally

Please follow the instruction how to install JanusGraph into your local .m2 directory here http://coffeetechgaff.com/src/components/blogs/gremlin-console-with-janusgraph-inlinux.html
Please follow the instruction from http://coffeetechgaff.com/src/components/blogs/creating-janusgraph-using-gremlin.html to run Cassandra and Elasticsearch.

## Getting Started

1. Please clone the project.
2. Make sure maven and JDK are installed on your machine.
3. Run mvn clean install. It will generate all avro classes for you as well. This required.

## Running the tests

Just run following command to run all tests
Run mvn clean install

## Deployment

Please make sure to run following images to run this topology:
1. Redis
2. Storm
3. Kafka
4. Cassandra
5. Elasticsearch

Then, please follow following steps to deploy the topology to the storm cluster.

1. copy the jar from your local system to the strom nimbus(image name)
docker cp storm-topology.jar 01e9664c2e9a:/tmp

2. run the jar in storm nimbus
./bin/storm jar -c nimbus.host=192.168.99.100 -c nimbus.thrift.port=49627 tmp/storm-topology.jar com.coffeetechgaff.storm.topology.ExampleTopology example-topology

This topology will consumes the message on the topic that use sets in Redis. Kafka Producer needs to be created which serialized the message using Avro serializer and publish the message on the same topic that is being used by this topology.
This complete working module for topology. Since, there are lots of moving parts on this topology, it might not be feasible to run locally unless the laptop has lots of memory because we need to run following docker images to make it work:
1. Redis
2. Storm
3. Kafka
4. Cassandra
5. Elasticsearch

This project can be used to see how the code for Storm topology looks like with spouts and bolts and how they work together with other external databases.

