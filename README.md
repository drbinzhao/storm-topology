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

1. copy the jar from your local system to the strom nimbus(image name)
docker cp storm-topology.jar 01e9664c2e9a:/tmp

2. run the jar in storm nimbus
./bin/storm jar -c nimbus.host=192.168.99.100 -c nimbus.thrift.port=49627 tmp/storm-topology.jar com.coffeetechgaff.storm.topology.ExampleTopology example-topology

