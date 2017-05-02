# storm-topology Example

This project is an example to show how we can use Apache Storm to process Apache Kafka Messages and write data to JanusGraph as a node. This project uses Avro serializer to serialize and de-serialize messages for Kafka.

## Prerequisites

Following software and framework are required to run this project
1. Apache Storm 1.0.2 version
2. Docker Toolbox or Docker for mac/windows
3. Docker image of Cassandra 2.1.9
4. Docker image of Elasticsearch 2.4.4
5. JanusGraph version 0.2.0 from https://github.com/JanusGraph/janusgraph/. Please follow the instruction how to install Janusgraph into your local m2 directory here http://coffeetechgaff.com/src/components/blogs/gremlin-console-with-janusgraph-inlinux.html

## Getting Started

1. Please clone the project.
2. Make sure maven and JDK is installed in your computer.
3. Run mvn clean install. It will generate all avro classes for you as well.

## Running the tests

Just run following command to run all tests
Run mvn clean install

## Deployment

1. copy the jar from your local system to the strom nimbus
docker cp asc-etl-zeppelin-listener-4-SNAPSHOT.jar 01e9664c2e9a:/tmp

2. run the jar in storm nimbus
./bin/storm jar -c nimbus.host=192.168.99.100 -c nimbus.thrift.port=49627 tmp/asc-zeppelin-listener-4-SNAPSHOT-everything.jar com.bah.na.asc.zeppelin.storm.topology.ZeppelinTopology zeppelin-topology