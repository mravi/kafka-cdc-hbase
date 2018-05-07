# Kafka Connect for Hbase

A Sink connector to write to HBase.  
I have the source connector implementation available at https://github.com/mravi/hbase-connect-kafka

## Pre-requisites
* Confluent 2.0
* HBase 1.0.0
* JDK 1.8

## Assumptions
* The HBase table already exists.
* Each Kafka topic is mapped to a HBase table.


## Properties

Below are the properties that need to be passed in the configuration file:

name | data type | required | description
-----|-----------|----------|------------
zookeeper.quorum | string | yes | Zookeeper quorum of the HBase cluster
event.parser.class | string | yes | Can be either AvroEventParser or JsonEventParser to parse avro or json events respectively.
topics | string | yes | list of kafka topics.
hbase.`<topicname>`.rowkey.columns | string | yes | The columns that represent the rowkey of the hbase table `<topicname>`
hbase.`<topicname>`.family | string | yes | Column family of the hbase table `<topicname>`.

Example connector.properties file

```bash
name=kafka-cdc-hbase
connector.class=io.svectors.hbase.sink.HBaseSinkConnector
tasks.max=1
topics=test
zookeeper.quorum=localhost:2181
event.parser.class=io.svectors.hbase.parser.AvroEventParser
hbase.test.rowkey.columns=id
hbase.test.rowkey.delimiter=|
hbase.test.family=d
```

## Packaging
* mvn clean package


## Deployment

* Follow the [Getting started](http://hbase.apache.org/book.html#standalone_dist) guide for HBase.

* [Download and install Confluent](http://www.confluent.io/)

* Copy hbase-sink.jar and hbase-sink.properties from the project build location to `$CONFLUENT_HOME/share/java/kafka-connect-hbase`

```bash
mkdir $CONFLUENT_HOME/share/java/kafka-connect-hbase
cp target/hbase-sink.jar  $CONFLUENT_HOME/share/java/kafka-connect-hbase/
cp hbase-sink.properties $CONFLUENT_HOME/share/java/kafka-connect-hbase/
```

* Start Zookeeper, Kafka and Schema registry

```bash
nohup $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties &
nohup $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties &
nohup $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties &"
```

* Create HBase table 'test' from hbase shell

* Start the hbase sink

```bash
export CLASSPATH=$CONFLUENT_HOME/share/java/kafka-connect-hbase/hbase-sink.jar

$CONFLUENT_HOME/bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka-connect-hbase/hbase-sink.properties
```

* Test with avro console, start the console to create the topic and write values

```bash
$CONFLUENT_HOME/bin/kafka-avro-console-producer \
--broker-list localhost:9092 --topic test \
--property value.schema='{"type":"record","name":"record","fields":[{"name":"id","type":"int"}, {"name":"name", "type": "string"}]}'
```

```bash
#insert at prompt
{"id": 1, "name": "foo"}
{"id": 2, "name": "bar"}
```
