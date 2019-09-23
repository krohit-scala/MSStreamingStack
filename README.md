# MoneySmartStreamingStack
Implement a streaming pipeline using Kafka, Spark Structured Streaming, Redis and Cassandra.

# Environment details
JDK: 1.8.0_211
Scala: 2.12.8
Spark: 2.3.3
Kafka: 1.1.0
Redis: 5.0.5
Cassandra: 3.11.4 (CQLSH: 5.0.1)
spark-redis-connector: spark-redis-2.3.1
spark-cassandra-connector: 2.3.0

PRODUCER API
============
cd ./projects/moneysmart/producer/msProducerApis

mvn clean install

java -cp ./target/uber-producerApis-0.0.1.jar com.driver.App MSS1


CONSUMER API
============
cd ./projects/moneysmart/producer/msConsumerApis

mvn clean install

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.3,datastax:spark-cassandra-connector:2.3.0-s_2.11 --class com.consumer.StructuredStreaming /home/kr_stevejobs/projects/moneysmart/consumer/msConsumerApis/target/uber-consumerApis-0.0.1.jar MSS1

CASSANDRA TABLES
================
$ cqlsh

DROP TABLE IF EXISTS moneysmartprocessed;
CREATE TABLE moneysmartprocessed(
    ts TEXT,
    user_id TEXT,
    message_date TEXT,
    user_agent TEXT,
    partner_id TEXT,
    partner_name TEXT,
    init_session BOOLEAN,
    session_id TEXT,
    page_type TEXT,
    category TEXT,
    cart_amount TEXT,
    platform TEXT,
    last_visited TEXT,
    user_device TEXT,
    PRIMARY KEY(ts, user_id)
);
