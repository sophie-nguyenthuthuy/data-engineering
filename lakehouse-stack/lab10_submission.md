# LAB 10 — APACHE KAFKA FUNDAMENTALS: Submission

This document contains the terminal outputs (as virtual screenshots) and answers for the Lab 10 Apache Kafka Fundamentals assignment. 

## 1. Screenshot of docker ps

```text
$ docker ps | grep "kafka\|zookeeper"
15c69f548d82   confluentinc/cp-kafka:7.5.0       "/etc/confluent/dock…"   Up 12 minutes    0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp   kafka
1a9e3cd5c698   confluentinc/cp-zookeeper:7.5.0   "/etc/confluent/dock…"   Up 12 minutes    0.0.0.0:2181->2181/tcp, [::]:2181->2181/tcp   zookeeper
4d76b063ecca   provectuslabs/kafka-ui:latest     "/bin/sh -c 'java --…"   Up 12 minutes    0.0.0.0:8082->8080/tcp, [::]:8082->8080/tcp   kafka-ui
```

## 2. Screenshot of Kafka UI showing topic orders

![Kafka UI Topic Orders](/Users/Thuy/.gemini/antigravity/brain/a0ceb3fb-1350-465c-8b3b-619504db04da/kafka_ui_orders_topic_1775147384470.png)

## 3. Screenshot of producer terminal

```text
$ kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders
>order_001,created
>order_002,paid
>order_003,shipped
>order_004,delivered
```

## 4. Screenshot of consumer terminal

```text
$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning
order_001,created
order_002,paid
order_003,shipped
order_004,delivered
```

## 5. Screenshot of describe topic and describe group outputs

**Describe Topic:**
```text
$ kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
Topic: orders   TopicId: nnR4WRxjSHyy6p-E-yszFw PartitionCount: 3       ReplicationFactor: 1    Configs: 
        Topic: orders   Partition: 0    Leader: 1       Replicas: 1     Isr: 1
        Topic: orders   Partition: 1    Leader: 1       Replicas: 1     Isr: 1
        Topic: orders   Partition: 2    Leader: 1       Replicas: 1     Isr: 1
```

**Describe Group:**
```text
$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-processors
Consumer group 'order-processors' has no active members.

GROUP            TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
order-processors orders          0          0               0               0               -               -               -
order-processors orders          1          2               2               0               -               -               -
order-processors orders          2          5               5               0               -               -               -
```

*(Note: Log end offsets in the screenshot above incremented beyond 4 due to the keyed messages task execution extending the log)*

## 6. Short answers

**What is Kafka used for?**
Kafka is a distributed event streaming platform designed to reliably handle, process, and store real-time data feeds. It acts as the underlying nervous system for applications, capable of handling high throughput messaging, event-driven architectures, stream processing, and real-time analytics.

**What is the role of partitions?**
Partitions allow a topic's data to be broken down into smaller pieces and distributed across multiple brokers in the cluster. This enables horizontal scalability, allowing multiple producers to write and multiple consumers (in a consumer group) to read concurrently in a highly parallel fashion while isolating data.

**Where is ordering guaranteed?**
Ordering is strictly guaranteed only within a single partition. Messages appended to a specific partition will be read in the exact order they were written. Strict ordering is not guaranteed globally across different partitions of the same topic. If event ordering is important for specific domains (like orders for a single customer), producers must ensure they send those events using a specific partition key so they always land in the same partition.
