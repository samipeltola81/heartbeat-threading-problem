# Camel Issue: CAMEL-20563

Example project for Camel Kafka Jira ticket: https://issues.apache.org/jira/browse/CAMEL-20563

Using breakOnFirstError option for Kafka component creates a new heartbeat thread every time KafkaFetchRecords is set to reconnect, which happens when there an exception is allowed to propagate back from the route.

Run with:

```
mvn clean verify -Pintegrationtests -Dkafka.instance.type=local-strimzi-container
```
