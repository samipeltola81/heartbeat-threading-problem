package org.example.camel;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.camel.util.KafkaAdminUtil;
import org.apache.camel.BeanInject;
import org.apache.camel.CamelContext;
import org.apache.camel.test.infra.kafka.services.KafkaService;
import org.apache.camel.test.infra.kafka.services.KafkaServiceFactory;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.example.camel.util.KafkaTestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class AbstractKafkaTest extends CamelTestSupport  {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractKafkaTest.class);
    public static final int TEST_TIMEOUT = 30;

    @RegisterExtension
    protected static KafkaService SERVICE = KafkaServiceFactory.createSingletonService();

    protected org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;

    protected static AdminClient kafkaAdminClient;

    @BeanInject
    CamelContext camelContext;

    @AfterAll
    public static void cleanUpResources() {
        if (kafkaAdminClient != null ) {
            LOG.info("Closing kafkaAdminClient");
            kafkaAdminClient.close(Duration.ofSeconds(3));
            kafkaAdminClient = null;
        }
    }

    @BeforeEach
    public void setKafkaAdminClient() {

        Properties kafkaClientProperties = KafkaTestUtil.getDefaultProperties(SERVICE);
        producer = new org.apache.kafka.clients.producer.KafkaProducer<>(kafkaClientProperties);

        if (kafkaAdminClient == null) {
            kafkaAdminClient = KafkaAdminUtil.createAdminClient(SERVICE);
        }
    }

    protected void printOffsets(String consumerGroupId) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> offsets = kafkaAdminClient.listConsumerGroupOffsets(consumerGroupId).partitionsToOffsetAndMetadata().get();
        LOG.info("********* Offsets --> *********");
        offsets.forEach( (partition, offsetAndMetadata) ->
                LOG.info("Partition: {}, metadata: {}, offset: {}", partition, offsetAndMetadata.metadata(), offsetAndMetadata.offset())
        );
        LOG.info("********* <-- Offsets *********");
    }

    protected String getPropertiesLocation() {
        return "classpath:application-test.properties";
    }


    public static List<ProducerRecord> asProducerRecords(String topic, Integer partition, String key, List<String> messages) {
        return new ArrayList<>(messages.stream().map(msg -> new ProducerRecord(topic, partition, key, msg)).toList());
    }
    protected void sendToTopic(List<ProducerRecord> messages) {
        messages.forEach(producerRecord -> {
            LOG.info("Sending message: {}", producerRecord.value());
            Future<RecordMetadata> future =  producer.send(producerRecord);
            try {
                RecordMetadata metadata = future.get();
                LOG.info("Record metadata: {}: -> {}", metadata.timestamp(), metadata);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
