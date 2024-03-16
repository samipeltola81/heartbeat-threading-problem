package org.example.camel.util;

import org.apache.camel.test.infra.kafka.services.KafkaService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class KafkaAdminUtil {

    private KafkaAdminUtil() {

    }

    public static AdminClient createAdminClient(KafkaService service) {
        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, service.getBootstrapServers());

        return KafkaAdminClient.create(properties);
    }

    public static Map<String, ConsumerGroupDescription> getConsumerGroupInfo(String groupId, AdminClient kafkaAdminClient)
            throws InterruptedException, ExecutionException, TimeoutException {
        return kafkaAdminClient.describeConsumerGroups(Collections.singletonList(groupId)).all().get(30, TimeUnit.SECONDS);
    }

    /**
     *
     * @param service
     * @param topic
     * @param partition
     * @param consumerGroupId
     * @return Returns the offset of a consumer group in a given topic partition. Returns -1 is not found.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static long getConsumerGroupOffset(KafkaService service, String topic, int partition, String consumerGroupId) throws ExecutionException, InterruptedException {
        return getConsumerGroupOffset(KafkaAdminUtil.createAdminClient(service), topic, partition, consumerGroupId);
    }
    public static long getConsumerGroupOffset(AdminClient adminClient, String topic, int partition, String consumerGroupId) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> offsets = adminClient.listConsumerGroupOffsets(consumerGroupId).partitionsToOffsetAndMetadata().get();
        OffsetAndMetadata offsetAndMetadata = offsets.get(new TopicPartition(topic, partition));
        return offsetAndMetadata != null ? offsetAndMetadata.offset() : -1;

    }

}