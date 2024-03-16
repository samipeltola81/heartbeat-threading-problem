package org.example.camel.util;

import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.test.infra.kafka.services.KafkaService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public final class KafkaTestUtil {

    public static final String MOCK_RESULT = "mock:result";
    public static final String MOCK_RESULT_BAR = "mock:resultBar";
    public static final String MOCK_DLQ = "mock:dlq";

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTestUtil.class);

    private KafkaTestUtil() {

    }

    public static void setServiceProperties(KafkaService service) {
        LOG.info("### Embedded Kafka cluster broker list: {}", service.getBootstrapServers());
        System.setProperty("bootstrapServers", service.getBootstrapServers());
    }

    public static AdminClient createAdminClient(KafkaService service) {
        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, service.getBootstrapServers());

        return KafkaAdminClient.create(properties);
    }

    public static Properties getDefaultProperties(String bootstrapService) {
        LOG.info("Connecting to Kafka {}", bootstrapService);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapService);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConstants.KAFKA_DEFAULT_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaConstants.KAFKA_DEFAULT_SERIALIZER);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, "0");
        return props;
    }

    public static Properties getDefaultProperties(KafkaService service) {
        return getDefaultProperties(service.getBootstrapServers());
    }
}
