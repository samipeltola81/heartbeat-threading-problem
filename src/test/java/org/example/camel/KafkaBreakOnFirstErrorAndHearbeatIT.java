package org.example.camel;

import org.apache.camel.*;
import org.apache.camel.builder.NotifyBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.component.mock.MockEndpoint;
import org.example.camel.util.KafkaAdminUtil;
import org.example.camel.util.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.camel.util.PropertiesHelper.asProperties;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Demonstrates the behavior of breakOnFirstError and an exception occurs.
 *
 * Will result in a new heartbeat thread being created for each poll
 */
class KafkaBreakOnFirstErrorAndHearbeatIT extends AbstractKafkaTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaBreakOnFirstErrorAndHearbeatIT.class);
    public static final String LOCAL_FROM_KAFKA_ERROR_MANUALCOMMIT_ROUTE = "LocalFromKafkaErrorManualCommitRoute";
    public static final String MOCK_RESULT = "mock:result";

    @PropertyInject("consumer.group")
    String consumerGroupId;
    protected final String TOPIC_ERROR = "testerrorhandling";
    protected final String TOPIC_NORMAL = "testnormal";

    @EndpointInject("mock:result")
    private MockEndpoint to;

    @EndpointInject("kafka:" + TOPIC_ERROR
            + "?brokers={{kafka.brokers}}&clientId={{consumer.clientId}}&groupId={{consumer.group}}"
            + "&maxPollRecords={{consumer.maxPollRecords}}&autoOffsetReset={{consumer.autoOffsetReset}}"
            + "&autoCommitEnable={{consumer.autoCommitEnable}}&allowManualCommit={{consumer.allowManualCommit}}&breakOnFirstError={{consumer.breakOnFirstError}}")
    private Endpoint from;

    @Override
    protected CamelContext createCamelContext() throws Exception {

        boolean allowManualCommit = true;
        boolean autoCommitEnable = false;
        boolean breakOnFirstError = true;

        CamelContext camelContext = super.createCamelContext();
        // Set the location of the configuration
        camelContext.getPropertiesComponent().setLocation(getPropertiesLocation());

        // Override the host and port of the broker
        camelContext.getPropertiesComponent().setOverrideProperties(
                asProperties(
                        "kafka.brokers", SERVICE.getBootstrapServers(),
                        // These settings will override Endpoint initialization
                        "consumer.autoCommitEnable", String.valueOf(autoCommitEnable),
                        // Set manual commits on the endpoint level
                        "consumer.allowManualCommit", String.valueOf(allowManualCommit),
                        "consumer.breakOnFirstError", String.valueOf(breakOnFirstError),
                        // let's not start consumer route automatically
                        "consumer.autostartRoute", String.valueOf(false)
                )
        );

        return camelContext;
    }

    @Test
    @DisplayName("breakOnFirstError enabled will cause multiple heartbeat threads to be created")
    void breakOnFirstError_enabled() throws Exception {
        int partition = 0;
        List<String> messages = TestUtil.createTestMessages(1);

        // Expectation is that only these message will be sent to the mock
        List<String> expectedMessages = List.copyOf(messages);

        // Let's add ERROR message in between
        messages.add("ERROR");
        // This message should not be sent to the mock
        messages.add("message-after-error");

        to.expectedMessageCount(expectedMessages.size());
        to.expectedBodiesReceived(expectedMessages);

        sendToTopic(asProducerRecords(TOPIC_ERROR, 0, "", messages));

        NotifyBuilder notify = new NotifyBuilder(context).fromRoute(LOCAL_FROM_KAFKA_ERROR_MANUALCOMMIT_ROUTE)
                // We'll let retry occur once
                .whenDone(messages.size())
                .create();

        LOG.info("**** Starting consumer ****");
        camelContext.getRouteController().startRoute(LOCAL_FROM_KAFKA_ERROR_MANUALCOMMIT_ROUTE);

        assertTrue(
                notify.matches(TEST_TIMEOUT, TimeUnit.SECONDS), String.format("Messages should be completed: %d", messages.size())
        );

        to.assertIsSatisfied(3000);
        printOffsets(consumerGroupId);
        // Expectation is that the offset will be committed before ERROR-message
        Assertions.assertEquals(1, KafkaAdminUtil.getConsumerGroupOffset(kafkaAdminClient, TOPIC_ERROR, partition, consumerGroupId));

        printAllThreads();

        int heartbeatThreadCount = countHeartbeatThreads();
        LOG.info("Number of heartbeat threads is: {}", heartbeatThreadCount);

        Assertions.assertEquals(1, heartbeatThreadCount);
    }


    protected void printAllThreads() {
        Set<Thread> threads = Thread.getAllStackTraces().keySet();
        LOG.info("****** Running Threads ***********");
        LOG.info("Name | State | Priority | isDaemon");
        for (Thread t : threads) {
            LOG.info("{} | {} | {} | {}", t.getName(), t.getState(), t.getPriority(), t.isDaemon());
        }
    }

    protected int countHeartbeatThreads() {
        Set<Thread> threads = Thread.getAllStackTraces().keySet();
        int count = 0;
        for (Thread t : threads) {
            if (t.getName().contains("heartbeat"))
                count++;
        }
        return count;
    }

    @Override
    protected RoutesBuilder[] createRouteBuilders() {

        return new RoutesBuilder[]{
                // Error route will throw errors when the msg body is ERROR
                new RouteBuilder() {
                    @Override
                    public void configure() throws Exception {
                        from(from)
                                .autoStartup("{{consumer.autostartRoute}}")
                                .routeId(LOCAL_FROM_KAFKA_ERROR_MANUALCOMMIT_ROUTE)
                                .log("Message received from Kafka : ${body}")
                                .log("    on the topic ${headers[kafka.TOPIC]}")
                                .log("    on the partition ${headers[kafka.PARTITION]}")
                                .log("    with the offset ${headers[kafka.OFFSET]}")
                                .log("    with the key ${headers[kafka.KEY]}")
                                .log("LAST_RECORD_BEFORE_COMMIT: ${headers[" + KafkaConstants.LAST_RECORD_BEFORE_COMMIT + "]}" )
                                .log("LAST_POLL_RECORD: ${headers[" + KafkaConstants.LAST_POLL_RECORD + "]}" )
                                .choice()
                                    .when(simple("${body} == 'ERROR'"))
                                        .log("!!! Throw exception !!!!")
                                        .throwException(new RuntimeException("INTENTIONAL ERROR THROWN"))
                                    .endChoice()
                                .end()
                                .to(MOCK_RESULT)
                                // we'll commit after every successful handling
                                .process(exchange -> {
                                    KafkaManualCommit manual =
                                            exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
                                    manual.commit();
                                });
                    }
                }
        };
    }
}
