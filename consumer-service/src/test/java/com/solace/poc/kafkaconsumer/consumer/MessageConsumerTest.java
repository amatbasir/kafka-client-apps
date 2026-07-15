package com.solace.poc.kafkaconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Broker-free unit test: drives {@link MessageConsumer#consume} with a
 * hand-built {@link ConsumerRecord} and asserts the tracked status.
 */
class MessageConsumerTest {

    private static final String JSON = """
            {"timestamp":"2026-01-01T00:00:00.000+0000","trace_id":"trace-body",\
            "service":{"name":"onboarding-service"},\
            "deployment":{"environment":"test"},\
            "payload":{"merchant_onboarding_id":"merchant-123456"}}""";

    @Test
    void consumesEventAndUpdatesStatus() {
        MessageConsumer consumer = new MessageConsumer(new ObjectMapper());
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("merchant.onboarding.completed", 0, 42L, "merchant-123456", JSON);
        record.headers().add(new RecordHeader("trace-id", "trace-hdr".getBytes(StandardCharsets.UTF_8)));

        consumer.consume(record);

        assertThat(consumer.getReceivedCount()).isEqualTo(1);
        Map<String, Object> last = consumer.getLastEvent();
        assertThat(last).isNotNull();
        assertThat(last.get("merchantOnboardingId")).isEqualTo("merchant-123456");
        assertThat(last.get("traceId")).isEqualTo("trace-hdr"); // header wins over body trace_id
        assertThat(last.get("partition")).isEqualTo(0);
        assertThat(last.get("offset")).isEqualTo(42L);
    }

    @Test
    void fallsBackToBodyTraceIdWhenHeaderMissing() {
        MessageConsumer consumer = new MessageConsumer(new ObjectMapper());
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("merchant.onboarding.completed", 1, 7L, "merchant-123456", JSON);

        consumer.consume(record);

        assertThat(consumer.getReceivedCount()).isEqualTo(1);
        assertThat(consumer.getLastEvent().get("traceId")).isEqualTo("trace-body");
    }

    @Test
    void malformedPayloadDoesNotIncrementCount() {
        MessageConsumer consumer = new MessageConsumer(new ObjectMapper());
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("merchant.onboarding.completed", 0, 1L, "k", "not-json");

        consumer.consume(record);

        assertThat(consumer.getReceivedCount()).isZero();
        assertThat(consumer.getLastEvent()).isNull();
    }
}
