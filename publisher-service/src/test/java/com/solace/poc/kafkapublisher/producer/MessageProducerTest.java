package com.solace.poc.kafkapublisher.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Broker-free unit test: exercises the envelope/key/trace-id semantics of
 * {@link MessageProducer} with a mocked {@link KafkaTemplate}.
 */
class MessageProducerTest {

    private KafkaTemplate<String, String> kafkaTemplate;
    private MessageProducer producer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        // Return an un-completed future so the whenComplete callback is not invoked.
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());

        producer = new MessageProducer(kafkaTemplate, new ObjectMapper());
        ReflectionTestUtils.setField(producer, "topicName", "merchant.onboarding.completed");
        ReflectionTestUtils.setField(producer, "serviceName", "onboarding-service");
        ReflectionTestUtils.setField(producer, "deploymentEnvironment", "test");
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishesRecordWithKeyTraceHeaderAndJsonBody() {
        producer.publishOnboardingCompleted("merchant-123456", "trace-abc");

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();

        assertThat(record.topic()).isEqualTo("merchant.onboarding.completed");
        assertThat(record.key()).isEqualTo("merchant-123456");
        assertThat(record.value())
                .contains("\"merchant_onboarding_id\":\"merchant-123456\"")
                .contains("\"trace_id\":\"trace-abc\"")
                .contains("\"name\":\"onboarding-service\"")
                .contains("\"environment\":\"test\"");

        Header traceHeader = record.headers().lastHeader("trace-id");
        assertThat(traceHeader).isNotNull();
        assertThat(new String(traceHeader.value(), StandardCharsets.UTF_8)).isEqualTo("trace-abc");
    }

    @Test
    @SuppressWarnings("unchecked")
    void generatesTraceIdWhenNotProvided() {
        producer.publishOnboardingCompleted("merchant-999", null);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());

        Header traceHeader = captor.getValue().headers().lastHeader("trace-id");
        assertThat(traceHeader).isNotNull();
        assertThat(new String(traceHeader.value(), StandardCharsets.UTF_8)).isNotBlank();
    }
}
