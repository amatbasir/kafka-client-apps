package com.solace.poc.kafkapublisher.producer;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import com.solace.poc.kafkapublisher.event.MerchantOnboardingCompletedEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Publishes the merchant.onboarding.completed event.
 *
 * This is the piece Abishek will swap out when exploring the "native Solace
 * API" migration option (e.g. replacing KafkaTemplate/ProducerRecord with a
 * Solace JCSMP/JMS publisher), while keeping the same envelope, key, and
 * trace-id header semantics.
 */
@Service
public class MessageProducer {

    private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic:merchant.onboarding.completed}")
    private String topicName;

    @Value("${app.service.name:onboarding-service}")
    private String serviceName;

    @Value("${app.deployment.environment:local}")
    private String deploymentEnvironment;

    public MessageProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Builds and publishes a merchant.onboarding.completed event.
     *
     * @param merchantOnboardingId used as both the Kafka record key and the
     *                             payload.merchant_onboarding_id field
     * @param traceId              propagated as both the trace-id header and
     *                             the trace_id body field; a new one is
     *                             generated if not supplied
     */
    public void publishOnboardingCompleted(String merchantOnboardingId, String traceId) {
        String effectiveTraceId = (traceId == null || traceId.isBlank())
                ? UUID.randomUUID().toString()
                : traceId;

        MerchantOnboardingCompletedEvent event = new MerchantOnboardingCompletedEvent(
                ZonedDateTime.now().format(TIMESTAMP_FORMAT),
                effectiveTraceId,
                new MerchantOnboardingCompletedEvent.ServiceInfo(serviceName),
                new MerchantOnboardingCompletedEvent.DeploymentInfo(deploymentEnvironment),
                new MerchantOnboardingCompletedEvent.MerchantOnboardingPayload(merchantOnboardingId)
        );

        String json;
        try {
            json = objectMapper.writeValueAsString(event);
        } catch (JacksonException e) {
            log.error("Failed to serialize onboarding event for merchantOnboardingId={}", merchantOnboardingId, e);
            return;
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, merchantOnboardingId, json);
        record.headers().add(new RecordHeader("trace-id", effectiveTraceId.getBytes(StandardCharsets.UTF_8)));

        log.info("Publishing merchant.onboarding.completed -> key={}, traceId={}, payload={}",
                merchantOnboardingId, effectiveTraceId, json);

        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish onboarding event for merchantOnboardingId={}", merchantOnboardingId, ex);
            } else {
                log.info("Published merchantOnboardingId={} -> partition={}, offset={}",
                        merchantOnboardingId,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
