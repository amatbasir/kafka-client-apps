package com.solace.poc.kafkaconsumer.consumer;

import tools.jackson.databind.ObjectMapper;
import com.solace.poc.kafkaconsumer.event.MerchantOnboardingCompletedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Notification Service: consumes merchant.onboarding.completed and (in this
 * POC) simulates sending a "welcome" notification to the merchant.
 *
 * This is the piece that can be left untouched when validating the Kafka
 * Proxy option (should just work against the proxy's bootstrap address),
 * or swapped for a native Solace JCSMP/JMS consumer when exploring that option.
 */
@Component
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    private final ObjectMapper objectMapper;
    private final AtomicLong receivedCount = new AtomicLong(0);
    private final AtomicReference<Map<String, Object>> lastEvent = new AtomicReference<>();

    public MessageConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${app.kafka.topic:merchant.onboarding.completed}",
            groupId = "${spring.kafka.consumer.group-id:notification-service-group}"
    )
    public void consume(ConsumerRecord<String, String> record) {
        String headerTraceId = extractHeader(record, "trace-id");

        try {
            MerchantOnboardingCompletedEvent event =
                    objectMapper.readValue(record.value(), MerchantOnboardingCompletedEvent.class);

            String merchantOnboardingId = event.payload().merchantOnboardingId();

            log.info(
                    "Received merchant.onboarding.completed -> partition={}, offset={}, key={}, " +
                            "headerTraceId={}, bodyTraceId={}, merchantOnboardingId={}",
                    record.partition(), record.offset(), record.key(),
                    headerTraceId, event.traceId(), merchantOnboardingId
            );

            // Simulated notification action - replace with real email/SMS/push integration.
            log.info("Sending welcome notification to merchantOnboardingId={} (traceId={})",
                    merchantOnboardingId, headerTraceId);

            receivedCount.incrementAndGet();
            lastEvent.set(Map.of(
                    "merchantOnboardingId", merchantOnboardingId,
                    "traceId", headerTraceId != null ? headerTraceId : event.traceId(),
                    "partition", record.partition(),
                    "offset", record.offset(),
                    "timestamp", event.timestamp()
            ));
        } catch (Exception e) {
            log.error("Failed to process merchant.onboarding.completed record at partition={}, offset={}",
                    record.partition(), record.offset(), e);
        }
    }

    private String extractHeader(ConsumerRecord<String, String> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    public long getReceivedCount() {
        return receivedCount.get();
    }

    public Map<String, Object> getLastEvent() {
        return lastEvent.get();
    }
}
