package com.solace.poc.kafkapublisher.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topic contract for merchant.onboarding.completed, per the event catalog entry:
 *   - partitions: 2
 *   - key: merchant_onboarding_id (used for producer partitioning)
 *   - TTL: 30 days -> retention.ms
 *
 * The publisher (Onboarding Service) owns topic creation so the topic exists
 * before the consumer-service (Notification Service) starts listening.
 * Auto-create can be turned off once topics are provisioned/managed
 * separately (e.g. via Event Portal on the Solace side).
 */
@Configuration
public class KafkaTopicConfig {

    private static final long RETENTION_30_DAYS_MS = 30L * 24 * 60 * 60 * 1000; // 2_592_000_000 ms

    @Value("${app.kafka.topic:merchant.onboarding.completed}")
    private String topicName;

    @Bean
    public NewTopic merchantOnboardingCompletedTopic() {
        return TopicBuilder.name(topicName)
                .partitions(2)
                .replicas(1)
                .config("retention.ms", String.valueOf(RETENTION_30_DAYS_MS))
                .build();
    }
}
