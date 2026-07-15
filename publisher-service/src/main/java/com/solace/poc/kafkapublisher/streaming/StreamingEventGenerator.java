package com.solace.poc.kafkapublisher.streaming;

import com.solace.poc.kafkapublisher.producer.MessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates a continuous stream of merchant.onboarding.completed events
 * (as if prospective merchants keep finishing the onboarding form) so the
 * POC has realistic ongoing traffic to demo and to benchmark
 * (throughput/latency) when comparing Kafka vs. the Kafka Proxy vs. native
 * Solace API.
 *
 * Start/stop is controlled at runtime via StreamingController, with
 * defaults coming from app.streaming.* in application.yml.
 */
@Component
public class StreamingEventGenerator {

    private static final Logger log = LoggerFactory.getLogger(StreamingEventGenerator.class);

    private final MessageProducer producer;
    private final AtomicBoolean streaming;
    private final AtomicLong eventCount = new AtomicLong(0);

    public StreamingEventGenerator(
            MessageProducer producer,
            @Value("${app.streaming.enabled:true}") boolean enabledByDefault
    ) {
        this.producer = producer;
        this.streaming = new AtomicBoolean(enabledByDefault);
    }

    @Scheduled(fixedDelayString = "${app.streaming.interval-ms:2000}")
    public void tick() {
        if (!streaming.get()) {
            return;
        }

        String merchantOnboardingId = "merchant-" + ThreadLocalRandom.current().nextInt(100000, 999999);
        String traceId = UUID.randomUUID().toString();

        producer.publishOnboardingCompleted(merchantOnboardingId, traceId);
        eventCount.incrementAndGet();
    }

    public boolean start() {
        boolean wasRunning = streaming.getAndSet(true);
        if (!wasRunning) {
            log.info("Streaming event generator started");
        }
        return true;
    }

    public boolean stop() {
        boolean wasRunning = streaming.getAndSet(false);
        if (wasRunning) {
            log.info("Streaming event generator stopped after {} events", eventCount.get());
        }
        return false;
    }

    public boolean isRunning() {
        return streaming.get();
    }

    public long getEventCount() {
        return eventCount.get();
    }
}
