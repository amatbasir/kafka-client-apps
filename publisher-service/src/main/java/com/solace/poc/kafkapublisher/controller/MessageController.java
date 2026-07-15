package com.solace.poc.kafkapublisher.controller;

import com.solace.poc.kafkapublisher.producer.MessageProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

/**
 * REST front door for the Onboarding Service so the POC can be exercised
 * with curl/Postman: simulates a merchant finishing the onboarding form,
 * which publishes merchant.onboarding.completed.
 */
@RestController
@RequestMapping("/api/onboarding")
public class MessageController {

    private final MessageProducer producer;

    public MessageController(MessageProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/completed")
    public Map<String, String> publish(@RequestBody Map<String, String> body) {
        String merchantOnboardingId = body.getOrDefault("merchantOnboardingId", "merchant-" + UUID.randomUUID());
        String traceId = body.get("traceId"); // optional; producer generates one if absent

        producer.publishOnboardingCompleted(merchantOnboardingId, traceId);

        return Map.of(
                "status", "published",
                "topic", "merchant.onboarding.completed",
                "merchantOnboardingId", merchantOnboardingId
        );
    }
}
