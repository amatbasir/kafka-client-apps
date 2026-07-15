package com.solace.poc.kafkapublisher.event;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Event contract published when a prospective merchant finishes filling out
 * the onboarding form.
 *
 * Topic: merchant.onboarding.completed
 * Key:   merchant_onboarding_id
 * Header: trace-id (string)
 * TTL:   30 days
 */
public record MerchantOnboardingCompletedEvent(
        @JsonProperty("timestamp") String timestamp,
        @JsonProperty("trace_id") String traceId,
        @JsonProperty("service") ServiceInfo service,
        @JsonProperty("deployment") DeploymentInfo deployment,
        @JsonProperty("payload") MerchantOnboardingPayload payload
) {

    public record ServiceInfo(@JsonProperty("name") String name) {
    }

    public record DeploymentInfo(@JsonProperty("environment") String environment) {
    }

    public record MerchantOnboardingPayload(@JsonProperty("merchant_onboarding_id") String merchantOnboardingId) {
    }
}
