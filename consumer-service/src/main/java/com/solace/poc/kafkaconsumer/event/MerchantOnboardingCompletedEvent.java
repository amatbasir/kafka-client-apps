package com.solace.poc.kafkaconsumer.event;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Event contract consumed from merchant.onboarding.completed, mirroring the
 * publisher-service (Onboarding Service) envelope.
 *
 * Note: for a real multi-module setup this DTO would live in a shared
 * library instead of being duplicated per service - kept duplicated here
 * to keep each service independently deployable for the POC.
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
