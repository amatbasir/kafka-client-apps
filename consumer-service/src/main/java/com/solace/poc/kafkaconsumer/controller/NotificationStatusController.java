package com.solace.poc.kafkaconsumer.controller;

import com.solace.poc.kafkaconsumer.consumer.MessageConsumer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Lets you check, without tailing logs, how many onboarding-completed
 * events the Notification Service has processed and what the last one was.
 */
@RestController
@RequestMapping("/api/notifications")
public class NotificationStatusController {

    private final MessageConsumer consumer;

    public NotificationStatusController(MessageConsumer consumer) {
        this.consumer = consumer;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of(
                "eventsReceived", consumer.getReceivedCount(),
                "lastEvent", consumer.getLastEvent() != null ? consumer.getLastEvent() : Map.of()
        );
    }
}
