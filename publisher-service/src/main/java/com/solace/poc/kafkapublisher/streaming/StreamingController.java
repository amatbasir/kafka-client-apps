package com.solace.poc.kafkapublisher.streaming;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Runtime controls for the streaming event simulator, so the demo/POC
 * can be started and stopped without restarting the service.
 */
@RestController
@RequestMapping("/api/events/stream")
public class StreamingController {

    private final StreamingEventGenerator generator;

    public StreamingController(StreamingEventGenerator generator) {
        this.generator = generator;
    }

    @PostMapping("/start")
    public Map<String, Object> start() {
        generator.start();
        return status();
    }

    @PostMapping("/stop")
    public Map<String, Object> stop() {
        generator.stop();
        return status();
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of(
                "running", generator.isRunning(),
                "eventsSent", generator.getEventCount()
        );
    }
}
