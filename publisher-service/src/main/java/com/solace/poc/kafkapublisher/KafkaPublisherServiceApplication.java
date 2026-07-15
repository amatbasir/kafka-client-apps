package com.solace.poc.kafkapublisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaPublisherServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaPublisherServiceApplication.class, args);
    }
}
