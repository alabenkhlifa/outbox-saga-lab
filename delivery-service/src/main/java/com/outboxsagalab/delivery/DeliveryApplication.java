package com.outboxsagalab.delivery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the delivery-service.
 *
 * <p>This service is a saga participant: it consumes commands from
 * {@code delivery-commands} and emits replies to {@code delivery-events}.
 * It implements the textbook outbox + idempotent-consumer patterns; the
 * scheduled outbox poller is enabled here via {@link EnableScheduling}.
 */
@SpringBootApplication
@EnableScheduling
public class DeliveryApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeliveryApplication.class, args);
    }
}
