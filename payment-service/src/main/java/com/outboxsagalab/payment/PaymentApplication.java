package com.outboxsagalab.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the payment-service.
 *
 * <p>This service is a saga participant: it consumes commands from
 * {@code payment-commands} and emits replies to {@code payment-events}.
 * It implements the textbook outbox + idempotent-consumer patterns; the
 * scheduled outbox poller is enabled here via {@link EnableScheduling}.
 */
@SpringBootApplication
@EnableScheduling
public class PaymentApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentApplication.class, args);
    }
}
