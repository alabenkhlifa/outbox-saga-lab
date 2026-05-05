package com.outboxsagalab.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * order-service entry point.
 *
 * The order-service is the saga orchestrator: it owns the order lifecycle,
 * sends commands to participants (payment, inventory, delivery) and reacts
 * to their reply events.
 */
@SpringBootApplication
@EnableScheduling
public class OrderApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
