package com.outboxsagalab.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the inventory-service.
 *
 * Responsibilities:
 *  - Consume {@code inventory-commands} (ReserveStock / ReleaseStock)
 *  - Emit {@code inventory-events} (StockReserved / StockReservationFailed / StockReleased)
 *  - Persist domain state in its own Postgres database (port 5434).
 *
 * Scheduling is enabled here because the outbox poller runs as a {@code @Scheduled} task.
 */
@SpringBootApplication
@EnableScheduling
public class InventoryApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryApplication.class, args);
    }
}
