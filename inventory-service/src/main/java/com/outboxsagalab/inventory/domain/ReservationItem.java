package com.outboxsagalab.inventory.domain;

/**
 * One line item carried inside a ReserveStock command — the SKU to reserve
 * and how many units. Plain DTO, no JPA annotations.
 */
public record ReservationItem(String sku, int qty) {
}
