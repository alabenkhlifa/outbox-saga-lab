package com.outboxsagalab.order.api;

import java.math.BigDecimal;
import java.util.List;

/**
 * REST DTO for POST /orders.
 */
public record CreateOrderRequest(String customerId, List<Item> items) {

    public record Item(String sku, int quantity, BigDecimal unitPrice) {
    }
}
