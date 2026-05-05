package com.outboxsagalab.order.api;

import com.outboxsagalab.order.domain.Order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * REST DTO returned by POST /orders and GET /orders/{id}.
 *
 * The "status" field exposes the saga state — that's what GET /orders/{id}
 * is for in this lab.
 */
public record OrderResponse(
        UUID id,
        String status,
        String customerId,
        BigDecimal totalAmount,
        Instant createdAt,
        Instant updatedAt,
        List<Item> items
) {

    public record Item(String sku, int quantity, BigDecimal unitPrice) {
    }

    public static OrderResponse from(Order order) {
        List<Item> items = order.getItems().stream()
                .map(i -> new Item(i.getSku(), i.getQuantity(), i.getUnitPrice()))
                .toList();
        return new OrderResponse(
                order.getId(),
                order.getStatus().name(),
                order.getCustomerId(),
                order.getTotalAmount(),
                order.getCreatedAt(),
                order.getUpdatedAt(),
                items
        );
    }
}
