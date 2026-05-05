package com.outboxsagalab.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * One row per (order_id, sku) reservation attempt. Drives the participant's
 * compensation logic — to release we look up RESERVED rows by order_id.
 */
@Entity
@Table(name = "reservations")
public class Reservation {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "sku", nullable = false, length = 64)
    private String sku;

    @Column(name = "qty", nullable = false)
    private int qty;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private ReservationStatus status;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    protected Reservation() {
    }

    public Reservation(UUID id, UUID orderId, String sku, int qty, ReservationStatus status) {
        this.id = id;
        this.orderId = orderId;
        this.sku = sku;
        this.qty = qty;
        this.status = status;
        this.createdAt = OffsetDateTime.now();
    }

    public UUID getId() {
        return id;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getSku() {
        return sku;
    }

    public int getQty() {
        return qty;
    }

    public ReservationStatus getStatus() {
        return status;
    }

    public void markReleased() {
        this.status = ReservationStatus.RELEASED;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}
