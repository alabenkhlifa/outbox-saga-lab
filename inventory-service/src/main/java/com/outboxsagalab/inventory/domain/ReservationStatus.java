package com.outboxsagalab.inventory.domain;

/**
 * Lifecycle of a single (order_id, sku) reservation row.
 *
 * <ul>
 *   <li>{@code RESERVED} — stock was decremented, awaiting either completion or release.</li>
 *   <li>{@code RELEASED} — saga rolled back; stock has been added back.</li>
 *   <li>{@code FAILED}   — initial reserve attempt found insufficient stock; no decrement happened.</li>
 * </ul>
 */
public enum ReservationStatus {
    RESERVED,
    RELEASED,
    FAILED
}
