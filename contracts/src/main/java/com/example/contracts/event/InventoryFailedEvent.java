package io.github.prathmesh717.contracts.event;

import java.util.UUID;

/**
 * ==========================================
 * Inventory Failed Event
 * ==========================================
 *
 * Published By:
 * - inventory-service
 *
 * Consumed By:
 * - payment-service
 *
 * Kafka Topic:
 * - inventory-failed
 *
 * Purpose:
 * Signals that inventory reservation could
 * not be completed for an order.
 *
 * This event triggers the compensation
 * path of the Saga workflow.
 *
 * Business Outcome:
 * - Payment Service initiates refund
 * - Payment Service publishes PaymentRefundedEvent
 *
 * Common Failure Reasons:
 * - Product not found
 * - Insufficient inventory
 */
public record InventoryFailedEvent(

        UUID orderId,

        Long productId,

        Integer requestedQuantity,

        String reason

) {
}