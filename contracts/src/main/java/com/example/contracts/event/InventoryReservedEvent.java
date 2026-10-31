package io.github.prathmesh717.contracts.event;

import java.util.UUID;

/**
 * ==========================================
 * Inventory Reserved Event
 * ==========================================
 *
 * Published By:
 * - inventory-service
 *
 * Consumed By:
 * - order-service
 * - notification-service
 * - analytics-service
 *
 * Kafka Topic:
 * - inventory-reserved
 *
 * Purpose:
 * Signals that inventory has been reserved
 * successfully for an order.
 *
 * This event represents the successful
 * completion path of the Saga workflow.
 *
 * Business Outcome:
 * - Order Service marks order as COMPLETED
 * - Notification Service sends completion notification
 * - Analytics Service increments COMPLETED_ORDERS
 */
public record InventoryReservedEvent(

        UUID orderId,

        Long productId,

        Integer quantity

) {
}