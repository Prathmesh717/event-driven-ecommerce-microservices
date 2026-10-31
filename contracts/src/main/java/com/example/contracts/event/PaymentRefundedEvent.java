package io.github.prathmesh717.contracts.event;

import java.util.UUID;

/**
 * ==========================================
 * Payment Refunded Event
 * ==========================================
 *
 * Published By:
 * - payment-service
 *
 * Consumed By:
 * - order-service
 * - notification-service
 * - analytics-service
 *
 * Kafka Topic:
 * - payment-refunded
 *
 * Purpose:
 * Signals that a previously successful payment
 * has been refunded as part of Saga compensation.
 *
 * This event represents the completion of the
 * compensation path of the Saga workflow.
 *
 * Business Outcome:
 * - Order Service marks order as CANCELLED
 * - Notification Service sends cancellation notification
 * - Analytics Service increments CANCELLED_ORDERS
 *
 * Common Refund Reasons:
 * - Product not found
 * - Insufficient inventory
 */
public record PaymentRefundedEvent(

        UUID orderId,

        UUID paymentId,

        String reason

) {
}