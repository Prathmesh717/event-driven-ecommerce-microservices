package io.github.prathmesh717.contracts.event;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * ==========================================
 * Order Created Event
 * ==========================================
 *
 * Published By:
 * - order-service
 *
 * Consumed By:
 * - payment-service
 * - analytics-service
 *
 * Kafka Topic:
 * - order-created
 *
 * Purpose:
 * Signals that a new order has been created
 * and payment processing can begin.
 */
public record OrderCreatedEvent(

        UUID orderId,

        Long customerId,

        Long productId,

        Integer quantity,

        BigDecimal amount,

        String status

) {
}