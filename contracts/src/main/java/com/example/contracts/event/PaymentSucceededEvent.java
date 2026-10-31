package io.github.prathmesh717.contracts.event;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * ==========================================
 * Payment Succeeded Event
 * ==========================================
 *
 * Published By:
 * - payment-service
 *
 * Consumed By:
 * - inventory-service
 *
 * Kafka Topics:
 * - payment-succeeded
 * - payment-succeeded-retry
 * - payment-succeeded-dlt
 *
 * Purpose:
 * Signals that payment has been processed
 * successfully and inventory reservation
 * can begin.
 *
 * Notes:
 * - eventId is used for Idempotent Consumer
 *   processing.
 * - retryCount is used for Retry and
 *   Dead Letter Topic (DLT) handling.
 */
public record PaymentSucceededEvent(

        UUID eventId,

        Integer retryCount,

        UUID paymentId,

        UUID orderId,

        Long productId,

        Integer quantity,

        BigDecimal amount

) {
}