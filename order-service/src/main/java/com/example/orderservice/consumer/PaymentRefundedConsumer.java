package io.github.prathmesh717.orderservice.consumer;

import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.PaymentRefundedEvent;
import io.github.prathmesh717.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * ==========================================
 * Handles Payment Refunded Events
 * ==========================================
 *
 * Responsibilities:
 * - Consume PaymentRefundedEvent
 * - Cancel Orders
 *
 * Pattern Implemented:
 * - Choreography-Based Saga
 * - Saga Compensation
 * - Eventual Consistency
 *
 * This consumer represents the compensation
 * path of the Saga workflow.
 *
 * Flow:
 *
 * Order Service
 *      ↓
 * order-created (Kafka Topic)
 *      ↓
 * Payment Service
 *      ↓
 * payment-succeeded (Kafka Topic)
 *      ↓
 * Inventory Service
 *      ↓
 * inventory-failed (Kafka Topic)
 *      ↓
 * Payment Service
 *      ↓
 * payment-refunded (Kafka Topic)
 *      ↓
 * Order Service
 *      ↓
 * Order Status = CANCELLED
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRefundedConsumer {

    private final OrderService orderService;

    /**
     * Consumes PaymentRefundedEvent and marks
     * the order as CANCELLED.
     *
     * Triggered when inventory reservation
     * fails and the payment has been refunded
     * as part of Saga compensation.
     */
    @KafkaListener(
            topics = KafkaTopics.PAYMENT_REFUNDED,
            groupId = "order-service-group",
            containerFactory =
                    "paymentRefundedKafkaListenerContainerFactory"
    )
    public void consume(
            PaymentRefundedEvent event) {

        log.info(
                "Received PaymentRefundedEvent. orderId={}",
                event.orderId());

        /*
         * Payment has been refunded successfully.
         *
         * Complete the compensation flow by
         * updating the order status to CANCELLED.
         */
        orderService.cancelOrder(
                event.orderId());
    }
}