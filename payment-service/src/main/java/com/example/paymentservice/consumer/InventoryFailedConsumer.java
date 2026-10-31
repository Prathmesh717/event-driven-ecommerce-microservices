package io.github.prathmesh717.paymentservice.consumer;

import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.InventoryFailedEvent;
import io.github.prathmesh717.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * ==========================================
 * Handles Inventory Failed Events
 * ==========================================
 *
 * Responsibilities:
 * - Consume InventoryFailedEvent
 * - Initiate Payment Refund
 *
 * Patterns Implemented:
 * - Choreography-Based Saga
 * - Saga Compensation
 * - Eventual Consistency
 *
 * This consumer represents the compensation
 * path of the Saga workflow.
 *
 * Flow:
 *
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
 * Refund Payment
 *      ↓
 * payment-refunded (Kafka Topic)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryFailedConsumer {

    private final PaymentService paymentService;

    /**
     * Consumes InventoryFailedEvent and initiates
     * payment refund processing.
     *
     * Triggered when inventory reservation
     * fails after payment has already been
     * processed successfully.
     */
    @KafkaListener(
            topics = KafkaTopics.INVENTORY_FAILED,
            groupId = "payment-service-group"
    )
    public void consume(
            InventoryFailedEvent event) {

        log.info(
                "Received InventoryFailedEvent. orderId={}",
                event.orderId());

        /*
         * Start Saga compensation by
         * refunding the payment.
         */
        paymentService.refundPayment(event);
    }
}