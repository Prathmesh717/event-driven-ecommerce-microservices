package io.github.prathmesh717.inventoryservice.consumer;

import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.PaymentSucceededEvent;
import io.github.prathmesh717.inventoryservice.producer.RetryEventProducer;
import io.github.prathmesh717.inventoryservice.service.InventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * ==========================================
 * Handles Payment Succeeded Events
 * ==========================================
 *
 * Responsibilities:
 * - Consume PaymentSucceededEvent
 * - Reserve Inventory
 * - Retry Failed Processing
 * - Publish Events to Retry Topic
 * - Publish Events to Dead Letter Topic (DLT)
 *
 * Patterns Implemented:
 * - Choreography-Based Saga
 * - Retry Pattern
 * - Dead Letter Topic (DLT) Pattern
 * - Eventual Consistency
 *
 * Flow:
 *
 * Payment Service
 *      ↓
 * payment-succeeded (Kafka Topic)
 *      ↓
 * Inventory Service
 *      ↓
 * Reserve Inventory
 *
 * Success:
 *
 * inventory-reserved (Kafka Topic)
 *
 * Failure:
 *
 * payment-succeeded-retry (Kafka Topic)
 *      ↓
 * Retry Processing
 *      ↓
 * payment-succeeded-dlt (Kafka Topic)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentSucceededConsumer {

    /**
     * Maximum number of retry attempts before
     * sending the event to Dead Letter Topic.
     */
    private static final int MAX_RETRIES = 3;

    private final InventoryService inventoryService;

    private final RetryEventProducer retryEventProducer;

    /**
     * Consumes PaymentSucceededEvent and attempts
     * to reserve inventory.
     *
     * If processing fails:
     * - Retry event is published to Retry Topic.
     * - After max retries, event is published to DLT.
     */
    @KafkaListener(
            topics = KafkaTopics.PAYMENT_SUCCEEDED,
            groupId = "inventory-service-group",
            containerFactory =
                    "kafkaListenerContainerFactory"
    )
    public void consume(
            PaymentSucceededEvent event) {

        try {

            /*
             * Attempt inventory reservation.
             *
             * On success, Inventory Service
             * publishes InventoryReservedEvent.
             */
            inventoryService.reserveInventory(
                    event);

        } catch (Exception ex) {

            log.error(
                    "Inventory processing failed. orderId={}",
                    event.orderId(),
                    ex);

            /*
             * Create a new event with an
             * incremented retry count.
             */
            PaymentSucceededEvent retryEvent =
                    new PaymentSucceededEvent(
                            event.eventId(),
                            event.retryCount() + 1,
                            event.paymentId(),
                            event.orderId(),
                            event.productId(),
                            event.quantity(),
                            event.amount());

            /*
             * Route event either to Retry Topic
             * or Dead Letter Topic based on
             * retry threshold.
             */
            if (retryEvent.retryCount()
                    >= MAX_RETRIES) {

                log.warn(
                        "Max retries reached. Sending event to DLT. orderId={}",
                        event.orderId());

                retryEventProducer
                        .publishDltEvent(
                                retryEvent);

            } else {

                log.info(
                        "Publishing event to retry topic. orderId={}, retryCount={}",
                        event.orderId(),
                        retryEvent.retryCount());

                retryEventProducer
                        .publishRetryEvent(
                                retryEvent);
            }
        }
    }
}