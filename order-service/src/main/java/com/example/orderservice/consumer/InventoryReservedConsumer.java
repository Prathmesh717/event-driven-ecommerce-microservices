package io.github.prathmesh717.orderservice.consumer;

import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.InventoryReservedEvent;
import io.github.prathmesh717.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * ==========================================
 * Handles Inventory Reserved Events
 * ==========================================
 *
 * Responsibilities:
 * - Consume InventoryReservedEvent
 * - Complete Order Processing
 *
 * Pattern Implemented:
 * - Choreography-Based Saga
 * - Eventual Consistency
 *
 * This consumer represents the successful
 * completion path of the Saga workflow.
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
 * inventory-reserved (Kafka Topic)
 *      ↓
 * Order Service
 *      ↓
 * Order Status = COMPLETED
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryReservedConsumer {

    private final OrderService orderService;

    /**
     * Consumes InventoryReservedEvent and marks
     * the order as COMPLETED.
     */
    @KafkaListener(
            topics = KafkaTopics.INVENTORY_RESERVED,
            groupId = "order-service-group",
            containerFactory =
                    "inventoryReservedKafkaListenerContainerFactory"
    )
    public void consume(
            InventoryReservedEvent event) {

        log.info(
                "Received InventoryReservedEvent. orderId={}",
                event.orderId());

        /*
         * Inventory has been reserved successfully.
         *
         * Complete the Saga by updating the
         * order status to COMPLETED.
         */
        orderService.completeOrder(
                event.orderId());
    }
}