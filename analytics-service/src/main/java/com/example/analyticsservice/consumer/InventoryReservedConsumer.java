package io.github.prathmesh717.analyticsservice.consumer;

import io.github.prathmesh717.analyticsservice.service.AnalyticsService;
import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.InventoryReservedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * ==========================================
 * Handles Inventory Reserved Events
 * ==========================================
 *
 * Responsibilities:
 * - Consume InventoryReservedEvent
 * - Update Analytics Metrics
 *
 * Patterns Implemented:
 * - Event-Driven Analytics
 * - Eventual Consistency
 *
 * This consumer listens for successful inventory
 * reservations and updates completed order metrics.
 *
 * Flow:
 *
 * Inventory Service
 *      ↓
 * inventory-reserved (Kafka Topic)
 *      ↓
 * Analytics Service
 *      ↓
 * Increment COMPLETED_ORDERS
 */
@Component
@RequiredArgsConstructor
public class InventoryReservedConsumer {

    private final AnalyticsService analyticsService;

    /**
     * Consumes InventoryReservedEvent and increments
     * the completed orders metric.
     */
    @KafkaListener(
            topics = KafkaTopics.INVENTORY_RESERVED,
            groupId = "analytics-service-group",
            containerFactory =
                    "inventoryReservedKafkaListenerContainerFactory"
    )
    public void consume(
            InventoryReservedEvent event) {

        /*
         * Update analytics metrics when
         * an order is completed successfully.
         */
        analyticsService.incrementCompletedOrders();
    }
}