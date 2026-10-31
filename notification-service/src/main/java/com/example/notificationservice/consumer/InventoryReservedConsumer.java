package io.github.prathmesh717.notificationservice.consumer;

import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.InventoryReservedEvent;
import io.github.prathmesh717.notificationservice.service.NotificationService;
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
 * - Send Order Completion Notifications
 *
 * Patterns Implemented:
 * - Event-Driven Communication
 * - Eventual Consistency
 *
 * This consumer listens for successful inventory
 * reservations and sends order completion
 * notifications to customers.
 *
 * Flow:
 *
 * Inventory Service
 *      ↓
 * inventory-reserved (Kafka Topic)
 *      ↓
 * Notification Service
 *      ↓
 * Send Order Completed Notification
 */
@Component
@RequiredArgsConstructor
public class InventoryReservedConsumer {

    private final NotificationService notificationService;

    /**
     * Consumes InventoryReservedEvent and sends
     * an order completion notification.
     */
    @KafkaListener(
            topics = KafkaTopics.INVENTORY_RESERVED,
            groupId = "notification-service-group",
            containerFactory =
                    "inventoryReservedKafkaListenerContainerFactory"
    )
    public void consume(
            InventoryReservedEvent event) {

        /*
         * Notify customer that the order
         * has been completed successfully.
         */
        notificationService
                .sendOrderCompletedNotification(
                        event);
    }
}