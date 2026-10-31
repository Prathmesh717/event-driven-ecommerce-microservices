package io.github.prathmesh717.analyticsservice.consumer;

import io.github.prathmesh717.analyticsservice.service.AnalyticsService;
import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * ==========================================
 * Handles Order Created Events
 * ==========================================
 *
 * Responsibilities:
 * - Consume OrderCreatedEvent
 * - Update Analytics Metrics
 *
 * Patterns Implemented:
 * - Event-Driven Analytics
 * - Eventual Consistency
 *
 * This consumer listens for newly created orders
 * and updates analytics metrics without directly
 * accessing Order Service data.
 *
 * Flow:
 *
 * Order Service
 *      ↓
 * order-created (Kafka Topic)
 *      ↓
 * Analytics Service
 *      ↓
 * Increment TOTAL_ORDERS
 */
@Component
@RequiredArgsConstructor
public class OrderCreatedConsumer {

    private final AnalyticsService analyticsService;

    /**
     * Consumes OrderCreatedEvent and increments
     * the total orders metric.
     */
    @KafkaListener(
            topics = KafkaTopics.ORDER_CREATED,
            groupId = "analytics-service-group",
            containerFactory =
                    "orderCreatedKafkaListenerContainerFactory"
    )
    public void consume(
            OrderCreatedEvent event) {

        /*
         * Update analytics metrics when
         * a new order is created.
         */
        analyticsService.incrementTotalOrders();
    }
}