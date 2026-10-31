package io.github.prathmesh717.orderservice.scheduler;

import io.github.prathmesh717.contracts.event.OrderCreatedEvent;
import io.github.prathmesh717.orderservice.entity.OutboxEvent;
import io.github.prathmesh717.orderservice.kafka.OrderEventProducer;
import io.github.prathmesh717.orderservice.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * ==========================================
 * Publishes events from the Outbox table
 * ==========================================
 *
 * Responsibilities:
 * - Read unpublished events from Outbox table
 * - Publish events to Kafka
 * - Mark events as published
 *
 * Pattern Implemented:
 * - Transactional Outbox Pattern
 *
 * Why?
 * Orders and events must be persisted together
 * in a single database transaction. This publisher
 * asynchronously sends stored events to Kafka,
 * ensuring database and messaging consistency.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxEventRepository
            outboxEventRepository;

    private final OrderEventProducer
            orderEventProducer;

    private final ObjectMapper objectMapper;

    /**
     * Polls the Outbox table periodically and
     * publishes unpublished events to Kafka.
     *
     * After successful publication, the event
     * is marked as published to prevent
     * duplicate processing.
     */
    @Scheduled(fixedDelay = 5000)
    public void publishEvents() {

        List<OutboxEvent> events =
                outboxEventRepository
                        .findByPublishedFalse();

        for (OutboxEvent event : events) {

            try {

                /*
                 * Deserialize stored event payload
                 * from the Outbox table.
                 */
                OrderCreatedEvent orderCreatedEvent =
                        objectMapper.readValue(
                                event.getPayload(),
                                OrderCreatedEvent.class);

                /*
                 * Publish event to Kafka Topic:
                 * order-created
                 */
                orderEventProducer
                        .publishOrderCreatedEvent(
                                orderCreatedEvent);

                /*
                 * Mark event as published after
                 * successful Kafka publication.
                 */
                event.setPublished(true);

                outboxEventRepository.save(event);

                log.info(
                        "Outbox event published successfully. id={}",
                        event.getId());

            } catch (Exception ex) {

                /*
                 * Event remains unpublished and
                 * will be retried during the next
                 * scheduler execution.
                 */
                log.error(
                        "Failed to publish outbox event. id={}",
                        event.getId(),
                        ex);
            }
        }
    }
}