package io.github.prathmesh717.inventoryservice.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * ==========================================
 * Stores Processed Kafka Events
 * ==========================================
 *
 * Purpose:
 * - Track successfully processed events
 * - Prevent duplicate event processing
 *
 * Pattern Implemented:
 * - Idempotent Consumer Pattern
 *
 * When a Kafka event is processed successfully,
 * its eventId is stored in this table.
 *
 * If the same event is received again,
 * processing is skipped.
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    /**
     * Unique identifier of the processed event.
     */
    @Id
    private UUID eventId;

    /**
     * Timestamp when the event was processed.
     */
    private LocalDateTime processedAt;
}