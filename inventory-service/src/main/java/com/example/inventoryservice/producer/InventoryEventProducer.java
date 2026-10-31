package io.github.prathmesh717.inventoryservice.producer;

import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.InventoryReservedEvent;
import io.github.prathmesh717.contracts.event.InventoryFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publish InventoryReservedEvent.
     */
    public void publishInventoryReservedEvent(
            InventoryReservedEvent event) {

        kafkaTemplate.send(
                        KafkaTopics.INVENTORY_RESERVED,
                        event.orderId().toString(),
                        event)
                .whenComplete((result, ex) -> {

                    if (ex == null) {

                        log.info(
                                "Published InventoryReservedEvent. orderId={}, partition={}, offset={}",
                                event.orderId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());

                    } else {

                        log.error(
                                "Failed to publish InventoryReservedEvent. orderId={}",
                                event.orderId(),
                                ex);
                    }
                });
    }

    public void publishInventoryFailedEvent(
            InventoryFailedEvent event) {

        kafkaTemplate.send(
                        KafkaTopics.INVENTORY_FAILED,
                        event.orderId().toString(),
                        event)
                .whenComplete((result, ex) -> {

                    if (ex == null) {

                        log.info(
                                "Published InventoryFailedEvent. orderId={}",
                                event.orderId());

                    } else {

                        log.error(
                                "Failed to publish InventoryFailedEvent. orderId={}",
                                event.orderId(),
                                ex);
                    }
                });
    }
}