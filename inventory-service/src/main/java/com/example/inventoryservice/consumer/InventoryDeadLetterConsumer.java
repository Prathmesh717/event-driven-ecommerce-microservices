package io.github.prathmesh717.inventoryservice.consumer;

import io.github.prathmesh717.contracts.event.PaymentSucceededEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class InventoryDeadLetterConsumer {

    @KafkaListener(
            topics = "payment-succeeded-dlt",
            groupId = "inventory-dlt-group",
            containerFactory =
                    "kafkaListenerContainerFactory"
    )
    public void consume(PaymentSucceededEvent event) {

        log.error(
                "Message moved to DLT. orderId={}, productId={}",
                event.orderId(),
                event.productId());
    }
}